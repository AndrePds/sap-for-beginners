# Requirements: pip install pandas pyarrow faker tqdm
"""SAP Synthetic Data Generator — Parquet Edition.

Replicates the ShadowTraffic sap-synthetic-data-azure.json dataset in Parquet format.
Maintains full referential integrity via an in-memory lookup registry.

Output: datasets/generator/sample_data_parquet/<table>/<table>-0.parquet
"""

from __future__ import annotations

import itertools
import logging
import random
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from faker import Faker

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    from unittest.mock import MagicMock
    tqdm = MagicMock  # type: ignore[misc]
    HAS_TQDM = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sap_generator")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).parent.parent.parent
OUTPUT_BASE = Path(__file__).parent / "sample_data_parquet"

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

fake = Faker("pt_BR")
random.seed(42)

# Registry: keyPrefix → list of generated record dicts
_registry: dict[str, list[dict[str, Any]]] = {}

# ---------------------------------------------------------------------------
# Helper: random datetime with variation
# ---------------------------------------------------------------------------

def _random_date_str() -> str:
    """Return a random YYYYMMDD string spread over ~3 years."""
    delta = timedelta(days=random.randint(-730, 365))
    dt = datetime.now() + delta
    return dt.strftime("%Y%m%d")


def _random_time_str() -> str:
    """Return a random HHMMSS string."""
    h = random.randint(0, 23)
    m = random.randint(0, 59)
    s = random.randint(0, 59)
    return f"{h:02d}{m:02d}{s:02d}"


# ---------------------------------------------------------------------------
# _gen primitive resolvers
# ---------------------------------------------------------------------------

def _resolve(spec: Any, cycles: dict[str, Any]) -> Any:
    """Resolve a single ShadowTraffic _gen spec to a Python value."""
    if not isinstance(spec, dict) or "_gen" not in spec:
        return spec

    gen = spec["_gen"]

    if gen == "constant":
        return spec["x"]

    if gen == "digitString":
        return "".join(random.choices("0123456789", k=spec["n"]))

    if gen == "oneOf":
        return random.choice(spec["choices"])

    if gen == "weightedOneOf":
        choices = spec["choices"]
        values = [c["value"] for c in choices]
        weights = [c["weight"] for c in choices]
        result = random.choices(values, weights=weights, k=1)[0]
        # Nested _gen inside weightedOneOf
        if isinstance(result, dict) and "_gen" in result:
            return _resolve(result, cycles)
        return result

    if gen == "cycle":
        key = str(spec["sequence"])
        if key not in cycles:
            cycles[key] = itertools.cycle(spec["sequence"])
        return next(cycles[key])

    if gen == "uniformDistribution":
        lo, hi = spec["bounds"]
        dec = spec.get("decimals", 2)
        return round(random.uniform(lo, hi), dec)

    if gen == "normalDistribution":
        mean = spec["mean"]
        sd = spec["sd"]
        dec = spec.get("decimals", 2)
        value = random.gauss(mean, sd)
        return round(max(0.0, value), dec)

    if gen == "formatDateTime":
        fmt = spec.get("format", "yyyyMMdd")
        # Convert Java date pattern to Python strftime
        py_fmt = fmt.replace("yyyy", "%Y").replace("MM", "%m").replace("dd", "%d")
        py_fmt = py_fmt.replace("HH", "%H").replace("mm", "%M").replace("ss", "%S")
        delta = timedelta(days=random.randint(-730, 365))
        dt = datetime.now() + delta
        return dt.strftime(py_fmt)

    if gen == "now":
        delta = timedelta(days=random.randint(-730, 0))
        return int((datetime.now() + delta).timestamp() * 1000)

    if gen == "string":
        expr: str = spec.get("expr", "")
        return _faker_expr(expr)

    if gen == "lookup":
        key_prefix = spec["keyPrefix"]
        path = spec["path"]  # e.g. ["data", "vbeln"]
        field_name = path[-1]  # last element is always the field name
        records = _registry.get(key_prefix, [])
        if not records:
            logger.warning("Lookup on empty registry: %s", key_prefix)
            return None
        record = random.choice(records)
        return record.get(field_name)

    logger.warning("Unknown _gen: %s", gen)
    return None


def _faker_expr(expr: str) -> str:
    """Convert ShadowTraffic Faker expression to a value."""
    mapping = {
        "#{Company.name}": fake.company,
        "#{Name.full_name}": fake.name,
        "#{Name.first_name}": fake.first_name,
        "#{Name.last_name}": fake.last_name,
        "#{Commerce.productName}": fake.catch_phrase,
        "#{Commerce.department}": fake.job,
        "#{Address.city}": fake.city,
        "#{Internet.slug}": fake.slug,
        "#{Lorem.word}": fake.word,
    }
    fn = mapping.get(expr)
    if fn:
        return fn()
    # Fallback: return a generic word
    return fake.word()


# ---------------------------------------------------------------------------
# Table generator
# ---------------------------------------------------------------------------

@dataclass
class TableSpec:
    """Definition of one SAP table generator."""
    table_name: str      # lowercase SAP table name (e.g. "vbak")
    key_prefix: str      # ShadowTraffic keyPrefix (e.g. "vbak/orders-")
    max_events: int
    fields: dict[str, Any]  # field_name → _gen spec


def generate_table(spec: TableSpec) -> pd.DataFrame:
    """Generate `spec.max_events` records for one SAP table.

    Registers all records in the global _registry under spec.key_prefix
    so downstream lookups can reference them.
    """
    records: list[dict[str, Any]] = []
    cycles: dict[str, Any] = {}  # per-table cycle iterators

    iterator = range(spec.max_events)
    if HAS_TQDM:
        iterator = tqdm(iterator, desc=f"  {spec.table_name:<20}", leave=False, unit="rec")

    for _ in iterator:
        row: dict[str, Any] = {}
        for col_name, col_spec in spec.fields.items():
            row[col_name] = _resolve(col_spec, cycles)
        records.append(row)

    _registry[spec.key_prefix] = records
    return pd.DataFrame(records)


def save_parquet(df: pd.DataFrame, table_name: str) -> Path:
    """Write DataFrame to Parquet under OUTPUT_BASE/<table_name>/."""
    out_dir = OUTPUT_BASE / table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{table_name}-0.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="snappy")
    return out_path


# ---------------------------------------------------------------------------
# Table definitions (ordered by dependency chain)
# ---------------------------------------------------------------------------

def _build_table_specs() -> list[TableSpec]:
    """Return all table specs in dependency order (master → transactional)."""
    return [

        # ── Master Data ────────────────────────────────────────────────────

        TableSpec("kna1", "kna1/customers-", 1000, {
            "mandt": {"_gen": "constant", "x": "100"},
            "kunnr": {"_gen": "digitString", "n": 10},
            "name1": {"_gen": "string", "expr": "#{Company.name}"},
            "mcod1": {"_gen": "string", "expr": "#{Company.name}"},
            "stcd1": {"_gen": "digitString", "n": 14},
            "ort01": {"_gen": "oneOf", "choices": ["SAO PAULO","CAMACARI","TRIUNFO","MAUA","SANTO ANDRE"]},
            "regio": {"_gen": "oneOf", "choices": ["SP","BA","RS","RJ","MG","PR","PE","CE"]},
            "land1": {"_gen": "constant", "x": "BR"},
            "ktokd": {"_gen": "oneOf", "choices": ["0001","0002","Z001","Z002","Z003"]},
        }),

        TableSpec("lfa1", "lfa1/vendors-", 200, {
            "mandt": {"_gen": "constant", "x": "100"},
            "lifnr": {"_gen": "digitString", "n": 10},
            "name1": {"_gen": "string", "expr": "#{Company.name}"},
            "mcod1": {"_gen": "string", "expr": "#{Company.name}"},
            "ort01": {"_gen": "oneOf", "choices": ["SAO PAULO","CAMPINAS","CURITIBA","BELO HORIZONTE","PORTO ALEGRE"]},
            "regio": {"_gen": "oneOf", "choices": ["SP","PR","MG","RS","RJ"]},
            "land1": {"_gen": "constant", "x": "BR"},
            "stcd1": {"_gen": "digitString", "n": 14},
        }),

        TableSpec("mara", "mara/materials-", 500, {
            "mandt": {"_gen": "constant", "x": "100"},
            "matnr": {"_gen": "digitString", "n": 18},
            "bismt": {"_gen": "digitString", "n": 18},
            "prdha": {"_gen": "oneOf", "choices": [
                "00001000100010001","00001000100010002","00001000200010001",
                "00002000100010001","00002000200020002","00003000100010001","00003000200010002",
            ]},
            "matkl": {"_gen": "oneOf", "choices": ["1000","1001","2000","2001","3000","4000"]},
            "meins": {"_gen": "oneOf", "choices": ["UN","KG","L","M3","TON","KL"]},
            "gewei": {"_gen": "oneOf", "choices": ["KG","G","LB"]},
            "ntgew": {"_gen": "uniformDistribution", "bounds": [0.1, 5000], "decimals": 3},
            "brgew": {"_gen": "uniformDistribution", "bounds": [0.1, 5200], "decimals": 3},
        }),

        TableSpec("pa0001", "pa0001/employees-", 100, {
            "mandt": {"_gen": "constant", "x": "100"},
            "pernr": {"_gen": "digitString", "n": 8},
            "begda": {"_gen": "constant", "x": "19900101"},
            "endda": {"_gen": "constant", "x": "99991231"},
            "ename": {"_gen": "string", "expr": "#{Name.full_name}"},
            "vorna": {"_gen": "string", "expr": "#{Name.first_name}"},
            "nachn": {"_gen": "string", "expr": "#{Name.last_name}"},
            "plans": {"_gen": "digitString", "n": 8},
            "orgeh": {"_gen": "digitString", "n": 8},
        }),

        TableSpec("t001w", "t001w/plants-", 20, {
            "mandt": {"_gen": "constant", "x": "100"},
            "werks": {"_gen": "cycle", "sequence": [
                "BR01","BR10","BR51","BR55","BR41","BR45","BR05","BR06","BR07","BR08",
                "BR11","BR12","BR13","BR14","BR15","BR16","BR17","BR18","BR19","BR20",
            ]},
            "name1": {"_gen": "string", "expr": "#{Company.name}"},
            "bwkey": {"_gen": "oneOf", "choices": ["BR01","BR10","BR51","BR55","BR41"]},
            "bukrs": {"_gen": "oneOf", "choices": ["BR01","BR10","BR51","BR55"]},
            "land1": {"_gen": "constant", "x": "BR"},
            "regio": {"_gen": "oneOf", "choices": ["SP","BA","RS","RJ","MG","PR","PE","CE"]},
            "ort01": {"_gen": "string", "expr": "#{Address.city}"},
        }),

        # ── Text / Config Tables ───────────────────────────────────────────

        TableSpec("makt", "makt/material-texts-", 500, {
            "mandt": {"_gen": "constant", "x": "100"},
            "matnr": {"_gen": "lookup", "container": "sap", "keyPrefix": "mara/materials-", "path": ["data", "matnr"]},
            "spras": {"_gen": "constant", "x": "P"},
            "maktx": {"_gen": "string", "expr": "#{Commerce.productName}"},
            "maktg": {"_gen": "string", "expr": "#{Commerce.productName}"},
        }),

        TableSpec("tvsbt", "tvsbt/shipping-conditions-", 10, {
            "mandt": {"_gen": "constant", "x": "100"},
            "spras": {"_gen": "constant", "x": "P"},
            "vsbed": {"_gen": "cycle", "sequence": ["01","02","03","04","05","06","07","08","09","10"]},
            "vtext": {"_gen": "string", "expr": "#{Lorem.word}"},
        }),

        TableSpec("t173t", "t173t/shipping-types-", 8, {
            "mandt": {"_gen": "constant", "x": "100"},
            "spras": {"_gen": "constant", "x": "P"},
            "vsart": {"_gen": "cycle", "sequence": ["01","02","03","04","05","06","07","08"]},
            "bezei": {"_gen": "oneOf", "choices": ["RODOVIARIO","FERROVIARIO","AEREO","MARITIMO","DUTOVIA","MULTIMODAL","CABOTAGEM","FLUVIAL"]},
        }),

        TableSpec("tvrot", "tvrot/routes-", 10, {
            "mandt": {"_gen": "constant", "x": "100"},
            "spras": {"_gen": "constant", "x": "P"},
            "route": {"_gen": "cycle", "sequence": ["0001","0002","0003","0004","BR01","BR02","BR03","BR04","BR05","BR06"]},
            "bezei": {"_gen": "string", "expr": "#{Address.city}"},
        }),

        TableSpec("tvstt", "tvstt/shipping-points-", 6, {
            "mandt": {"_gen": "constant", "x": "100"},
            "spras": {"_gen": "constant", "x": "P"},
            "vstel": {"_gen": "cycle", "sequence": ["BR01","BR02","BR03","BR04","BR05","BR06"]},
            "vtext": {"_gen": "string", "expr": "#{Address.city}"},
        }),

        TableSpec("tvaut", "tvaut/order-reasons-", 10, {
            "mandt": {"_gen": "constant", "x": "100"},
            "spras": {"_gen": "constant", "x": "P"},
            "augru": {"_gen": "cycle", "sequence": ["001","002","003","004","005","006","007","008","009","010"]},
            "bezei": {"_gen": "string", "expr": "#{Lorem.word}"},
        }),

        TableSpec("tvagt", "tvagt/rejection-reasons-", 15, {
            "mandt": {"_gen": "constant", "x": "100"},
            "spras": {"_gen": "constant", "x": "P"},
            "abgru": {"_gen": "cycle", "sequence": ["01","02","03","04","05","06","07","08","09","10","11","12","13","14","15"]},
            "bezei": {"_gen": "string", "expr": "#{Lorem.word}"},
        }),

        TableSpec("tvkggt", "tvkggt/customer-groups-", 20, {
            "mandt": {"_gen": "constant", "x": "100"},
            "spras": {"_gen": "constant", "x": "P"},
            "kdkgr": {"_gen": "digitString", "n": 3},
            "vtext": {"_gen": "string", "expr": "#{Commerce.department}"},
        }),

        TableSpec("dd07t", "dd07t/domain-texts-", 20, {
            "mandt":      {"_gen": "constant", "x": "100"},
            "domname":    {"_gen": "oneOf", "choices": ["CMGST","ZZ_ST_DEAL","OIG_SSTSF"]},
            "ddlanguage": {"_gen": "constant", "x": "P"},
            "domvalue_l": {"_gen": "cycle", "sequence": ["A","B","C","D","E","1","2","3","4","5","X","Y","01","02","03","04","05","06","07","08"]},
            "ddtext":     {"_gen": "string", "expr": "#{Lorem.word}"},
        }),

        TableSpec("dd07v", "dd07v/domain-values-", 30, {
            "mandt":      {"_gen": "constant", "x": "100"},
            "domname":    {"_gen": "oneOf", "choices": ["OIG_SSTSF","CMGST","VSBED","LFART","GBSTK"]},
            "ddlanguage": {"_gen": "constant", "x": "P"},
            "domvalue_l": {"_gen": "cycle", "sequence": ["A","B","C","D","E","1","2","3","4","5","X","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","J"]},
            "ddtext":     {"_gen": "string", "expr": "#{Lorem.word}"},
        }),

        TableSpec("knmt", "knmt/customer-material-", 2000, {
            "mandt": {"_gen": "constant", "x": "100"},
            "vkorg": {"_gen": "oneOf", "choices": ["BR01","BR10","BR51","BR55","BR41","BR45"]},
            "vtweg": {"_gen": "oneOf", "choices": ["10","20","30","40"]},
            "kunnr": {"_gen": "lookup", "container": "sap", "keyPrefix": "kna1/customers-", "path": ["data", "kunnr"]},
            "matnr": {"_gen": "lookup", "container": "sap", "keyPrefix": "mara/materials-", "path": ["data", "matnr"]},
            "kdmat": {"_gen": "digitString", "n": 35},
            "sortl": {"_gen": "digitString", "n": 10},
        }),

        # ── Sales Order Chain ──────────────────────────────────────────────

        TableSpec("vbak", "vbak/orders-", 5000, {
            "mandt":      {"_gen": "constant", "x": "100"},
            "vbeln":      {"_gen": "digitString", "n": 10},
            "erdat":      {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "ernam":      {"_gen": "string", "expr": "#{Internet.slug}"},
            "erzet":      {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "HHmmss"},
            "audat":      {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "vbtyp":      {"_gen": "constant", "x": "C"},
            "auart":      {"_gen": "oneOf", "choices": ["ZOR","ZRE","ZFRE","ZKE","ZOB","ZOTB"]},
            "vkorg":      {"_gen": "oneOf", "choices": ["BR01","BR10","BR51","BR55","BR41","BR45"]},
            "vtweg":      {"_gen": "oneOf", "choices": ["10","20","30","40"]},
            "spart":      {"_gen": "oneOf", "choices": ["01","02","05","06","07"]},
            "vkbur":      {"_gen": "oneOf", "choices": ["BR01","BR02","BR03","BR04","BR05"]},
            "vkgrp":      {"_gen": "oneOf", "choices": ["001","002","003","004","005"]},
            "kunnr":      {"_gen": "lookup", "container": "sap", "keyPrefix": "kna1/customers-", "path": ["data", "kunnr"]},
            "vbeln_grp":  {"_gen": "weightedOneOf", "choices": [
                {"weight": 70, "value": None},
                {"weight": 30, "value": {"_gen": "digitString", "n": 10}},
            ]},
            "knumv":      {"_gen": "digitString", "n": 10},
            "vsbed":      {"_gen": "oneOf", "choices": ["01","02","03","04","05","06"]},
            "vdatu":      {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "waerk":      {"_gen": "constant", "x": "BRL"},
            "netwr":      {"_gen": "normalDistribution", "mean": 120000, "sd": 80000, "decimals": 2},
            "lifsk":      {"_gen": "weightedOneOf", "choices": [
                {"weight": 85, "value": None},
                {"weight": 15, "value": {"_gen": "oneOf", "choices": ["01","02","Z1","Z2"]}},
            ]},
            "faksk":      {"_gen": "weightedOneOf", "choices": [
                {"weight": 88, "value": None},
                {"weight": 12, "value": {"_gen": "oneOf", "choices": ["01","Z1","Z2"]}},
            ]},
            "gbstk":      {"_gen": "oneOf", "choices": ["A","B","C"]},
            "vgbel":      {"_gen": "weightedOneOf", "choices": [
                {"weight": 80, "value": None},
                {"weight": 20, "value": {"_gen": "digitString", "n": 10}},
            ]},
            "augru":      {"_gen": "weightedOneOf", "choices": [
                {"weight": 75, "value": None},
                {"weight": 25, "value": {"_gen": "oneOf", "choices": ["001","002","003","004","005"]}},
            ]},
            "zzcod_deal": {"_gen": "weightedOneOf", "choices": [
                {"weight": 60, "value": None},
                {"weight": 40, "value": {"_gen": "digitString", "n": 10}},
            ]},
            "zzknumh":    {"_gen": "digitString", "n": 10},
        }),

        TableSpec("vbap", "vbap/order-items-", 15000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "vbeln":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbak/orders-", "path": ["data", "vbeln"]},
            "posnr":  {"_gen": "cycle", "sequence": ["000010","000020","000030","000040","000050"]},
            "matnr":  {"_gen": "lookup", "container": "sap", "keyPrefix": "mara/materials-", "path": ["data", "matnr"]},
            "matkl":  {"_gen": "lookup", "container": "sap", "keyPrefix": "mara/materials-", "path": ["data", "matkl"]},
            "werks":  {"_gen": "oneOf", "choices": ["BR01","BR10","BR51","BR55","BR41"]},
            "lgort":  {"_gen": "digitString", "n": 4},
            "vstel":  {"_gen": "oneOf", "choices": ["BR01","BR02","BR03","BR04"]},
            "kwmeng": {"_gen": "uniformDistribution", "bounds": [1, 9999], "decimals": 3},
            "kbmeng": {"_gen": "uniformDistribution", "bounds": [0, 9999], "decimals": 3},
            "vrkme":  {"_gen": "lookup", "container": "sap", "keyPrefix": "mara/materials-", "path": ["data", "meins"]},
            "meins":  {"_gen": "lookup", "container": "sap", "keyPrefix": "mara/materials-", "path": ["data", "meins"]},
            "netwr":  {"_gen": "normalDistribution", "mean": 15000, "sd": 8000, "decimals": 2},
            "waerk":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbak/orders-", "path": ["data", "waerk"]},
            "route":  {"_gen": "oneOf", "choices": ["0001","0002","0003","0004","BR01","BR02"]},
            "abdat":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "kdmat":  {"_gen": "digitString", "n": 35},
            "charg":  {"_gen": "digitString", "n": 10},
            "posex":  {"_gen": "digitString", "n": 6},
            "fkrel":  {"_gen": "constant", "x": "A"},
            "abgru":  {"_gen": "weightedOneOf", "choices": [
                {"weight": 93, "value": None},
                {"weight": 7, "value": {"_gen": "oneOf", "choices": ["01","02","03","04","05"]}},
            ]},
            "lfsta":  {"_gen": "oneOf", "choices": ["A","B","C"]},
            "fksta":  {"_gen": "oneOf", "choices": ["A","B","C"]},
        }),

        TableSpec("vbep", "vbep/schedule-lines-", 15000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "vbeln":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "vbeln"]},
            "posnr":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "posnr"]},
            "etenr":  {"_gen": "cycle", "sequence": ["0001","0002"]},
            "wmeng":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "kwmeng"]},
            "bmeng":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "kbmeng"]},
            "vrkme":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "vrkme"]},
            "edatu":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "wadat":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
        }),

        TableSpec("vbkd", "vbkd/commercial-", 5000, {
            "mandt":       {"_gen": "constant", "x": "100"},
            "vbeln":       {"_gen": "lookup", "container": "sap", "keyPrefix": "vbak/orders-", "path": ["data", "vbeln"]},
            "posnr":       {"_gen": "constant", "x": "000000"},
            "zterm":       {"_gen": "oneOf", "choices": ["0001","0002","0003","Z001","Z002","Z030","Z045","Z060"]},
            "konda":       {"_gen": "oneOf", "choices": ["01","02","03","04","05"]},
            "vsart":       {"_gen": "oneOf", "choices": ["01","02","03","04","05","06","07","08"]},
            "inco1":       {"_gen": "oneOf", "choices": ["CIF","FOB","FCA","DAP","EXW","CFR"]},
            "bstkd":       {"_gen": "digitString", "n": 35},
            "prsdt":       {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "kdkg1":       {"_gen": "weightedOneOf", "choices": [
                {"weight": 75, "value": None},
                {"weight": 12, "value": "1"},
                {"weight": 13, "value": "2"},
            ]},
            "kdkg2":       {"_gen": "weightedOneOf", "choices": [
                {"weight": 60, "value": None},
                {"weight": 40, "value": {"_gen": "digitString", "n": 3}},
            ]},
            "kdkg3":       {"_gen": "weightedOneOf", "choices": [
                {"weight": 80, "value": None},
                {"weight": 10, "value": "1"},
                {"weight": 10, "value": "2"},
            ]},
            "kdkg5":       {"_gen": "weightedOneOf", "choices": [
                {"weight": 80, "value": None},
                {"weight": 20, "value": {"_gen": "digitString", "n": 3}},
            ]},
            "zzdeal_efet": {"_gen": "weightedOneOf", "choices": [
                {"weight": 60, "value": None},
                {"weight": 40, "value": {"_gen": "digitString", "n": 10}},
            ]},
            "zzdeal_orig": {"_gen": "weightedOneOf", "choices": [
                {"weight": 60, "value": None},
                {"weight": 40, "value": {"_gen": "digitString", "n": 10}},
            ]},
        }),

        TableSpec("vbpa", "vbpa/partners-", 10000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "vbeln":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "vbeln"]},
            "posnr":  {"_gen": "constant", "x": "000000"},
            "parvw":  {"_gen": "oneOf", "choices": ["ZG","ZA","WE","RE","AG","TR"]},
            "kunnr":  {"_gen": "lookup", "container": "sap", "keyPrefix": "kna1/customers-", "path": ["data", "kunnr"]},
            "pernr":  {"_gen": "lookup", "container": "sap", "keyPrefix": "pa0001/employees-", "path": ["data", "pernr"]},
            "lifnr":  {"_gen": "weightedOneOf", "choices": [
                {"weight": 85, "value": None},
                {"weight": 15, "value": {"_gen": "digitString", "n": 10}},
            ]},
        }),

        TableSpec("vbuk", "vbuk/order-status-", 5000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "vbeln":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbak/orders-", "path": ["data", "vbeln"]},
            "cmgst":  {"_gen": "weightedOneOf", "choices": [
                {"weight": 70, "value": None},
                {"weight": 15, "value": "A"},
                {"weight": 10, "value": "B"},
                {"weight": 5, "value": "C"},
            ]},
            "lfstk":  {"_gen": "oneOf", "choices": ["A","B","C"]},
            "fkstk":  {"_gen": "oneOf", "choices": ["A","B","C"]},
            "gbstk":  {"_gen": "oneOf", "choices": ["A","B","C"]},
        }),

        TableSpec("konv", "konv/pricing-", 30000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "knumv":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbak/orders-", "path": ["data", "knumv"]},
            "kposn":  {"_gen": "cycle", "sequence": ["000010","000020","000030","000040","000050"]},
            "stunr":  {"_gen": "uniformDistribution", "bounds": [1, 99], "decimals": 0},
            "zaehk":  {"_gen": "uniformDistribution", "bounds": [1, 9], "decimals": 0},
            "kschl":  {"_gen": "oneOf", "choices": ["ZPB6","ZPB4","ZPB3","ZPB2","ZFOG","ZE01","ZK06","ZK04","VPRS","ZBB1","ZBB2","ZBB4","ZBB5","ZBB6","ZBB7","ZBK3"]},
            "kbetr":  {"_gen": "normalDistribution", "mean": 5000, "sd": 3000, "decimals": 4},
            "kwert":  {"_gen": "normalDistribution", "mean": 10000, "sd": 5000, "decimals": 2},
            "kwaeh":  {"_gen": "constant", "x": "BRL"},
            "kmein":  {"_gen": "oneOf", "choices": ["UN","KG","L","M3"]},
            "kpein":  {"_gen": "uniformDistribution", "bounds": [1, 1000], "decimals": 0},
        }),

        # ── Delivery ───────────────────────────────────────────────────────

        TableSpec("lips", "lips/delivery-items-", 15000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "vbeln":  {"_gen": "digitString", "n": 10},
            "posnr":  {"_gen": "cycle", "sequence": ["000010","000020","000030"]},
            "vgbel":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "vbeln"]},
            "vgpos":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "posnr"]},
            "matnr":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "matnr"]},
            "werks":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "werks"]},
            "lgort":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "lgort"]},
            "charg":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "charg"]},
            "lfimg":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "kwmeng"]},
            "lgmng":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "kwmeng"]},
            "vrkme":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "vrkme"]},
            "meins":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "meins"]},
            "aeskd":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "brgew":  {"_gen": "normalDistribution", "mean": 500, "sd": 300, "decimals": 3},
            "dtexp":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "dfexp":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "dpexp":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
        }),

        TableSpec("likp", "likp/delivery-header-", 5000, {
            "mandt":     {"_gen": "constant", "x": "100"},
            "vbeln":     {"_gen": "lookup", "container": "sap", "keyPrefix": "lips/delivery-items-", "path": ["data", "vbeln"]},
            "erdat":     {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "lfart":     {"_gen": "oneOf", "choices": ["LF","LR","NL","EL","RL"]},
            "vkorg":     {"_gen": "oneOf", "choices": ["BR01","BR10","BR51","BR55","BR41"]},
            "kunnr":     {"_gen": "digitString", "n": 10},
            "lfdat":     {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "wadat_ist": {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "trspg":     {"_gen": "weightedOneOf", "choices": [
                {"weight": 80, "value": None},
                {"weight": 20, "value": {"_gen": "oneOf", "choices": ["01","02","Z1"]}},
            ]},
            "tdlnr":     {"_gen": "lookup", "container": "sap", "keyPrefix": "lfa1/vendors-", "path": ["data", "lifnr"]},
        }),

        TableSpec("vbup", "vbup/item-status-", 15000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "vbeln":  {"_gen": "lookup", "container": "sap", "keyPrefix": "lips/delivery-items-", "path": ["data", "vbeln"]},
            "posnr":  {"_gen": "lookup", "container": "sap", "keyPrefix": "lips/delivery-items-", "path": ["data", "posnr"]},
            "lfsta":  {"_gen": "oneOf", "choices": ["A","B","C"]},
            "fksta":  {"_gen": "oneOf", "choices": ["A","B","C"]},
            "kosta":  {"_gen": "oneOf", "choices": ["A","B","C"]},
        }),

        TableSpec("oigsi", "oigsi/shipments-", 3000, {
            "mandt":      {"_gen": "constant", "x": "100"},
            "shnumber":   {"_gen": "digitString", "n": 10},
            "doc_number": {"_gen": "lookup", "container": "sap", "keyPrefix": "likp/delivery-header-", "path": ["data", "vbeln"]},
            "doc_typ":    {"_gen": "constant", "x": "J"},
            "erdat":      {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
        }),

        TableSpec("oigs", "oigs/ot-header-", 1500, {
            "mandt":      {"_gen": "constant", "x": "100"},
            "shnumber":   {"_gen": "digitString", "n": 10},
            "doc_number": {"_gen": "lookup", "container": "sap", "keyPrefix": "likp/delivery-header-", "path": ["data", "vbeln"]},
            "doc_typ":    {"_gen": "constant", "x": "J"},
            "status":     {"_gen": "oneOf", "choices": ["A","B","C","D","E"]},
            "erdat":      {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
        }),

        # ── Transport ──────────────────────────────────────────────────────

        TableSpec("vttp", "vttp/transport-items-", 5000, {
            "mandt": {"_gen": "constant", "x": "100"},
            "tknum": {"_gen": "digitString", "n": 10},
            "tpnum": {"_gen": "uniformDistribution", "bounds": [1, 10], "decimals": 0},
            "vbeln": {"_gen": "lookup", "container": "sap", "keyPrefix": "likp/delivery-header-", "path": ["data", "vbeln"]},
        }),

        TableSpec("vttk", "vttk/transport-header-", 2000, {
            "mandt": {"_gen": "constant", "x": "100"},
            "tknum": {"_gen": "lookup", "container": "sap", "keyPrefix": "vttp/transport-items-", "path": ["data", "tknum"]},
            "shtyp": {"_gen": "oneOf", "choices": ["0001","0002","0003","0004"]},
            "route": {"_gen": "oneOf", "choices": ["0001","0002","0003","0004","BR01","BR02"]},
            "sttrg": {"_gen": "oneOf", "choices": ["1","2","3","4","5"]},
            "tdlnr": {"_gen": "lookup", "container": "sap", "keyPrefix": "lfa1/vendors-", "path": ["data", "lifnr"]},
        }),

        # ── Billing ────────────────────────────────────────────────────────

        TableSpec("vbrp", "vbrp/invoice-items-", 12000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "vbeln":  {"_gen": "digitString", "n": 10},
            "posnr":  {"_gen": "lookup", "container": "sap", "keyPrefix": "lips/delivery-items-", "path": ["data", "posnr"]},
            "vgbel":  {"_gen": "lookup", "container": "sap", "keyPrefix": "lips/delivery-items-", "path": ["data", "vbeln"]},
            "vgpos":  {"_gen": "lookup", "container": "sap", "keyPrefix": "lips/delivery-items-", "path": ["data", "posnr"]},
            "aubel":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "vbeln"]},
            "aupos":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "posnr"]},
            "matnr":  {"_gen": "lookup", "container": "sap", "keyPrefix": "lips/delivery-items-", "path": ["data", "matnr"]},
            "fklmg":  {"_gen": "lookup", "container": "sap", "keyPrefix": "lips/delivery-items-", "path": ["data", "lfimg"]},
            "meins":  {"_gen": "lookup", "container": "sap", "keyPrefix": "lips/delivery-items-", "path": ["data", "meins"]},
            "netwr":  {"_gen": "normalDistribution", "mean": 15000, "sd": 8000, "decimals": 2},
            "mwsbp":  {"_gen": "normalDistribution", "mean": 1500, "sd": 800, "decimals": 2},
            "waerk":  {"_gen": "constant", "x": "BRL"},
        }),

        TableSpec("vbrk", "vbrk/billing-header-", 4000, {
            "mandt":      {"_gen": "constant", "x": "100"},
            "vbeln":      {"_gen": "lookup", "container": "sap", "keyPrefix": "vbrp/invoice-items-", "path": ["data", "vbeln"]},
            "fkart":      {"_gen": "oneOf", "choices": ["ZF2","ZRE","ZS1","ZG2","ZFI","ZL2"]},
            "vbtyp":      {"_gen": "constant", "x": "M"},
            "erdat":      {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "fkdat":      {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "kunrg":      {"_gen": "digitString", "n": 10},
            "waerk":      {"_gen": "constant", "x": "BRL"},
            "netwr":      {"_gen": "lookup", "container": "sap", "keyPrefix": "vbrp/invoice-items-", "path": ["data", "netwr"]},
            "mwsbk":      {"_gen": "lookup", "container": "sap", "keyPrefix": "vbrp/invoice-items-", "path": ["data", "mwsbp"]},
            "knumv":      {"_gen": "lookup", "container": "sap", "keyPrefix": "vbak/orders-", "path": ["data", "knumv"]},
            "sfakn":      {"_gen": "weightedOneOf", "choices": [
                {"weight": 90, "value": None},
                {"weight": 10, "value": {"_gen": "digitString", "n": 10}},
            ]},
            "fksto":      {"_gen": "weightedOneOf", "choices": [
                {"weight": 95, "value": None},
                {"weight": 5, "value": "X"},
            ]},
            "zlsch":      {"_gen": "oneOf", "choices": ["B","D","T","R","C"]},
            "zzcod_deal": {"_gen": "weightedOneOf", "choices": [
                {"weight": 60, "value": None},
                {"weight": 40, "value": {"_gen": "digitString", "n": 10}},
            ]},
        }),

        # ── NF-e (Nota Fiscal) ─────────────────────────────────────────────

        TableSpec("j_1bnfdoc", "j_1bnfdoc/nf-header-", 4000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "docnum": {"_gen": "digitString", "n": 16},
            "bukrs":  {"_gen": "oneOf", "choices": ["BR01","BR10","BR51","BR55"]},
            "bupla":  {"_gen": "digitString", "n": 4},
            "stcd1":  {"_gen": "digitString", "n": 14},
            "direct": {"_gen": "oneOf", "choices": ["O","I"]},
            "docdat": {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "refkey": {"_gen": "lookup", "container": "sap", "keyPrefix": "vbrk/billing-header-", "path": ["data", "vbeln"]},
            "nfnum":  {"_gen": "digitString", "n": 9},
            "serie":  {"_gen": "oneOf", "choices": ["1","2","3","A","B"]},
        }),

        TableSpec("j_1bnflin", "j_1bnflin/nf-items-", 12000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "docnum": {"_gen": "lookup", "container": "sap", "keyPrefix": "j_1bnfdoc/nf-header-", "path": ["data", "docnum"]},
            "itmnum": {"_gen": "cycle", "sequence": ["000010","000020","000030","000040","000050"]},
            "matnr":  {"_gen": "lookup", "container": "sap", "keyPrefix": "mara/materials-", "path": ["data", "matnr"]},
            "cfop":   {"_gen": "oneOf", "choices": ["6101","6102","6103","6104","6401","6402","6403","6404","5101","5102"]},
            "nfqtd":  {"_gen": "uniformDistribution", "bounds": [1, 9999], "decimals": 3},
            "nfpric": {"_gen": "normalDistribution", "mean": 5000, "sd": 3000, "decimals": 2},
            "nfval":  {"_gen": "normalDistribution", "mean": 15000, "sd": 8000, "decimals": 2},
        }),

        TableSpec("j_1bnfstx", "j_1bnfstx/nf-taxes-", 36000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "docnum": {"_gen": "lookup", "container": "sap", "keyPrefix": "j_1bnflin/nf-items-", "path": ["data", "docnum"]},
            "itmnum": {"_gen": "lookup", "container": "sap", "keyPrefix": "j_1bnflin/nf-items-", "path": ["data", "itmnum"]},
            "taxtyp": {"_gen": "cycle", "sequence": ["IPI","ICMS","PIS","COFINS","CSLL","ISS"]},
            "taxval": {"_gen": "normalDistribution", "mean": 1500, "sd": 800, "decimals": 2},
            "taxrat": {"_gen": "uniformDistribution", "bounds": [0.5, 25.0], "decimals": 2},
        }),

        # ── Accounting (FI) ────────────────────────────────────────────────

        TableSpec("bkpf", "bkpf/accounting-", 4000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "bukrs":  {"_gen": "oneOf", "choices": ["BR01","BR10","BR51","BR55"]},
            "belnr":  {"_gen": "digitString", "n": 10},
            "gjahr":  {"_gen": "oneOf", "choices": ["2023","2024","2025","2026"]},
            "awtyp":  {"_gen": "constant", "x": "VBRK"},
            "awkey":  {"_gen": "lookup", "container": "sap", "keyPrefix": "vbrk/billing-header-", "path": ["data", "vbeln"]},
            "budat":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "bldat":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "waers":  {"_gen": "constant", "x": "BRL"},
        }),

        # ── Purchasing (MM) ────────────────────────────────────────────────

        TableSpec("ekko", "ekko/po-header-", 500, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "ebeln":  {"_gen": "digitString", "n": 10},
            "bukrs":  {"_gen": "oneOf", "choices": ["BR01","BR10","BR51","BR55"]},
            "bsart":  {"_gen": "oneOf", "choices": ["NB","FO","UB","MK","MPN"]},
            "aedat":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "ernam":  {"_gen": "string", "expr": "#{Internet.slug}"},
            "lifnr":  {"_gen": "lookup", "container": "sap", "keyPrefix": "lfa1/vendors-", "path": ["data", "lifnr"]},
            "ekgrp":  {"_gen": "oneOf", "choices": ["001","002","003","004","005","B01","B02","B03"]},
            "bedat":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "waers":  {"_gen": "constant", "x": "BRL"},
        }),

        TableSpec("ekpo", "ekpo/po-items-", 2000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "ebeln":  {"_gen": "lookup", "container": "sap", "keyPrefix": "ekko/po-header-", "path": ["data", "ebeln"]},
            "ebelp":  {"_gen": "cycle", "sequence": ["00010","00020","00030","00040","00050"]},
            "matnr":  {"_gen": "lookup", "container": "sap", "keyPrefix": "mara/materials-", "path": ["data", "matnr"]},
            "werks":  {"_gen": "oneOf", "choices": ["BR01","BR10","BR51","BR55","BR41"]},
            "lgort":  {"_gen": "digitString", "n": 4},
            "menge":  {"_gen": "uniformDistribution", "bounds": [1, 9999], "decimals": 3},
            "meins":  {"_gen": "lookup", "container": "sap", "keyPrefix": "mara/materials-", "path": ["data", "meins"]},
            "netpr":  {"_gen": "normalDistribution", "mean": 5000, "sd": 3000, "decimals": 2},
            "peinh":  {"_gen": "oneOf", "choices": ["1","10","100","1000"]},
            "waers":  {"_gen": "constant", "x": "BRL"},
        }),

        TableSpec("eket", "eket/po-schedule-", 2500, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "ebeln":  {"_gen": "lookup", "container": "sap", "keyPrefix": "ekpo/po-items-", "path": ["data", "ebeln"]},
            "ebelp":  {"_gen": "lookup", "container": "sap", "keyPrefix": "ekpo/po-items-", "path": ["data", "ebelp"]},
            "etenr":  {"_gen": "cycle", "sequence": ["0001","0002","0003"]},
            "eindt":  {"_gen": "formatDateTime", "ms": {"_gen": "now"}, "format": "yyyyMMdd"},
            "menge":  {"_gen": "uniformDistribution", "bounds": [1, 9999], "decimals": 3},
            "wemng":  {"_gen": "uniformDistribution", "bounds": [0, 9999], "decimals": 3},
        }),

        TableSpec("ekbe", "ekbe/po-history-", 3000, {
            "mandt":  {"_gen": "constant", "x": "100"},
            "ebeln":  {"_gen": "lookup", "container": "sap", "keyPrefix": "ekpo/po-items-", "path": ["data", "ebeln"]},
            "ebelp":  {"_gen": "lookup", "container": "sap", "keyPrefix": "ekpo/po-items-", "path": ["data", "ebelp"]},
            "zekkn":  {"_gen": "uniformDistribution", "bounds": [1, 99], "decimals": 0},
            "vgabe":  {"_gen": "oneOf", "choices": ["1","2","3","4","5","6","7","8","9"]},
            "gjahr":  {"_gen": "oneOf", "choices": ["2023","2024","2025","2026"]},
            "belnr":  {"_gen": "digitString", "n": 10},
            "buzei":  {"_gen": "cycle", "sequence": ["001","002","003","004","005"]},
            "menge":  {"_gen": "uniformDistribution", "bounds": [1, 9999], "decimals": 3},
            "wrbtr":  {"_gen": "normalDistribution", "mean": 10000, "sd": 5000, "decimals": 2},
            "waers":  {"_gen": "constant", "x": "BRL"},
        }),

        # ── Document Flow ──────────────────────────────────────────────────

        TableSpec("vbfa", "vbfa/doc-flow-", 20000, {
            "mandt":   {"_gen": "constant", "x": "100"},
            "vbeln":   {"_gen": "digitString", "n": 10},
            "posnr":   {"_gen": "cycle", "sequence": ["000010","000020","000030"]},
            "vbelv":   {"_gen": "lookup", "container": "sap", "keyPrefix": "vbak/orders-", "path": ["data", "vbeln"]},
            "posnv":   {"_gen": "lookup", "container": "sap", "keyPrefix": "vbap/order-items-", "path": ["data", "posnr"]},
            "vbtyp_n": {"_gen": "oneOf", "choices": ["J","M","R"]},
            "vbtyp_v": {"_gen": "constant", "x": "C"},
            "rfmng":   {"_gen": "uniformDistribution", "bounds": [1, 999], "decimals": 3},
            "rfwrt":   {"_gen": "normalDistribution", "mean": 15000, "sd": 8000, "decimals": 2},
            "waers":   {"_gen": "constant", "x": "BRL"},
        }),

    ]


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run() -> None:
    """Generate all SAP tables and write Parquet files."""
    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

    specs = _build_table_specs()
    total_tables = len(specs)

    logger.info("SAP Data Generator starting — %d tables, output: %s", total_tables, OUTPUT_BASE)

    total_records = 0
    for i, spec in enumerate(specs, start=1):
        logger.info("[%d/%d] Generating %s (%d records)...", i, total_tables, spec.table_name, spec.max_events)

        df = generate_table(spec)
        out_path = save_parquet(df, spec.table_name)
        total_records += len(df)

        logger.info("  -> Saved %s (%d rows, %.1f KB)",
                    out_path.name, len(df), out_path.stat().st_size / 1024)

    logger.info("Done. %d tables | %d total records | Output: %s",
                total_tables, total_records, OUTPUT_BASE)

    # Print summary table
    print("\n" + "="*60)
    print(f"{'Table':<20} {'Rows':>8} {'File Size':>12}")
    print("-"*60)
    for spec in specs:
        out_path = OUTPUT_BASE / spec.table_name / f"{spec.table_name}-0.parquet"
        if out_path.exists():
            size_kb = out_path.stat().st_size / 1024
            print(f"{spec.table_name:<20} {spec.max_events:>8,} {size_kb:>10.1f} KB")
    print("="*60)


if __name__ == "__main__":
    try:
        import pandas  # noqa: F401
        import pyarrow  # noqa: F401
        from faker import Faker  # noqa: F401
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install pandas pyarrow faker tqdm")
        sys.exit(1)

    run()
