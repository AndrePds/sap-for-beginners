"""
bronze_schemas.py  –  Explicit PySpark StructTypes for all 41 SAP source tables.

Loaded as the FIRST library in the Bronze DLT pipeline so that
bronze_pipeline.py can reference BRONZE_SCHEMAS and ALL_BRONZE_TABLES.

SAP-type -> Bronze Python-type mapping:
  CHAR / NUMC / CUKY / CLNT -> StringType   (preserve leading zeros)
  DATS (YYYYMMDD)           -> StringType   (cast to DateType in Silver)
  TIMS (HHMMSS)             -> StringType
  CURR / DEC / QUAN         -> DoubleType
  INT4 / INT2               -> IntegerType
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
)

def _s(name): return StructField(name, StringType(), True)
def _d(name): return StructField(name, DoubleType(),  True)
def _i(name): return StructField(name, IntegerType(), True)

# ---------------------------------------------------------------------------
# Explicit schemas
# ---------------------------------------------------------------------------
BRONZE_SCHEMAS = {

    # == MASTER DATA ===========================================================

    "kna1": StructType([_s("mandt"),_s("kunnr"),_s("name1"),_s("mcod1"),
                        _s("stcd1"),_s("ktokd"),_s("ort01"),_s("regio"),_s("land1")]),

    "lfa1": StructType([_s("mandt"),_s("lifnr"),_s("name1"),_s("mcod1"),
                        _s("ort01"),_s("regio"),_s("land1"),_s("stcd1")]),

    "mara": StructType([_s("mandt"),_s("matnr"),_s("matkl"),_s("meins"),
                        _s("gewei"),_s("prdha"),_d("ntgew"),_d("brgew"),_s("bismt")]),

    "makt": StructType([_s("mandt"),_s("matnr"),_s("spras"),_s("maktx")]),

    "pa0001": StructType([_s("mandt"),_s("pernr"),_s("vorna"),_s("nachn"),
                          _s("ename"),_s("begda"),_s("endda"),_s("plans"),_s("orgeh")]),

    "t001w": StructType([_s("mandt"),_s("werks"),_s("name1"),_s("bwkey"),
                         _s("bukrs"),_s("land1"),_s("regio"),_s("ort01")]),

    "knmt": StructType([_s("mandt"),_s("kunnr"),_s("matnr"),_s("vkorg"),
                        _s("vtweg"),_s("kdmat"),_s("maktx")]),

    # == SD SALES ORDER ========================================================

    "vbak": StructType([
        _s("mandt"),_s("vbeln"),_s("kunnr"),
        _s("audat"),_s("erdat"),_s("erzet"),_s("ernam"),
        _s("auart"),_s("vkorg"),_s("vtweg"),_s("spart"),_s("vbtyp"),
        _s("knumv"),_d("netwr"),_s("waerk"),
        _s("augru"),_s("faksk"),_s("lifsk"),_s("gbstk"),
        _s("vkgrp"),_s("vkbur"),_s("vsbed"),_s("vdatu"),_s("vgbel"),
        _s("zzcod_deal"),_s("zzknumh"),_s("vbeln_grp"),
    ]),

    "vbap": StructType([
        _s("mandt"),_s("vbeln"),_s("posnr"),
        _s("matnr"),_s("werks"),_s("lgort"),_s("meins"),_s("vrkme"),
        _d("netwr"),_s("waerk"),_d("kwmeng"),_d("kbmeng"),
        _s("charg"),_s("route"),_s("matkl"),_s("vstel"),
        _s("abdat"),_s("abgru"),_s("fksta"),_s("lfsta"),_s("fkrel"),
        _s("posex"),_s("kdmat"),
    ]),

    "vbep": StructType([_s("mandt"),_s("vbeln"),_s("posnr"),_s("etenr"),
                        _s("edatu"),_s("wadat"),_d("wmeng"),_d("bmeng"),_s("vrkme")]),

    "vbfa": StructType([_s("mandt"),_s("vbelv"),_s("posnv"),
                        _s("vbeln"),_s("posnr"),_s("vbtyp_n"),_s("vbtyp_v"),
                        _d("rfmng"),_d("rfwrt"),_s("waers")]),

    # == BILLING ===============================================================

    "vbrk": StructType([
        _s("mandt"),_s("vbeln"),
        _s("fkdat"),_s("erdat"),_s("fkart"),_s("vbtyp"),
        _s("kunrg"),_s("knumv"),_d("netwr"),_s("waerk"),_d("mwsbk"),
        _s("fksto"),_s("sfakn"),_s("zlsch"),_s("zzcod_deal"),
    ]),

    "vbrp": StructType([
        _s("mandt"),_s("vbeln"),_s("posnr"),
        _s("matnr"),_s("meins"),_d("netwr"),_s("waerk"),_d("fklmg"),_d("mwsbp"),
        _s("vgbel"),_s("vgpos"),_s("aupos"),_s("aubel"),
    ]),

    # == DELIVERY ==============================================================

    "likp": StructType([_s("mandt"),_s("vbeln"),_s("kunnr"),_s("vkorg"),_s("tdlnr"),
                        _s("lfdat"),_s("erdat"),_s("wadat_ist"),_s("lfart"),_s("trspg")]),

    "lips": StructType([
        _s("mandt"),_s("vbeln"),_s("posnr"),
        _s("matnr"),_s("meins"),_s("vrkme"),_s("lgort"),_s("werks"),_s("charg"),
        _d("lfimg"),_d("lgmng"),_d("brgew"),
        _s("vgbel"),_s("vgpos"),
        _s("dfexp"),_s("dtexp"),_s("dpexp"),_s("aeskd"),
    ]),

    # == TRANSPORT =============================================================

    "vttk": StructType([_s("mandt"),_s("tknum"),_s("shtyp"),
                        _s("route"),_s("sttrg"),_s("tdlnr")]),

    # == PURCHASING (MM-PUR) ===================================================

    "ekko": StructType([_s("mandt"),_s("ebeln"),_s("bukrs"),_s("lifnr"),
                        _s("ekgrp"),_s("bsart"),_s("bedat"),
                        _s("waers"),_s("ernam"),_s("aedat")]),

    "ekpo": StructType([_s("mandt"),_s("ebeln"),_s("ebelp"),
                        _s("matnr"),_s("werks"),_s("lgort"),
                        _d("menge"),_s("meins"),_d("netpr"),_s("peinh"),_s("waers")]),

    "eket": StructType([_s("mandt"),_s("ebeln"),_s("ebelp"),_s("etenr"),
                        _s("eindt"),_d("menge"),_d("wamng"),_s("waers")]),

    "ekbe": StructType([_s("mandt"),_s("ebeln"),_s("ebelp"),
                        _i("zekkn"),_s("belnr"),_s("gjahr"),_s("buzei"),
                        _s("vgabe"),_d("wrbtr"),_d("menge"),_s("waers")]),

    # == NOTA FISCAL (Brazil) ==================================================

    "j_1bnfdoc": StructType([_s("mandt"),_s("bukrs"),_s("docnum"),_s("nfnum"),
                              _s("bupla"),_s("serie"),_s("refkey"),
                              _s("stcd1"),_s("direct"),_s("docdat")]),

    "j_1bnflin": StructType([_s("mandt"),_s("docnum"),_s("itmnum"),
                              _s("matnr"),_s("cfop"),
                              _d("nfqtd"),_d("nfpric"),_d("nfval")]),

    "j_1bnfstx": StructType([_s("mandt"),_s("docnum"),_s("itmnum"),
                              _s("taxtyp"),_d("taxval"),_d("taxrat")]),

    # == FINANCIAL ACCOUNTING (FI) =============================================

    "bkpf": StructType([_s("mandt"),_s("bukrs"),_s("belnr"),_s("gjahr"),
                        _s("budat"),_s("bldat"),_s("waers"),
                        _s("awkey"),_s("awtyp")]),
}

# Tables using Auto Loader schema inference (config/text + variable-structure tables)
INFERRED_TABLES = [
    "dd07t", "dd07v", "t173t", "tvagt", "tvaut",
    "tvkggt", "tvrot", "tvsbt", "tvstt",
    "konv",
    "vbkd", "vbpa", "vbuk", "vbup",
    "vttp", "oigs", "oigsi",
]

# Full ordered list (master -> config -> transactional -> fiscal -> accounting)
ALL_BRONZE_TABLES = list(BRONZE_SCHEMAS.keys()) + INFERRED_TABLES
