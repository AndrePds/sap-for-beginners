# silver_pipeline.py  –  SAP Medallion Silver Layer
#
# L1: Type-safe, cleansed versions of Bronze tables.
#     * Filters mandt = 100
#     * Casts SAP date strings (YYYYMMDD) -> DateType
#     * Casts amounts/quantities -> DecimalType
#     * Adds DLT expectations on primary-key fields
#     * Adds _processed_time metadata
#
# L2: Domain models (denormalized joins of L1 tables):
#     * dim_customer       (KNA1)
#     * dim_material       (MARA + MAKT – Portuguese descriptions only)
#     * dim_vendor         (LFA1)
#     * fact_sales_order   (VBAK + VBAP)
#     * fact_billing       (VBRK + VBRP, cancelled excluded)
#     * fact_delivery      (LIKP + LIPS)
#     * fact_purchase_order (EKKO + EKPO + GR/IR from EKBE)
#     * fact_nota_fiscal   (J_1BNFDOC + J_1BNFLIN + J_1BNFSTX)
#
# Reads:  {bronze_catalog}.bronze_sap.bronze_<table>  (Delta streaming)
# Writes: catalog.silver_sap.<table>

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

BRONZE_CAT = spark.conf.get("bronze_catalog", "dev")
MANDT      = F.lit("100")

def _bronze(t):
    return spark.readStream.table(f"{BRONZE_CAT}.bronze_sap.bronze_{t}")

def _to_date(c):
    return F.to_date(c, "yyyyMMdd")

def _amt(c):
    return c.cast(DecimalType(17, 2))

def _qty(c):
    return c.cast(DecimalType(17, 3))


# =============================================================================
# L1 – MASTER DATA
# =============================================================================

@dlt.expect("kunnr_not_null", "kunnr IS NOT NULL")
@dlt.table(name="silver_kna1", comment="Customer master – typed, mandt=100 only.")
def silver_kna1():
    return (
        _bronze("kna1")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("kunnr"), F.col("name1"), F.col("mcod1"),
            F.col("stcd1").alias("cnpj_cpf"), F.col("ktokd"),
            F.col("ort01").alias("city"),
            F.col("regio").alias("state"),
            F.col("land1").alias("country"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.expect("lifnr_not_null", "lifnr IS NOT NULL")
@dlt.table(name="silver_lfa1", comment="Vendor master – typed, mandt=100 only.")
def silver_lfa1():
    return (
        _bronze("lfa1")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("lifnr"), F.col("name1"), F.col("mcod1"),
            F.col("ort01").alias("city"),
            F.col("regio").alias("state"),
            F.col("land1").alias("country"),
            F.col("stcd1").alias("cnpj_cpf"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.expect("matnr_not_null", "matnr IS NOT NULL")
@dlt.table(name="silver_mara", comment="Material master – typed quantities.")
def silver_mara():
    return (
        _bronze("mara")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("matnr"), F.col("matkl"), F.col("meins"),
            F.col("gewei"), F.col("prdha"),
            _qty(F.col("ntgew")).alias("ntgew"),
            _qty(F.col("brgew")).alias("brgew"),
            F.col("bismt"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.table(
    name="silver_makt",
    comment="Material descriptions – Portuguese only (spras=P)."
)
def silver_makt():
    return (
        _bronze("makt")
        .filter((F.col("mandt") == MANDT) & (F.col("spras") == F.lit("P")))
        .select(
            F.col("matnr"),
            F.col("maktx").alias("material_desc"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.expect("pernr_not_null", "pernr IS NOT NULL")
@dlt.table(name="silver_pa0001", comment="HR employee master – typed dates.")
def silver_pa0001():
    return (
        _bronze("pa0001")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("pernr"), F.col("ename"),
            _to_date(F.col("begda")).alias("start_date"),
            _to_date(F.col("endda")).alias("end_date"),
            F.col("plans"), F.col("orgeh"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


# =============================================================================
# L1 – SALES & DISTRIBUTION
# =============================================================================

@dlt.expect("vbeln_not_null", "vbeln IS NOT NULL")
@dlt.expect("kunnr_not_null", "kunnr IS NOT NULL")
@dlt.table(
    name="silver_vbak",
    comment="Sales order headers – DATS cast to DateType, netwr typed."
)
def silver_vbak():
    return (
        _bronze("vbak")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("vbeln"), F.col("kunnr"),
            _to_date(F.col("audat")).alias("order_date"),
            _to_date(F.col("erdat")).alias("created_date"),
            _to_date(F.col("vdatu")).alias("requested_delivery_date"),
            F.col("auart"), F.col("vkorg"), F.col("vtweg"),
            F.col("spart"), F.col("vbtyp"), F.col("knumv"),
            _amt(F.col("netwr")).alias("netwr"), F.col("waerk"),
            F.col("augru"), F.col("faksk"), F.col("lifsk"), F.col("gbstk"),
            F.col("vkgrp"), F.col("vkbur"), F.col("vsbed"), F.col("vgbel"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.expect_or_drop("vbeln_not_null", "vbeln IS NOT NULL")
@dlt.expect_or_drop("posnr_not_null", "posnr IS NOT NULL")
@dlt.table(
    name="silver_vbap",
    comment="Sales order items – quantities and amounts typed."
)
def silver_vbap():
    return (
        _bronze("vbap")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("vbeln"), F.col("posnr"), F.col("matnr"),
            F.col("werks"), F.col("lgort"), F.col("meins"), F.col("vrkme"),
            _amt(F.col("netwr")).alias("netwr"), F.col("waerk"),
            _qty(F.col("kwmeng")).alias("kwmeng"),
            _qty(F.col("kbmeng")).alias("kbmeng"),
            F.col("charg"), F.col("route"), F.col("matkl"),
            _to_date(F.col("abdat")).alias("requested_date"),
            F.col("fksta"), F.col("lfsta"), F.col("fkrel"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.expect("vbeln_not_null", "vbeln IS NOT NULL")
@dlt.table(
    name="silver_vbrk",
    comment="Billing headers – typed, fksto IS NOT NULL means cancelled."
)
def silver_vbrk():
    return (
        _bronze("vbrk")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("vbeln"),
            _to_date(F.col("fkdat")).alias("billing_date"),
            _to_date(F.col("erdat")).alias("created_date"),
            F.col("fkart"), F.col("vbtyp"), F.col("kunrg"), F.col("knumv"),
            _amt(F.col("netwr")).alias("netwr"), F.col("waerk"),
            _amt(F.col("mwsbk")).alias("tax_amount"),
            F.col("fksto").alias("cancelled_flag"),
            F.col("sfakn").alias("cancel_ref_doc"),
            F.col("zlsch"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.expect_or_drop("vbeln_not_null", "vbeln IS NOT NULL")
@dlt.table(
    name="silver_vbrp",
    comment="Billing items – typed. Join to order via aubel+aupos."
)
def silver_vbrp():
    return (
        _bronze("vbrp")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("vbeln"), F.col("posnr"), F.col("matnr"), F.col("meins"),
            _amt(F.col("netwr")).alias("netwr"), F.col("waerk"),
            _qty(F.col("fklmg")).alias("billed_qty"),
            _amt(F.col("mwsbp")).alias("item_tax"),
            F.col("vgbel"), F.col("vgpos"), F.col("aubel"), F.col("aupos"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


# =============================================================================
# L1 – DELIVERY
# =============================================================================

@dlt.expect("vbeln_not_null", "vbeln IS NOT NULL")
@dlt.table(name="silver_likp", comment="Delivery headers – typed dates.")
def silver_likp():
    return (
        _bronze("likp")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("vbeln"), F.col("kunnr"), F.col("vkorg"), F.col("tdlnr"),
            _to_date(F.col("lfdat")).alias("planned_delivery_date"),
            _to_date(F.col("wadat_ist")).alias("actual_goods_issue_date"),
            _to_date(F.col("erdat")).alias("created_date"),
            F.col("lfart"), F.col("trspg"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.expect_or_drop("vbeln_not_null", "vbeln IS NOT NULL")
@dlt.table(name="silver_lips", comment="Delivery items – typed quantities.")
def silver_lips():
    return (
        _bronze("lips")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("vbeln"), F.col("posnr"), F.col("matnr"),
            F.col("meins"), F.col("vrkme"), F.col("lgort"), F.col("werks"),
            _qty(F.col("lfimg")).alias("delivered_qty"),
            F.col("vgbel"), F.col("vgpos"),
            _to_date(F.col("dfexp")).alias("export_date"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


# =============================================================================
# L1 – PURCHASING (MM-PUR)
# =============================================================================

@dlt.expect("ebeln_not_null", "ebeln IS NOT NULL")
@dlt.table(name="silver_ekko", comment="PO headers – typed dates.")
def silver_ekko():
    return (
        _bronze("ekko")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("ebeln"), F.col("bukrs"), F.col("lifnr"),
            F.col("ekgrp"), F.col("bsart"),
            _to_date(F.col("bedat")).alias("po_date"),
            _to_date(F.col("aedat")).alias("last_change_date"),
            F.col("waers"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.expect_or_drop("ebeln_not_null", "ebeln IS NOT NULL")
@dlt.table(name="silver_ekpo", comment="PO items – typed quantities and prices.")
def silver_ekpo():
    return (
        _bronze("ekpo")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("ebeln"), F.col("ebelp"), F.col("matnr"),
            F.col("werks"), F.col("meins"),
            _qty(F.col("menge")).alias("qty_ordered"),
            _amt(F.col("netpr")).alias("net_price"),
            F.col("peinh").alias("price_unit"), F.col("waers"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.expect("ebeln_not_null", "ebeln IS NOT NULL")
@dlt.table(
    name="silver_ekbe",
    comment=(
        "PO history movements. "
        "vgabe codes: 1=GR, 2=IR, 3=Subsequent debit, 4=Return, 6=Cancellation."
    )
)
def silver_ekbe():
    return (
        _bronze("ekbe")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("ebeln"), F.col("ebelp"), F.col("zekkn"),
            F.col("belnr"), F.col("gjahr"), F.col("vgabe"),
            _qty(F.col("menge")).alias("qty"),
            _amt(F.col("wrbtr")).alias("amount"), F.col("waers"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


# =============================================================================
# L1 – NOTA FISCAL (Brazil)
# =============================================================================

@dlt.expect("docnum_not_null", "docnum IS NOT NULL")
@dlt.table(
    name="silver_j_1bnfdoc",
    comment="NF header – direct=O outbound, direct=I inbound."
)
def silver_j_1bnfdoc():
    return (
        _bronze("j_1bnfdoc")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("docnum"), F.col("bukrs"), F.col("nfnum"),
            F.col("bupla"), F.col("serie"), F.col("refkey"),
            F.col("stcd1"), F.col("direct"),
            _to_date(F.col("docdat")).alias("docdat"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.expect_or_drop("docnum_not_null", "docnum IS NOT NULL")
@dlt.table(name="silver_j_1bnflin", comment="NF items with CFOP fiscal codes.")
def silver_j_1bnflin():
    return (
        _bronze("j_1bnflin")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("docnum"), F.col("itmnum"), F.col("matnr"), F.col("cfop"),
            _qty(F.col("nfqtd")).alias("nf_qty"),
            _amt(F.col("nfpric")).alias("nf_unit_price"),
            _amt(F.col("nfval")).alias("nf_value"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


@dlt.expect_or_drop("docnum_not_null", "docnum IS NOT NULL")
@dlt.table(
    name="silver_j_1bnfstx",
    comment="NF tax lines – taxtyp values: IPI, ICMS, PIS, COFINS, CSLL, ISS."
)
def silver_j_1bnfstx():
    return (
        _bronze("j_1bnfstx")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("docnum"), F.col("itmnum"), F.col("taxtyp"),
            _amt(F.col("taxval")).alias("tax_value"),
            F.col("taxrat").cast(DecimalType(7, 4)).alias("tax_rate"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


# =============================================================================
# L1 – FINANCIAL ACCOUNTING (FI)
# =============================================================================

@dlt.expect("belnr_not_null", "belnr IS NOT NULL")
@dlt.table(name="silver_bkpf", comment="FI accounting headers – typed posting dates.")
def silver_bkpf():
    return (
        _bronze("bkpf")
        .filter(F.col("mandt") == MANDT)
        .select(
            F.col("bukrs"), F.col("belnr"), F.col("gjahr"),
            _to_date(F.col("budat")).alias("posting_date"),
            _to_date(F.col("bldat")).alias("document_date"),
            F.col("waers"), F.col("awkey"), F.col("awtyp"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


# =============================================================================
# L2 – DOMAIN MODELS  (join Silver L1 tables into business entities)
# =============================================================================

@dlt.table(
    name="dim_customer",
    comment="Customer dimension. Grain: one row per kunnr."
)
def dim_customer():
    return dlt.read("silver_kna1")


@dlt.table(
    name="dim_material",
    comment=(
        "Material dimension: MARA joined with Portuguese descriptions (MAKT spras=P). "
        "Grain: one row per matnr."
    )
)
def dim_material():
    mara = dlt.read("silver_mara")
    makt = dlt.read("silver_makt")
    return (
        mara.join(makt, "matnr", "left")
        .select(
            mara["matnr"], mara["matkl"], mara["meins"],
            mara["prdha"], mara["ntgew"], mara["brgew"],
            makt["material_desc"],
        )
    )


@dlt.table(
    name="dim_vendor",
    comment="Vendor dimension. Grain: one row per lifnr."
)
def dim_vendor():
    return dlt.read("silver_lfa1")


@dlt.table(
    name="fact_sales_order",
    comment=(
        "Sales order fact: VBAK header joined to VBAP items. "
        "Grain: one row per vbeln + posnr."
    )
)
def fact_sales_order():
    hdr  = dlt.read("silver_vbak")
    item = dlt.read("silver_vbap")
    return (
        item.join(hdr, "vbeln", "inner")
        .select(
            item["vbeln"], item["posnr"],
            hdr["kunnr"], hdr["order_date"],
            hdr["auart"].alias("order_type"),
            hdr["vkorg"], hdr["vtweg"], hdr["spart"],
            hdr["gbstk"].alias("overall_status"),
            item["matnr"], item["werks"], item["meins"],
            item["kwmeng"].alias("qty_ordered"),
            item["netwr"].alias("item_net_value"), item["waerk"],
            item["fksta"].alias("billing_status"),
            item["lfsta"].alias("delivery_status"),
        )
    )


@dlt.table(
    name="fact_billing",
    comment=(
        "Billing fact: VBRK + VBRP. Cancelled invoices (fksto IS NOT NULL) excluded. "
        "Item chain: vbeln+posnr -> vgbel+vgpos (delivery) -> aubel+aupos (order). "
        "Grain: one row per vbeln + posnr."
    )
)
def fact_billing():
    hdr  = dlt.read("silver_vbrk").filter(F.col("cancelled_flag").isNull())
    item = dlt.read("silver_vbrp")
    return (
        item.join(hdr, "vbeln", "inner")
        .select(
            item["vbeln"], item["posnr"],
            hdr["kunrg"].alias("kunnr"),
            hdr["billing_date"],
            hdr["fkart"].alias("billing_type"),
            hdr["waerk"],
            item["matnr"], item["meins"],
            item["billed_qty"],
            item["netwr"].alias("item_net_value"),
            item["item_tax"],
            hdr["tax_amount"].alias("doc_tax"),
            item["vgbel"].alias("source_delivery"),
            item["aubel"].alias("source_order"),
        )
    )


@dlt.table(
    name="fact_delivery",
    comment=(
        "Delivery fact: LIKP + LIPS. "
        "Item join key: lips.vgbel+vgpos = vbap.vbeln+posnr (see sap-table-relationships.md). "
        "Grain: one row per vbeln + posnr."
    )
)
def fact_delivery():
    hdr  = dlt.read("silver_likp")
    item = dlt.read("silver_lips")
    return (
        item.join(hdr, "vbeln", "inner")
        .select(
            item["vbeln"], item["posnr"], hdr["kunnr"],
            hdr["planned_delivery_date"],
            hdr["actual_goods_issue_date"],
            item["matnr"], item["meins"],
            item["delivered_qty"],
            item["vgbel"].alias("source_order"),
            item["vgpos"].alias("source_order_item"),
        )
    )


@dlt.table(
    name="fact_purchase_order",
    comment=(
        "PO fact: EKKO + EKPO joined with GR/IR totals aggregated from EKBE. "
        "qty_received: sum of vgabe=1 (Goods Receipt). "
        "amount_invoiced: sum of vgabe=2 (Invoice Receipt). "
        "Grain: one row per ebeln + ebelp."
    )
)
def fact_purchase_order():
    hdr  = dlt.read("silver_ekko")
    item = dlt.read("silver_ekpo")
    hist = (
        dlt.read("silver_ekbe")
        .groupBy("ebeln", "ebelp")
        .agg(
            F.sum(F.when(F.col("vgabe") == F.lit("1"), F.col("qty"))).alias("qty_received"),
            F.sum(F.when(F.col("vgabe") == F.lit("2"), F.col("amount"))).alias("amount_invoiced"),
        )
    )
    return (
        item.join(hdr, "ebeln", "inner")
            .join(hist, ["ebeln", "ebelp"], "left")
        .select(
            item["ebeln"], item["ebelp"],
            hdr["lifnr"], hdr["bukrs"], hdr["po_date"],
            hdr["bsart"].alias("po_type"),
            item["matnr"], item["meins"],
            item["qty_ordered"], item["net_price"],
            F.col("qty_received"),
            F.col("amount_invoiced"),
        )
    )


@dlt.table(
    name="fact_nota_fiscal",
    comment=(
        "NF fact: J_1BNFDOC + J_1BNFLIN + J_1BNFSTX. "
        "Brazilian Nota Fiscal with CFOP codes and tax breakdown. "
        "Grain: one row per docnum + itmnum + taxtyp."
    )
)
def fact_nota_fiscal():
    doc = dlt.read("silver_j_1bnfdoc")
    lin = dlt.read("silver_j_1bnflin")
    tax = dlt.read("silver_j_1bnfstx")
    return (
        lin
        .join(doc, "docnum", "inner")
        .join(tax, ["docnum", "itmnum"], "left")
        .select(
            doc["docnum"], lin["itmnum"],
            doc["bukrs"], doc["nfnum"], doc["bupla"], doc["serie"],
            doc["direct"], doc["docdat"],
            lin["matnr"], lin["cfop"],
            lin["nf_qty"], lin["nf_value"],
            F.col("taxtyp"), F.col("tax_value"), F.col("tax_rate"),
        )
    )
