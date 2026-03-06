# gold_pipeline.py  –  SAP Medallion Gold Layer
#
# Business-ready KPI tables aggregated from Silver domain models.
# Each table has a documented grain optimised for dashboards / BI tools.
#
# Gold reads Silver as batch (not streaming) because aggregations are
# idempotent and scheduled to run after Silver is complete.
#
# Reads:  {silver_catalog}.silver_sap.<domain_model>  (batch Delta reads)
# Writes: catalog.gold_sap.<kpi_table>
#
# KPI tables:
#   kpi_revenue_by_customer_month  –  Monthly billing revenue per customer
#   kpi_order_to_cash              –  OTC cycle: order -> delivery -> billing
#   kpi_purchase_spend_by_vendor   –  PO spend per vendor per month
#   kpi_nota_fiscal_tax_summary    –  NF tax breakdown by type, company, month

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

SILVER_CAT = spark.conf.get("silver_catalog", "dev")


def _silver(t):
    return spark.read.table(f"{SILVER_CAT}.silver_sap.{t}")


# =============================================================================
# KPI 1 – Revenue by customer / month
# Grain: one row per (kunnr, billing_month, waerk)
# =============================================================================

@dlt.table(
    name="kpi_revenue_by_customer_month",
    comment=(
        "Monthly billing revenue aggregated by customer. "
        "Source: fact_billing joined with dim_customer. "
        "Excludes cancelled invoices (already filtered in fact_billing). "
        "Grain: kunnr x billing_month x waerk."
    ),
)
def kpi_revenue_by_customer_month():
    billing  = _silver("fact_billing")
    customer = _silver("dim_customer")
    return (
        billing
        .join(customer, billing["kunnr"] == customer["kunnr"], "left")
        .groupBy(
            billing["kunnr"],
            customer["name1"].alias("customer_name"),
            customer["state"],
            F.date_trunc("month", billing["billing_date"]).alias("billing_month"),
            billing["waerk"],
        )
        .agg(
            F.sum("item_net_value").cast(DecimalType(17, 2)).alias("total_net_revenue"),
            F.sum("item_tax").cast(DecimalType(17, 2)).alias("total_tax"),
            F.countDistinct("vbeln").alias("invoice_count"),
            F.countDistinct("matnr").alias("distinct_materials"),
        )
        .withColumn("_processed_time", F.current_timestamp())
    )


# =============================================================================
# KPI 2 – Order-to-Cash cycle time
# Grain: one row per sales order vbeln
# =============================================================================

@dlt.table(
    name="kpi_order_to_cash",
    comment=(
        "Order-to-Cash pipeline per sales order. "
        "Computes days from order_date to first goods issue and to first billing. "
        "NULL days_to_delivery = not yet delivered. "
        "NULL days_to_billing = not yet invoiced. "
        "Grain: one row per sales order (vbeln in VBAK)."
    ),
)
def kpi_order_to_cash():
    orders  = _silver("fact_sales_order")
    billing = _silver("fact_billing")
    deliv   = _silver("fact_delivery")

    first_bill = (
        billing
        .groupBy("source_order")
        .agg(F.min("billing_date").alias("first_billing_date"))
    )
    first_del = (
        deliv
        .groupBy("source_order")
        .agg(F.min("actual_goods_issue_date").alias("first_goods_issue_date"))
    )

    return (
        orders
        .groupBy("vbeln", "kunnr", "order_date", "order_type", "vkorg", "waerk")
        .agg(
            F.sum("item_net_value").cast(DecimalType(17, 2)).alias("order_net_value"),
            F.first("overall_status").alias("overall_status"),
        )
        .join(first_bill, F.col("vbeln") == first_bill["source_order"], "left")
        .join(first_del,  F.col("vbeln") == first_del["source_order"],  "left")
        .select(
            "vbeln", "kunnr", "order_date", "order_type", "vkorg", "waerk",
            "order_net_value", "overall_status",
            "first_goods_issue_date",
            "first_billing_date",
            F.datediff(
                F.col("first_goods_issue_date"), F.col("order_date")
            ).alias("days_to_delivery"),
            F.datediff(
                F.col("first_billing_date"), F.col("order_date")
            ).alias("days_to_billing"),
            F.current_timestamp().alias("_processed_time"),
        )
    )


# =============================================================================
# KPI 3 – Purchase spend by vendor / month
# Grain: one row per (lifnr, po_month, bukrs)
# =============================================================================

@dlt.table(
    name="kpi_purchase_spend_by_vendor",
    comment=(
        "Monthly purchase spend per vendor. "
        "amount_invoiced comes from EKBE vgabe=2 (Invoice Receipt) totals. "
        "fulfillment_rate = qty_received / qty_ordered (NULL if 0 ordered). "
        "Grain: lifnr x po_month x bukrs."
    ),
)
def kpi_purchase_spend_by_vendor():
    po     = _silver("fact_purchase_order")
    vendor = _silver("dim_vendor")
    return (
        po
        .join(vendor, po["lifnr"] == vendor["lifnr"], "left")
        .groupBy(
            po["lifnr"],
            vendor["name1"].alias("vendor_name"),
            vendor["country"],
            F.date_trunc("month", po["po_date"]).alias("po_month"),
            po["bukrs"],
        )
        .agg(
            F.sum("amount_invoiced").cast(DecimalType(17, 2)).alias("total_invoiced"),
            F.sum("qty_ordered").cast(DecimalType(17, 3)).alias("total_qty_ordered"),
            F.sum("qty_received").cast(DecimalType(17, 3)).alias("total_qty_received"),
            F.countDistinct("ebeln").alias("po_count"),
            F.countDistinct("matnr").alias("distinct_materials"),
        )
        .withColumn("_processed_time", F.current_timestamp())
    )


# =============================================================================
# KPI 4 – Nota Fiscal tax summary
# Grain: one row per (bukrs, tax_month, taxtyp)
# =============================================================================

@dlt.table(
    name="kpi_nota_fiscal_tax_summary",
    comment=(
        "Monthly NF tax obligations by company code and tax type. "
        "taxtyp values: IPI, ICMS, PIS, COFINS, CSLL, ISS. "
        "Filters direct=O (outbound NFs only – fiscal liability). "
        "Grain: bukrs x tax_month x taxtyp."
    ),
)
def kpi_nota_fiscal_tax_summary():
    nf = _silver("fact_nota_fiscal")
    return (
        nf
        .filter(F.col("direct") == F.lit("O"))
        .groupBy(
            "bukrs",
            F.date_trunc("month", F.col("docdat")).alias("tax_month"),
            "taxtyp",
        )
        .agg(
            F.sum("tax_value").cast(DecimalType(17, 2)).alias("total_tax_value"),
            F.avg("tax_rate").cast(DecimalType(7, 4)).alias("avg_tax_rate"),
            F.countDistinct("docnum").alias("nf_document_count"),
            F.sum("nf_value").cast(DecimalType(17, 2)).alias("total_nf_value"),
        )
        .withColumn("_processed_time", F.current_timestamp())
    )
