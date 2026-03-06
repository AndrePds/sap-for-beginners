# gold_schemas.py  –  Reference column list for Gold KPI tables.

GOLD_COLUMNS = {
    "kpi_revenue_by_customer_month": [
        "kunnr", "customer_name", "state", "billing_month", "waerk",
        "total_net_revenue", "total_tax",
        "invoice_count", "distinct_materials",
    ],
    "kpi_order_to_cash": [
        "vbeln", "kunnr", "order_date", "order_type", "vkorg", "waerk",
        "order_net_value", "overall_status",
        "first_goods_issue_date", "first_billing_date",
        "days_to_delivery", "days_to_billing",
    ],
    "kpi_purchase_spend_by_vendor": [
        "lifnr", "vendor_name", "country", "po_month", "bukrs",
        "total_invoiced", "total_qty_ordered", "total_qty_received",
        "po_count", "distinct_materials",
    ],
    "kpi_nota_fiscal_tax_summary": [
        "bukrs", "tax_month", "taxtyp",
        "total_tax_value", "avg_tax_rate",
        "nf_document_count", "total_nf_value",
    ],
}
