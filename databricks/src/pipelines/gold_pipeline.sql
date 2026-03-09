-- gold_pipeline.sql  –  SAP Medallion Gold Layer
--
-- Business-ready KPI tables aggregated from Silver L2 domain models.
-- All tables are  CREATE OR REFRESH MATERIALIZED VIEW  (batch – not streaming).
-- Sources are LIVE.fact_* and LIVE.dim_* from the same pipeline.
--
-- KPI tables:
--   kpi_revenue_by_customer_month  –  Monthly billing revenue per customer
--   kpi_order_to_cash              –  OTC cycle: order -> delivery -> billing
--   kpi_purchase_spend_by_vendor   –  PO spend per vendor per month
--   kpi_nota_fiscal_tax_summary    –  NF tax breakdown by type, company, month


-- =============================================================================
-- KPI 1 – Revenue by customer / month
-- Grain: one row per (kunnr, billing_month, waerk)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW kpi_revenue_by_customer_month
  COMMENT 'Monthly billing revenue per customer. Source: fact_billing x dim_customer. Excludes cancelled invoices (already filtered in fact_billing). Grain: kunnr x billing_month x waerk.'
AS SELECT
  b.kunnr,
  c.name1                                      AS customer_name,
  c.state,
  DATE_TRUNC('month', b.billing_date)          AS billing_month,
  b.waerk,
  CAST(SUM(b.item_net_value) AS DECIMAL(17,2)) AS total_net_revenue,
  CAST(SUM(b.item_tax)       AS DECIMAL(17,2)) AS total_tax,
  COUNT(DISTINCT b.vbeln)                      AS invoice_count,
  COUNT(DISTINCT b.matnr)                      AS distinct_materials,
  current_timestamp()                          AS _processed_time
FROM LIVE.fact_billing b
LEFT JOIN LIVE.dim_customer c ON b.kunnr = c.kunnr
GROUP BY
  b.kunnr,
  c.name1,
  c.state,
  DATE_TRUNC('month', b.billing_date),
  b.waerk;


-- =============================================================================
-- KPI 2 – Order-to-Cash cycle time
-- Grain: one row per sales order (vbeln)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW kpi_order_to_cash
  COMMENT 'Order-to-Cash pipeline per sales order. days_to_delivery = order_date -> first goods issue. days_to_billing = order_date -> first billing. NULL = not yet reached that milestone. Grain: one row per vbeln.'
AS WITH order_summary AS (
  SELECT
    vbeln,
    kunnr,
    order_date,
    order_type,
    vkorg,
    waerk,
    CAST(SUM(item_net_value) AS DECIMAL(17,2)) AS order_net_value,
    FIRST(overall_status)                       AS overall_status
  FROM LIVE.fact_sales_order
  GROUP BY vbeln, kunnr, order_date, order_type, vkorg, waerk
),
first_delivery AS (
  SELECT source_order, MIN(actual_goods_issue_date) AS first_goods_issue_date
  FROM LIVE.fact_delivery
  GROUP BY source_order
),
first_billing AS (
  SELECT source_order, MIN(billing_date) AS first_billing_date
  FROM LIVE.fact_billing
  GROUP BY source_order
)
SELECT
  o.vbeln,
  o.kunnr,
  o.order_date,
  o.order_type,
  o.vkorg,
  o.waerk,
  o.order_net_value,
  o.overall_status,
  d.first_goods_issue_date,
  b.first_billing_date,
  DATEDIFF(d.first_goods_issue_date, o.order_date) AS days_to_delivery,
  DATEDIFF(b.first_billing_date,     o.order_date) AS days_to_billing,
  current_timestamp()                               AS _processed_time
FROM order_summary         o
LEFT JOIN first_delivery   d ON o.vbeln = d.source_order
LEFT JOIN first_billing    b ON o.vbeln = b.source_order;


-- =============================================================================
-- KPI 3 – Purchase spend by vendor / month
-- Grain: one row per (lifnr, po_month, bukrs)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW kpi_purchase_spend_by_vendor
  COMMENT 'Monthly purchase spend per vendor. amount_invoiced comes from EKBE vgabe=2 (Invoice Receipt) totals. Grain: lifnr x po_month x bukrs.'
AS SELECT
  po.lifnr,
  v.name1                                          AS vendor_name,
  v.country,
  DATE_TRUNC('month', po.po_date)                  AS po_month,
  po.bukrs,
  CAST(SUM(po.amount_invoiced) AS DECIMAL(17,2))   AS total_invoiced,
  CAST(SUM(po.qty_ordered)     AS DECIMAL(17,3))   AS total_qty_ordered,
  CAST(SUM(po.qty_received)    AS DECIMAL(17,3))   AS total_qty_received,
  COUNT(DISTINCT po.ebeln)                         AS po_count,
  COUNT(DISTINCT po.matnr)                         AS distinct_materials,
  current_timestamp()                              AS _processed_time
FROM LIVE.fact_purchase_order po
LEFT JOIN LIVE.dim_vendor v ON po.lifnr = v.lifnr
GROUP BY
  po.lifnr,
  v.name1,
  v.country,
  DATE_TRUNC('month', po.po_date),
  po.bukrs;


-- =============================================================================
-- KPI 4 – Nota Fiscal tax summary
-- Grain: one row per (bukrs, tax_month, taxtyp)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW kpi_nota_fiscal_tax_summary
  COMMENT 'Monthly NF tax obligations by company code and tax type. Filters direct=O (outbound NFs – fiscal liability). taxtyp values: IPI, ICMS, PIS, COFINS, CSLL, ISS. Grain: bukrs x tax_month x taxtyp.'
AS SELECT
  bukrs,
  DATE_TRUNC('month', docdat)                    AS tax_month,
  taxtyp,
  CAST(SUM(tax_value)  AS DECIMAL(17,2))         AS total_tax_value,
  CAST(AVG(tax_rate)   AS DECIMAL(7,4))          AS avg_tax_rate,
  COUNT(DISTINCT docnum)                         AS nf_document_count,
  CAST(SUM(nf_value)   AS DECIMAL(17,2))         AS total_nf_value,
  current_timestamp()                            AS _processed_time
FROM LIVE.fact_nota_fiscal
WHERE direct = 'O'
GROUP BY
  bukrs,
  DATE_TRUNC('month', docdat),
  taxtyp;
