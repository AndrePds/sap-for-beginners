-- silver_pipeline.sql  –  SAP Medallion Silver Layer
--
-- L1  –  18 STREAMING tables: type-cast, filter and apply business column names.
--         Source:  STREAM(LIVE.bronze_<t>)  (same pipeline)
--         Filter:  mandt = '100'
--         Casts:   SAP DATS (YYYYMMDD STRING) -> DATE
--                  CURR / DEC                 -> DECIMAL(17,2)
--                  QUAN                       -> DECIMAL(17,3)
--
--         Column naming rules:
--           - SAP key/FK fields (vbeln, kunnr, matnr, ebeln, bukrs, etc.)
--             keep their SAP names to preserve join-ability.
--           - All descriptor/value columns use snake_case business names
--             from datasets/generator/sap-column-glossary.csv (business_name field).
--
-- L2  –  8 MATERIALIZED VIEWs: domain model joins across Silver L1.
--         Source:  ${catalog}.${silver_schema}.<table>  (same pipeline, batch read)


-- =============================================================================
-- SILVER L1  –  Typed, filtered and business-named streaming tables
-- =============================================================================

-- ----------------------------------------------------------------------------
-- Master Data
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_kna1 (
  CONSTRAINT valid_kunnr EXPECT (kunnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver KNA1: Customer master – typed, mandt-filtered, business column names.'
AS SELECT
  kunnr,
  name1         AS customer_name,
  mcod1,
  stcd1         AS cnpj_cpf,
  ktokd         AS account_group,
  ort01         AS city,
  regio         AS state,
  land1         AS country
FROM STREAM(LIVE.bronze_kna1)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_lfa1 (
  CONSTRAINT valid_lifnr EXPECT (lifnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver LFA1: Vendor master – typed, mandt-filtered, business column names.'
AS SELECT
  lifnr,
  name1         AS vendor_name,
  mcod1,
  stcd1         AS cnpj_cpf,
  ort01         AS city,
  regio         AS state,
  land1         AS country
FROM STREAM(LIVE.bronze_lfa1)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_mara (
  CONSTRAINT valid_matnr EXPECT (matnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver MARA: General material data – typed, mandt-filtered, business column names.'
AS SELECT
  matnr,
  matkl                        AS material_group,
  meins                        AS base_uom,
  gewei                        AS weight_unit,
  prdha                        AS product_hierarchy,
  CAST(ntgew AS DECIMAL(17,3)) AS net_weight,
  CAST(brgew AS DECIMAL(17,3)) AS gross_weight,
  bismt                        AS old_material_number
FROM STREAM(LIVE.bronze_mara)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_makt (
  CONSTRAINT valid_matnr EXPECT (matnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver MAKT: Material descriptions (PT language filter).'
AS SELECT
  matnr,
  maktx AS material_desc
FROM STREAM(LIVE.bronze_makt)
WHERE mandt = '100'
  AND spras = 'P';

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_pa0001 (
  CONSTRAINT valid_pernr EXPECT (pernr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver PA0001: HR org assignment – typed, mandt-filtered, business column names.'
AS SELECT
  pernr,
  ename                              AS employee_name,
  TO_DATE(begda, 'yyyyMMdd')         AS start_date,
  TO_DATE(endda, 'yyyyMMdd')         AS end_date,
  plans                              AS position,
  orgeh                              AS org_unit
FROM STREAM(LIVE.bronze_pa0001)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- SD – Sales Order
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_vbak (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver VBAK: Sales order header – typed, mandt-filtered, business column names.'
AS SELECT
  vbeln,
  kunnr,
  TO_DATE(audat, 'yyyyMMdd')   AS order_date,
  TO_DATE(erdat, 'yyyyMMdd')   AS created_date,
  TO_DATE(vdatu, 'yyyyMMdd')   AS requested_delivery_date,
  auart                        AS order_type,
  vkorg                        AS sales_org,
  vtweg                        AS distribution_channel,
  spart                        AS division,
  vbtyp                        AS doc_category,
  knumv,
  CAST(netwr AS DECIMAL(17,2)) AS net_value,
  waerk                        AS currency,
  augru                        AS order_reason,
  faksk                        AS billing_block,
  lifsk                        AS delivery_block,
  gbstk                        AS overall_status,
  vkgrp                        AS sales_group,
  vkbur                        AS sales_office,
  vsbed                        AS shipping_conditions,
  vgbel                        AS ref_doc_number
FROM STREAM(LIVE.bronze_vbak)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_vbap (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_posnr EXPECT (posnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver VBAP: Sales order items – typed, mandt-filtered, business column names.'
AS SELECT
  vbeln,
  posnr,
  matnr,
  werks,
  lgort                         AS storage_location,
  meins                         AS base_uom,
  vrkme                         AS sales_unit,
  CAST(netwr   AS DECIMAL(17,2)) AS net_value,
  waerk                         AS currency,
  CAST(kwmeng  AS DECIMAL(17,3)) AS order_qty,
  CAST(kbmeng  AS DECIMAL(17,3)) AS cumulative_ordered_qty,
  charg                         AS batch_number,
  route,
  matkl                         AS material_group,
  TO_DATE(abdat, 'yyyyMMdd')    AS requested_date,
  fksta                         AS billing_status,
  lfsta                         AS delivery_status,
  fkrel
FROM STREAM(LIVE.bronze_vbap)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- SD – Billing
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_vbrk (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver VBRK: Billing document header – typed, mandt-filtered. cancelled_flag=TRUE excludes from fact_billing.'
AS SELECT
  vbeln,
  TO_DATE(fkdat, 'yyyyMMdd')    AS billing_date,
  TO_DATE(erdat, 'yyyyMMdd')    AS created_date,
  fkart                         AS billing_type,
  vbtyp                         AS doc_category,
  kunrg                         AS payer_customer,
  knumv,
  CAST(netwr  AS DECIMAL(17,2)) AS net_value,
  waerk                         AS currency,
  CAST(mwsbk  AS DECIMAL(17,2)) AS tax_amount,
  CASE WHEN fksto = 'X' THEN TRUE ELSE FALSE END AS cancelled_flag,
  sfakn                         AS cancelled_invoice_no,
  zlsch
FROM STREAM(LIVE.bronze_vbrk)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_vbrp (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver VBRP: Billing document items – typed, mandt-filtered, business column names.'
AS SELECT
  vbeln,
  posnr,
  matnr,
  meins                         AS base_uom,
  CAST(netwr  AS DECIMAL(17,2)) AS net_value,
  waerk                         AS currency,
  CAST(fklmg  AS DECIMAL(17,3)) AS billed_qty,
  CAST(mwsbp  AS DECIMAL(17,2)) AS item_tax,
  vgbel                         AS ref_doc_number,
  vgpos                         AS ref_item_number,
  aubel                         AS source_order_no,
  aupos                         AS source_order_item_no
FROM STREAM(LIVE.bronze_vbrp)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- SD – Delivery
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_likp (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver LIKP: Delivery header – typed, mandt-filtered, business column names.'
AS SELECT
  vbeln,
  kunnr,
  vkorg                              AS sales_org,
  tdlnr,
  TO_DATE(lfdat,     'yyyyMMdd')     AS planned_delivery_date,
  TO_DATE(wadat_ist, 'yyyyMMdd')     AS actual_goods_issue_date,
  TO_DATE(erdat,     'yyyyMMdd')     AS created_date,
  lfart                              AS delivery_type,
  trspg
FROM STREAM(LIVE.bronze_likp)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_lips (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver LIPS: Delivery items – typed, mandt-filtered, business column names.'
AS SELECT
  vbeln,
  posnr,
  matnr,
  meins                         AS base_uom,
  vrkme                         AS sales_unit,
  lgort                         AS storage_location,
  werks,
  CAST(lfimg AS DECIMAL(17,3))  AS delivered_qty,
  vgbel                         AS ref_doc_number,
  vgpos                         AS ref_item_number,
  dfexp                         AS export_date
FROM STREAM(LIVE.bronze_lips)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- MM – Purchasing
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_ekko (
  CONSTRAINT valid_ebeln EXPECT (ebeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver EKKO: Purchase order header – typed, mandt-filtered, business column names.'
AS SELECT
  ebeln,
  bukrs                        AS company_code,
  lifnr,
  ekgrp                        AS purchasing_group,
  bsart                        AS doc_type,
  TO_DATE(bedat, 'yyyyMMdd')   AS po_date,
  TO_DATE(aedat, 'yyyyMMdd')   AS last_change_date,
  waers                        AS currency
FROM STREAM(LIVE.bronze_ekko)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_ekpo (
  CONSTRAINT valid_ebeln EXPECT (ebeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver EKPO: Purchase order items – typed, mandt-filtered, business column names.'
AS SELECT
  ebeln,
  ebelp,
  matnr,
  werks,
  meins                        AS base_uom,
  CAST(menge AS DECIMAL(17,3)) AS qty_ordered,
  CAST(netpr AS DECIMAL(17,2)) AS net_price,
  peinh                        AS price_unit,
  waers                        AS currency
FROM STREAM(LIVE.bronze_ekpo)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_ekbe (
  CONSTRAINT valid_ebeln EXPECT (ebeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver EKBE: PO history (GR/IR) – typed, mandt-filtered. event_type: 1=GR, 2=IR.'
AS SELECT
  ebeln,
  ebelp,
  zekkn                         AS account_assignment_no,
  belnr                         AS accounting_doc_no,
  gjahr                         AS fiscal_year,
  vgabe                         AS event_type,
  CAST(menge  AS DECIMAL(17,3)) AS qty,
  CAST(wrbtr  AS DECIMAL(17,2)) AS amount,
  waers                         AS currency
FROM STREAM(LIVE.bronze_ekbe)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- FI-NF – Nota Fiscal (Brazil)
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_j_1bnfdoc (
  CONSTRAINT valid_docnum EXPECT (docnum IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver J_1BNFDOC: Nota Fiscal header – typed, mandt-filtered. direct=O outbound, direct=I inbound.'
AS SELECT
  docnum,
  bukrs                              AS company_code,
  nfnum                              AS nf_number,
  bupla,
  serie,
  refkey,
  stcd1                              AS cnpj_cpf,
  direct,
  TO_DATE(docdat, 'yyyyMMdd')        AS doc_date
FROM STREAM(LIVE.bronze_j_1bnfdoc)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_j_1bnflin (
  CONSTRAINT valid_docnum EXPECT (docnum IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver J_1BNFLIN: Nota Fiscal line items – typed, mandt-filtered, business column names.'
AS SELECT
  docnum,
  itmnum                        AS nf_item_no,
  matnr,
  cfop,
  CAST(nfqtd  AS DECIMAL(17,3)) AS nf_qty,
  CAST(nfpric AS DECIMAL(17,2)) AS nf_unit_price,
  CAST(nfval  AS DECIMAL(17,2)) AS nf_value
FROM STREAM(LIVE.bronze_j_1bnflin)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_j_1bnfstx (
  CONSTRAINT valid_docnum EXPECT (docnum IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver J_1BNFSTX: Nota Fiscal taxes – typed, mandt-filtered. tax_type: IPI, ICMS, PIS, COFINS, CSLL, ISS.'
AS SELECT
  docnum,
  itmnum                        AS nf_item_no,
  taxtyp                        AS tax_type,
  CAST(taxval AS DECIMAL(17,2)) AS tax_value,
  CAST(taxrat AS DECIMAL(7,4))  AS tax_rate
FROM STREAM(LIVE.bronze_j_1bnfstx)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- FI – Financial Accounting
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_bkpf (
  CONSTRAINT valid_belnr EXPECT (belnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver BKPF: FI document header – typed, mandt-filtered, business column names.'
AS SELECT
  bukrs                        AS company_code,
  belnr,
  gjahr                        AS fiscal_year,
  TO_DATE(budat, 'yyyyMMdd')   AS posting_date,
  TO_DATE(bldat, 'yyyyMMdd')   AS document_date,
  waers                        AS currency,
  awkey                        AS ref_key,
  awtyp                        AS ref_transaction
FROM STREAM(LIVE.bronze_bkpf)
WHERE mandt = '100';


-- =============================================================================
-- SILVER L2  –  Domain models (batch joins across Silver L1)
-- =============================================================================

-- ----------------------------------------------------------------------------
-- Dimensions
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${silver_schema}.dim_customer
  COMMENT 'Customer dimension: deduped from silver_kna1. Grain: one row per kunnr.'
AS SELECT DISTINCT
  kunnr, customer_name, mcod1, cnpj_cpf, account_group, city, state, country
FROM ${catalog}.${silver_schema}.silver_kna1;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${silver_schema}.dim_vendor
  COMMENT 'Vendor dimension: deduped from silver_lfa1. Grain: one row per lifnr.'
AS SELECT DISTINCT
  lifnr, vendor_name, mcod1, cnpj_cpf, city, state, country
FROM ${catalog}.${silver_schema}.silver_lfa1;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${silver_schema}.dim_material
  COMMENT 'Material dimension: MARA joined with MAKT (PT description). Grain: one row per matnr.'
AS SELECT
  m.matnr,
  m.material_group,
  m.base_uom,
  m.product_hierarchy,
  m.net_weight,
  m.gross_weight,
  t.material_desc
FROM ${catalog}.${silver_schema}.silver_mara m
LEFT JOIN ${catalog}.${silver_schema}.silver_makt t ON m.matnr = t.matnr;


-- ----------------------------------------------------------------------------
-- Facts
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${silver_schema}.fact_sales_order
  COMMENT 'Sales order fact at item grain (VBAK x VBAP). Grain: vbeln + posnr.'
AS SELECT
  p.vbeln,
  p.posnr,
  h.kunnr,
  h.order_date,
  h.order_type,
  h.sales_org,
  h.distribution_channel,
  h.division,
  h.overall_status,
  p.matnr,
  p.werks,
  p.base_uom,
  p.order_qty,
  p.net_value                    AS item_net_value,
  p.currency,
  p.billing_status,
  p.delivery_status
FROM ${catalog}.${silver_schema}.silver_vbap  p
JOIN ${catalog}.${silver_schema}.silver_vbak  h ON p.vbeln = h.vbeln;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${silver_schema}.fact_billing
  COMMENT 'Billing fact at item grain (VBRK x VBRP). Excludes cancelled invoices. Grain: vbeln + posnr.'
AS SELECT
  p.vbeln,
  p.posnr,
  h.payer_customer               AS kunnr,
  h.billing_date,
  h.billing_type,
  p.currency,
  p.matnr,
  p.base_uom,
  p.billed_qty,
  p.net_value                    AS item_net_value,
  p.item_tax,
  h.tax_amount                   AS doc_tax,
  p.ref_doc_number               AS source_delivery,
  p.source_order_no              AS source_order
FROM ${catalog}.${silver_schema}.silver_vbrp p
JOIN ${catalog}.${silver_schema}.silver_vbrk h ON p.vbeln = h.vbeln
WHERE h.cancelled_flag = FALSE;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${silver_schema}.fact_delivery
  COMMENT 'Delivery fact at item grain (LIKP x LIPS). Grain: vbeln + posnr.'
AS SELECT
  i.vbeln,
  i.posnr,
  h.kunnr,
  h.planned_delivery_date,
  h.actual_goods_issue_date,
  i.matnr,
  i.base_uom,
  i.delivered_qty,
  i.ref_doc_number               AS source_order,
  i.ref_item_number              AS source_order_item
FROM ${catalog}.${silver_schema}.silver_lips  i
JOIN ${catalog}.${silver_schema}.silver_likp  h ON i.vbeln = h.vbeln;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${silver_schema}.fact_purchase_order
  COMMENT 'PO fact at item grain with GR (event_type=1) and IR (event_type=2) totals from EKBE. Grain: ebeln + ebelp.'
AS SELECT
  p.ebeln,
  p.ebelp,
  h.lifnr,
  h.company_code,
  h.po_date,
  h.doc_type                        AS po_type,
  p.matnr,
  p.base_uom,
  p.qty_ordered,
  p.net_price,
  CAST(COALESCE(gr.qty_received,    0) AS DECIMAL(17,3)) AS qty_received,
  CAST(COALESCE(ir.amount_invoiced, 0) AS DECIMAL(17,2)) AS amount_invoiced
FROM ${catalog}.${silver_schema}.silver_ekpo p
JOIN ${catalog}.${silver_schema}.silver_ekko h ON p.ebeln = h.ebeln
LEFT JOIN (
  SELECT ebeln, ebelp, SUM(qty) AS qty_received
  FROM ${catalog}.${silver_schema}.silver_ekbe
  WHERE event_type = '1'
  GROUP BY ebeln, ebelp
) gr ON p.ebeln = gr.ebeln AND p.ebelp = gr.ebelp
LEFT JOIN (
  SELECT ebeln, ebelp, SUM(amount) AS amount_invoiced
  FROM ${catalog}.${silver_schema}.silver_ekbe
  WHERE event_type = '2'
  GROUP BY ebeln, ebelp
) ir ON p.ebeln = ir.ebeln AND p.ebelp = ir.ebelp;

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${silver_schema}.fact_nota_fiscal
  COMMENT 'Nota Fiscal fact: header + line + taxes. Grain: docnum + nf_item_no + tax_type.'
AS SELECT
  d.docnum,
  l.nf_item_no,
  d.company_code,
  d.nf_number,
  d.bupla,
  d.serie,
  d.direct,
  d.doc_date,
  l.matnr,
  l.cfop,
  l.nf_qty,
  l.nf_value,
  t.tax_type,
  t.tax_value,
  t.tax_rate
FROM ${catalog}.${silver_schema}.silver_j_1bnfdoc  d
JOIN ${catalog}.${silver_schema}.silver_j_1bnflin  l ON d.docnum = l.docnum
LEFT JOIN ${catalog}.${silver_schema}.silver_j_1bnfstx t
  ON l.docnum = t.docnum AND l.nf_item_no = t.nf_item_no;
