-- silver_pipeline.sql  –  SAP Medallion Silver Layer
--
-- L1  –  18 STREAMING tables: type-cast and filter raw Bronze records.
--         Source:  STREAM(LIVE.bronze_<t>)  (same pipeline)
--         Filter:  mandt = '100'
--         Casts:   SAP DATS (YYYYMMDD STRING) -> DATE
--                  CURR / DEC                 -> DECIMAL(17,2)
--                  QUAN                       -> DECIMAL(17,3)
--
-- L2  –  8 LIVE (batch) tables: domain model joins across Silver L1.
--         Source:  LIVE.<silver_l1_table>  (same pipeline, batch read)
--
-- SAP column rename conventions applied here:
--   ort01  -> city  |  regio -> state  |  land1 -> country
--   stcd1  -> cnpj_cpf  (Brazilian tax ID on customer/vendor)
--   fkdat  -> billing_date  |  budat -> posting_date  |  bldat -> document_date
--   audat  -> order_date    |  erdat -> created_date
--   lfdat  -> planned_delivery_date  |  wadat_ist -> actual_goods_issue_date
--   bedat  -> po_date  |  aedat -> last_change_date
--   maktx  -> material_desc  |  gbstk -> overall_status


-- =============================================================================
-- SILVER L1  –  Typed and filtered streaming tables
-- =============================================================================

-- ----------------------------------------------------------------------------
-- Master Data
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver_kna1 (
  CONSTRAINT valid_kunnr EXPECT (kunnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver KNA1: Customer master – typed and mandt-filtered.'
AS SELECT
  kunnr,
  name1,
  mcod1,
  stcd1   AS cnpj_cpf,
  ktokd,
  ort01   AS city,
  regio   AS state,
  land1   AS country
FROM STREAM(LIVE.bronze_kna1)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE silver_lfa1 (
  CONSTRAINT valid_lifnr EXPECT (lifnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver LFA1: Vendor master – typed and mandt-filtered.'
AS SELECT
  lifnr,
  name1,
  mcod1,
  stcd1   AS cnpj_cpf,
  ort01   AS city,
  regio   AS state,
  land1   AS country
FROM STREAM(LIVE.bronze_lfa1)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE silver_mara (
  CONSTRAINT valid_matnr EXPECT (matnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver MARA: General material data – typed and mandt-filtered.'
AS SELECT
  matnr,
  matkl,
  meins,
  gewei,
  prdha,
  CAST(ntgew AS DECIMAL(17,3)) AS ntgew,
  CAST(brgew AS DECIMAL(17,3)) AS brgew,
  bismt
FROM STREAM(LIVE.bronze_mara)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE silver_makt (
  CONSTRAINT valid_matnr EXPECT (matnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver MAKT: Material descriptions (PT language filter).'
AS SELECT
  matnr,
  maktx AS material_desc
FROM STREAM(LIVE.bronze_makt)
WHERE mandt = '100'
  AND spras = 'P';

CREATE OR REFRESH STREAMING TABLE silver_pa0001 (
  CONSTRAINT valid_pernr EXPECT (pernr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver PA0001: HR org assignment – typed and mandt-filtered.'
AS SELECT
  pernr,
  ename,
  TO_DATE(begda, 'yyyyMMdd') AS start_date,
  TO_DATE(endda, 'yyyyMMdd') AS end_date,
  plans,
  orgeh
FROM STREAM(LIVE.bronze_pa0001)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- SD – Sales Order
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver_vbak (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver VBAK: Sales order header – typed and mandt-filtered.'
AS SELECT
  vbeln,
  kunnr,
  TO_DATE(audat, 'yyyyMMdd')  AS order_date,
  TO_DATE(erdat, 'yyyyMMdd')  AS created_date,
  TO_DATE(vdatu, 'yyyyMMdd')  AS requested_delivery_date,
  auart,
  vkorg,
  vtweg,
  spart,
  vbtyp,
  knumv,
  CAST(netwr AS DECIMAL(17,2)) AS netwr,
  waerk,
  augru,
  faksk,
  lifsk,
  gbstk,
  vkgrp,
  vkbur,
  vsbed,
  vgbel
FROM STREAM(LIVE.bronze_vbak)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE silver_vbap (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_posnr EXPECT (posnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver VBAP: Sales order items – typed and mandt-filtered.'
AS SELECT
  vbeln,
  posnr,
  matnr,
  werks,
  lgort,
  meins,
  vrkme,
  CAST(netwr   AS DECIMAL(17,2)) AS netwr,
  waerk,
  CAST(kwmeng  AS DECIMAL(17,3)) AS kwmeng,
  CAST(kbmeng  AS DECIMAL(17,3)) AS kbmeng,
  charg,
  route,
  matkl,
  TO_DATE(abdat, 'yyyyMMdd') AS requested_date,
  fksta,
  lfsta,
  fkrel
FROM STREAM(LIVE.bronze_vbap)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- SD – Billing
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver_vbrk (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver VBRK: Billing document header – typed and mandt-filtered. cancelled_flag=TRUE excludes from fact_billing.'
AS SELECT
  vbeln,
  TO_DATE(fkdat, 'yyyyMMdd')   AS billing_date,
  TO_DATE(erdat, 'yyyyMMdd')   AS created_date,
  fkart,
  vbtyp,
  kunrg,
  knumv,
  CAST(netwr  AS DECIMAL(17,2)) AS netwr,
  waerk,
  CAST(mwsbk  AS DECIMAL(17,2)) AS tax_amount,
  CASE WHEN fksto = 'X' THEN TRUE ELSE FALSE END AS cancelled_flag,
  sfakn                         AS cancel_ref_doc,
  zlsch
FROM STREAM(LIVE.bronze_vbrk)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE silver_vbrp (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver VBRP: Billing document items – typed and mandt-filtered.'
AS SELECT
  vbeln,
  posnr,
  matnr,
  meins,
  CAST(netwr  AS DECIMAL(17,2)) AS netwr,
  waerk,
  CAST(fklmg  AS DECIMAL(17,3)) AS billed_qty,
  CAST(mwsbp  AS DECIMAL(17,2)) AS item_tax,
  vgbel,
  vgpos,
  aubel,
  aupos
FROM STREAM(LIVE.bronze_vbrp)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- SD – Delivery
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver_likp (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver LIKP: Delivery header – typed and mandt-filtered.'
AS SELECT
  vbeln,
  kunnr,
  vkorg,
  tdlnr,
  TO_DATE(lfdat,     'yyyyMMdd') AS planned_delivery_date,
  TO_DATE(wadat_ist, 'yyyyMMdd') AS actual_goods_issue_date,
  TO_DATE(erdat,     'yyyyMMdd') AS created_date,
  lfart,
  trspg
FROM STREAM(LIVE.bronze_likp)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE silver_lips (
  CONSTRAINT valid_vbeln EXPECT (vbeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver LIPS: Delivery items – typed and mandt-filtered.'
AS SELECT
  vbeln,
  posnr,
  matnr,
  meins,
  vrkme,
  lgort,
  werks,
  CAST(lfimg AS DECIMAL(17,3)) AS delivered_qty,
  vgbel,
  vgpos,
  dfexp AS export_date
FROM STREAM(LIVE.bronze_lips)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- MM – Purchasing
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver_ekko (
  CONSTRAINT valid_ebeln EXPECT (ebeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver EKKO: Purchase order header – typed and mandt-filtered.'
AS SELECT
  ebeln,
  bukrs,
  lifnr,
  ekgrp,
  bsart,
  TO_DATE(bedat, 'yyyyMMdd') AS po_date,
  TO_DATE(aedat, 'yyyyMMdd') AS last_change_date,
  waers
FROM STREAM(LIVE.bronze_ekko)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE silver_ekpo (
  CONSTRAINT valid_ebeln EXPECT (ebeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver EKPO: Purchase order items – typed and mandt-filtered.'
AS SELECT
  ebeln,
  ebelp,
  matnr,
  werks,
  meins,
  CAST(menge AS DECIMAL(17,3)) AS qty_ordered,
  CAST(netpr AS DECIMAL(17,2)) AS net_price,
  peinh                        AS price_unit,
  waers
FROM STREAM(LIVE.bronze_ekpo)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE silver_ekbe (
  CONSTRAINT valid_ebeln EXPECT (ebeln IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver EKBE: PO history (GR/IR movements) – typed and mandt-filtered. vgabe=1 GR, vgabe=2 IR.'
AS SELECT
  ebeln,
  ebelp,
  zekkn,
  belnr,
  gjahr,
  vgabe,
  CAST(menge  AS DECIMAL(17,3)) AS qty,
  CAST(wrbtr  AS DECIMAL(17,2)) AS amount,
  waers
FROM STREAM(LIVE.bronze_ekbe)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- FI-NF – Nota Fiscal (Brazil)
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver_j_1bnfdoc (
  CONSTRAINT valid_docnum EXPECT (docnum IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver J_1BNFDOC: Nota Fiscal header – typed and mandt-filtered. direct=O outbound, direct=I inbound.'
AS SELECT
  docnum,
  bukrs,
  nfnum,
  bupla,
  serie,
  refkey,
  stcd1,
  direct,
  TO_DATE(docdat, 'yyyyMMdd') AS docdat
FROM STREAM(LIVE.bronze_j_1bnfdoc)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE silver_j_1bnflin (
  CONSTRAINT valid_docnum EXPECT (docnum IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver J_1BNFLIN: Nota Fiscal line items – typed and mandt-filtered.'
AS SELECT
  docnum,
  itmnum,
  matnr,
  cfop,
  CAST(nfqtd  AS DECIMAL(17,3)) AS nf_qty,
  CAST(nfpric AS DECIMAL(17,2)) AS nf_unit_price,
  CAST(nfval  AS DECIMAL(17,2)) AS nf_value
FROM STREAM(LIVE.bronze_j_1bnflin)
WHERE mandt = '100';

CREATE OR REFRESH STREAMING TABLE silver_j_1bnfstx (
  CONSTRAINT valid_docnum EXPECT (docnum IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver J_1BNFSTX: Nota Fiscal taxes – typed and mandt-filtered. taxtyp: IPI, ICMS, PIS, COFINS, CSLL, ISS.'
AS SELECT
  docnum,
  itmnum,
  taxtyp,
  CAST(taxval AS DECIMAL(17,2)) AS tax_value,
  CAST(taxrat AS DECIMAL(7,4))  AS tax_rate
FROM STREAM(LIVE.bronze_j_1bnfstx)
WHERE mandt = '100';


-- ----------------------------------------------------------------------------
-- FI – Financial Accounting
-- ----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver_bkpf (
  CONSTRAINT valid_belnr EXPECT (belnr IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Silver BKPF: FI document header – typed and mandt-filtered.'
AS SELECT
  bukrs,
  belnr,
  gjahr,
  TO_DATE(budat, 'yyyyMMdd') AS posting_date,
  TO_DATE(bldat, 'yyyyMMdd') AS document_date,
  waers,
  awkey,
  awtyp
FROM STREAM(LIVE.bronze_bkpf)
WHERE mandt = '100';


-- =============================================================================
-- SILVER L2  –  Domain models (batch joins across Silver L1)
-- =============================================================================

-- ----------------------------------------------------------------------------
-- Dimensions
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW dim_customer
  COMMENT 'Customer dimension: deduped from silver_kna1. Grain: one row per kunnr.'
AS SELECT DISTINCT
  kunnr, name1, mcod1, cnpj_cpf, ktokd, city, state, country
FROM LIVE.silver_kna1;

CREATE OR REFRESH MATERIALIZED VIEW dim_vendor
  COMMENT 'Vendor dimension: deduped from silver_lfa1. Grain: one row per lifnr.'
AS SELECT DISTINCT
  lifnr, name1, mcod1, cnpj_cpf, city, state, country
FROM LIVE.silver_lfa1;

CREATE OR REFRESH MATERIALIZED VIEW dim_material
  COMMENT 'Material dimension: MARA joined with MAKT (PT description). Grain: one row per matnr.'
AS SELECT
  m.matnr,
  m.matkl,
  m.meins,
  m.prdha,
  m.ntgew,
  m.brgew,
  t.material_desc
FROM LIVE.silver_mara m
LEFT JOIN LIVE.silver_makt t ON m.matnr = t.matnr;


-- ----------------------------------------------------------------------------
-- Facts
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW fact_sales_order
  COMMENT 'Sales order fact at item grain (VBAK x VBAP). Grain: vbeln + posnr.'
AS SELECT
  p.vbeln,
  p.posnr,
  h.kunnr,
  h.order_date,
  h.auart                        AS order_type,
  h.vkorg,
  h.vtweg,
  h.spart,
  h.gbstk                        AS overall_status,
  p.matnr,
  p.werks,
  p.meins,
  CAST(p.kwmeng AS DECIMAL(17,3)) AS qty_ordered,
  CAST(p.netwr  AS DECIMAL(17,2)) AS item_net_value,
  p.waerk,
  p.fksta                        AS billing_status,
  p.lfsta                        AS delivery_status
FROM LIVE.silver_vbap  p
JOIN LIVE.silver_vbak  h ON p.vbeln = h.vbeln;

CREATE OR REFRESH MATERIALIZED VIEW fact_billing
  COMMENT 'Billing fact at item grain (VBRK x VBRP). Excludes cancelled invoices (cancelled_flag = TRUE). Grain: vbeln + posnr.'
AS SELECT
  p.vbeln,
  p.posnr,
  h.kunrg                        AS kunnr,
  h.billing_date,
  h.fkart                        AS billing_type,
  p.waerk,
  p.matnr,
  p.meins,
  p.billed_qty,
  CAST(p.netwr     AS DECIMAL(17,2)) AS item_net_value,
  p.item_tax,
  CAST(h.tax_amount AS DECIMAL(17,2)) AS doc_tax,
  p.vgbel                        AS source_delivery,
  p.aubel                        AS source_order
FROM LIVE.silver_vbrp p
JOIN LIVE.silver_vbrk h ON p.vbeln = h.vbeln
WHERE h.cancelled_flag = FALSE;

CREATE OR REFRESH MATERIALIZED VIEW fact_delivery
  COMMENT 'Delivery fact at item grain (LIKP x LIPS). Grain: vbeln + posnr.'
AS SELECT
  i.vbeln,
  i.posnr,
  h.kunnr,
  h.planned_delivery_date,
  h.actual_goods_issue_date,
  i.matnr,
  i.meins,
  i.delivered_qty,
  i.vgbel AS source_order,
  i.vgpos AS source_order_item
FROM LIVE.silver_lips  i
JOIN LIVE.silver_likp  h ON i.vbeln = h.vbeln;

CREATE OR REFRESH MATERIALIZED VIEW fact_purchase_order
  COMMENT 'Purchase order fact at item grain with GR (vgabe=1) and IR (vgabe=2) totals from EKBE. Grain: ebeln + ebelp.'
AS SELECT
  p.ebeln,
  p.ebelp,
  h.lifnr,
  h.bukrs,
  h.po_date,
  h.bsart                           AS po_type,
  p.matnr,
  p.meins,
  p.qty_ordered,
  p.net_price,
  CAST(COALESCE(gr.qty_received,    0) AS DECIMAL(17,3)) AS qty_received,
  CAST(COALESCE(ir.amount_invoiced, 0) AS DECIMAL(17,2)) AS amount_invoiced
FROM LIVE.silver_ekpo p
JOIN LIVE.silver_ekko h ON p.ebeln = h.ebeln
LEFT JOIN (
  SELECT ebeln, ebelp, SUM(qty) AS qty_received
  FROM LIVE.silver_ekbe
  WHERE vgabe = '1'
  GROUP BY ebeln, ebelp
) gr ON p.ebeln = gr.ebeln AND p.ebelp = gr.ebelp
LEFT JOIN (
  SELECT ebeln, ebelp, SUM(amount) AS amount_invoiced
  FROM LIVE.silver_ekbe
  WHERE vgabe = '2'
  GROUP BY ebeln, ebelp
) ir ON p.ebeln = ir.ebeln AND p.ebelp = ir.ebelp;

CREATE OR REFRESH MATERIALIZED VIEW fact_nota_fiscal
  COMMENT 'Nota Fiscal fact joining header + line + taxes. One row per docnum + itmnum + taxtyp. Grain: docnum + itmnum + taxtyp.'
AS SELECT
  d.docnum,
  l.itmnum,
  d.bukrs,
  d.nfnum,
  d.bupla,
  d.serie,
  d.direct,
  d.docdat,
  l.matnr,
  l.cfop,
  l.nf_qty,
  l.nf_value,
  t.taxtyp,
  t.tax_value,
  t.tax_rate
FROM LIVE.silver_j_1bnfdoc  d
JOIN LIVE.silver_j_1bnflin  l ON d.docnum = l.docnum
LEFT JOIN LIVE.silver_j_1bnfstx t
  ON l.docnum = t.docnum AND l.itmnum = t.itmnum;
