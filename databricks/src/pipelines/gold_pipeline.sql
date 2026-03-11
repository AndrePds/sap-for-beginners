-- gold_pipeline.sql  –  SAP Medallion Gold Layer
--
-- Business-ready KPI tables aggregated from Silver L2 domain models.
-- All tables are  CREATE OR REFRESH MATERIALIZED VIEW  (batch – not streaming).
-- Sources are ${catalog}.${silver_schema}.fact_* and ${catalog}.${silver_schema}.dim_* from the same pipeline.
--
-- KPI tables:
--   kpi_revenue_by_customer_month  –  Monthly billing revenue per customer
--   kpi_order_to_cash              –  OTC cycle: order -> delivery -> billing
--   kpi_purchase_spend_by_vendor   –  PO spend per vendor per month
--   kpi_nota_fiscal_tax_summary    –  NF tax breakdown by type, company, month


-- =============================================================================
-- KPI 1 – Revenue by customer / month
-- Grain: one row per (kunnr, billing_month, currency)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.kpi_revenue_by_customer_month
  COMMENT 'Monthly billing revenue per customer. Source: fact_billing x dim_customer. Excludes cancelled invoices. Grain: kunnr x billing_month x currency.'
AS SELECT
  b.kunnr,
  c.customer_name,
  c.state,
  DATE_TRUNC('month', b.billing_date)          AS billing_month,
  b.currency,
  CAST(SUM(b.item_net_value) AS DECIMAL(17,2)) AS total_net_revenue,
  CAST(SUM(b.item_tax)       AS DECIMAL(17,2)) AS total_tax,
  COUNT(DISTINCT b.vbeln)                      AS invoice_count,
  COUNT(DISTINCT b.matnr)                      AS distinct_materials,
  current_timestamp()                          AS _processed_time
FROM ${catalog}.${silver_schema}.fact_billing b
LEFT JOIN ${catalog}.${silver_schema}.dim_customer c ON b.kunnr = c.kunnr
GROUP BY
  b.kunnr,
  c.customer_name,
  c.state,
  DATE_TRUNC('month', b.billing_date),
  b.currency;


-- =============================================================================
-- KPI 2 – Order-to-Cash cycle time
-- Grain: one row per sales order (vbeln)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.kpi_order_to_cash
  COMMENT 'Order-to-Cash pipeline per sales order. days_to_delivery = order_date -> first goods issue. days_to_billing = order_date -> first billing. NULL = not yet reached that milestone. Grain: one row per vbeln.'
AS WITH order_summary AS (
  SELECT
    vbeln,
    kunnr,
    order_date,
    order_type,
    sales_org,
    currency,
    CAST(SUM(item_net_value) AS DECIMAL(17,2)) AS order_net_value,
    FIRST(overall_status)                       AS overall_status
  FROM ${catalog}.${silver_schema}.fact_sales_order
  GROUP BY vbeln, kunnr, order_date, order_type, sales_org, currency
),
first_delivery AS (
  SELECT source_order, MIN(actual_goods_issue_date) AS first_goods_issue_date
  FROM ${catalog}.${silver_schema}.fact_delivery
  GROUP BY source_order
),
first_billing AS (
  SELECT source_order, MIN(billing_date) AS first_billing_date
  FROM ${catalog}.${silver_schema}.fact_billing
  GROUP BY source_order
)
SELECT
  o.vbeln,
  o.kunnr,
  o.order_date,
  o.order_type,
  o.sales_org,
  o.currency,
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
-- Grain: one row per (lifnr, po_month, company_code)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.kpi_purchase_spend_by_vendor
  COMMENT 'Monthly purchase spend per vendor. amount_invoiced from EKBE event_type=2 (IR) totals. Grain: lifnr x po_month x company_code.'
AS SELECT
  po.lifnr,
  v.vendor_name,
  v.country,
  DATE_TRUNC('month', po.po_date)                  AS po_month,
  po.company_code,
  CAST(SUM(po.amount_invoiced) AS DECIMAL(17,2))   AS total_invoiced,
  CAST(SUM(po.qty_ordered)     AS DECIMAL(17,3))   AS total_qty_ordered,
  CAST(SUM(po.qty_received)    AS DECIMAL(17,3))   AS total_qty_received,
  COUNT(DISTINCT po.ebeln)                         AS po_count,
  COUNT(DISTINCT po.matnr)                         AS distinct_materials,
  current_timestamp()                              AS _processed_time
FROM ${catalog}.${silver_schema}.fact_purchase_order po
LEFT JOIN ${catalog}.${silver_schema}.dim_vendor v ON po.lifnr = v.lifnr
GROUP BY
  po.lifnr,
  v.vendor_name,
  v.country,
  DATE_TRUNC('month', po.po_date),
  po.company_code;


-- =============================================================================
-- KPI 4 – Nota Fiscal tax summary
-- Grain: one row per (company_code, tax_month, tax_type)
-- =============================================================================


CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.kpi_nota_fiscal_tax_summary
  COMMENT 'Monthly NF tax obligations by company code and tax type. Filters direct=O (outbound NFs – fiscal liability). tax_type values: IPI, ICMS, PIS, COFINS, CSLL, ISS. Grain: company_code x tax_month x tax_type.'
AS SELECT
  company_code,
  DATE_TRUNC('month', doc_date)                  AS tax_month,
  tax_type,
  CAST(SUM(tax_value)  AS DECIMAL(17,2))         AS total_tax_value,
  CAST(AVG(tax_rate)   AS DECIMAL(7,4))          AS avg_tax_rate,
  COUNT(DISTINCT docnum)                         AS nf_document_count,
  CAST(SUM(nf_value)   AS DECIMAL(17,2))         AS total_nf_value,
  current_timestamp()                            AS _processed_time
FROM ${catalog}.${silver_schema}.fact_nota_fiscal
WHERE direct = 'O'
GROUP BY
  company_code,
  DATE_TRUNC('month', doc_date),
  tax_type;


-- =============================================================================
-- KPI 5 – ZF1RSD003: Pedidos em Aberto e Faturados (Open & Billed Orders)
-- Grain: one row per sales order item (vbeln + posnr)
-- =============================================================================
-- Replica do relatório transacional ZF1RSD003 (ZF1SDR_PED_ABERTOS_FATURADOS).
--
-- Adaptações para o pipeline Medallion vs. v2 original:
--   - Fontes: Bronze tables (nomes de campo SAP preservados; mandt = '100')
--   - Requer parâmetro de pipeline: ${bronze_schema}  (ex: "bronze")
--   - Sem filtros por parâmetros de execução — cobertura total do dataset
--   - Tabelas Z customizadas (ZF1TSD009/005/CTRC/DTCARTPED) ausentes no
--     dataset educacional → colunas correspondentes retornam NULL
--   - CTEs não contributivos ao output de 34 colunas foram removidos:
--     KONV (pricing), BKPF, VBUK, VBUP, OIGS (status), T173T (MODAL),
--     TVKGGT, TVAUT, TVAGT, TVSTT e totalizadores de impostos NF-e
--   - Idioma fixado em 'P' (Português Brasileiro) para tabelas de texto
--   - Datas DATS (YYYYMMDD STRING) convertidas para DATE no SELECT final
--
-- Referência: tests/convertions-to-sql/ZF1RSD003/v2/ZF1RSD003_conversion_v2.sql
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.zf1rsd003_pedidos_abertos_faturados
  COMMENT 'Replica do relatorio ZF1RSD003: pedidos em aberto e faturados por item de OV. 34 colunas ALV. Grain: vbeln + posnr. Fonte: Bronze (nomes SAP).'
AS WITH

-- ============================================================================
-- SECAO 1: DADOS BASE (TABELAS CONDUTORAS)
-- ============================================================================

-- VBAK: Cabecalho do pedido de venda
-- vsbed -> join com TVSBT (Condicao de Expedicao)
-- augru -> razao do pedido | vdatu -> data desejada de entrega
cte_vbak AS (
  SELECT
    vbeln, vkorg, vtweg, spart,
    auart, audat, kunnr,
    lifsk, faksk, knumv, vsbed,
    vdatu, waerk, erdat, augru
  FROM ${catalog}.${bronze_schema}.bronze_vbak
  WHERE mandt = '100'
),

-- VBAP: Itens do pedido de venda
-- Filtro ABAP: fkrel <> ' ' (somente itens relevantes para faturamento)
-- matkl -> Classificacao do produto | route -> Itinerario OV
cte_vbap AS (
  SELECT
    vbap.vbeln, vbap.posnr, vbap.werks, vbap.matnr,
    vbap.kwmeng, vbap.vrkme, vbap.matkl,
    vbap.lgort, vbap.route
  FROM ${catalog}.${bronze_schema}.bronze_vbap vbap
  INNER JOIN (SELECT DISTINCT vbeln FROM cte_vbak) vbak_keys
    ON vbap.vbeln = vbak_keys.vbeln
  WHERE vbap.mandt = '100'
    AND (vbap.fkrel IS NULL OR vbap.fkrel <> ' ')
),

-- ============================================================================
-- SECAO 2: PROGRAMACAO DE ENTREGA (VBEP)
-- ============================================================================

-- VBEP: Linhas de programacao — etenr define a ordem cronologica
-- ABAP: READ TABLE BINARY SEARCH -> pega primeiro registro (menor etenr)
-- [v2.3-F6] edatu = "1a data" (data de entrega programada, nao wadat/smerc)
cte_vbep AS (
  SELECT vbep.vbeln, vbep.posnr, vbep.etenr,
         vbep.wadat, vbep.edatu
  FROM ${catalog}.${bronze_schema}.bronze_vbep vbep
  INNER JOIN (SELECT DISTINCT vbeln FROM cte_vbak) vbak_keys
    ON vbep.vbeln = vbak_keys.vbeln
  WHERE vbep.mandt = '100'
),

-- Primeira linha de programacao por item (etenr = menor -> rn = 1)
-- KB: ABAP READ TABLE BINARY SEARCH -> ROW_NUMBER() OVER (ORDER BY etenr ASC)
cte_vbep_first AS (
  SELECT vbeln, posnr, wadat AS smerc, edatu
  FROM (
    SELECT vbeln, posnr, wadat, edatu,
           ROW_NUMBER() OVER (PARTITION BY vbeln, posnr ORDER BY etenr ASC) AS rn
    FROM cte_vbep
  ) t
  WHERE rn = 1
),

-- ============================================================================
-- SECAO 3: DADOS COMERCIAIS (VBKD)
-- ============================================================================

-- VBKD: Dados comerciais do pedido (nivel de cabecalho, posnr = '000000')
-- inco1 -> Condicao de entrega (Incoterms parte 1)
-- vsart -> para join com T173T (coluna MODAL — nao incluida no output)
cte_vbkd AS (
  SELECT vbkd.vbeln, vbkd.inco1, vbkd.vsart
  FROM ${catalog}.${bronze_schema}.bronze_vbkd vbkd
  INNER JOIN (SELECT DISTINCT vbeln FROM cte_vbap) vbap_keys
    ON vbkd.vbeln = vbap_keys.vbeln
  WHERE vbkd.mandt  = '100'
    AND vbkd.posnr  = '000000'
),

-- ============================================================================
-- SECAO 4: PARCEIROS DO PEDIDO (VBPA / PA0001)
-- ============================================================================

-- VBPA: Funcoes de parceiro associadas ao pedido
-- KB: FOR ALL ENTRIES -> INNER JOIN; depois filtrado por parvw abaixo
cte_vbpa AS (
  SELECT vbpa.vbeln, vbpa.posnr, vbpa.kunnr, vbpa.pernr, vbpa.parvw
  FROM ${catalog}.${bronze_schema}.bronze_vbpa vbpa
  INNER JOIN (SELECT DISTINCT vbeln FROM cte_vbap) vbap_keys
    ON vbpa.vbeln = vbap_keys.vbeln
  WHERE vbpa.mandt = '100'
),

-- GC — Gerente Comercial (parvw = 'ZG')
cte_vbpa_gc AS (
  SELECT vbeln, pernr FROM cte_vbpa WHERE parvw = 'ZG'
),

-- AV — Assistente de Vendas (parvw = 'ZA')
cte_vbpa_av AS (
  SELECT vbeln, pernr FROM cte_vbpa WHERE parvw = 'ZA'
),

-- WE — Recebedor de Mercadoria (parvw = 'WE')
cte_vbpa_rec AS (
  SELECT vbeln, kunnr FROM cte_vbpa WHERE parvw = 'WE'
),

-- PA0001: Nomes dos funcionarios para GC e Assistente de Vendas
-- Nota UC: campo ename contem nome completo pre-concatenado (vorna + nachn)
-- pernr e numero de pessoal de 8 digitos com zero a esquerda
cte_pa0001 AS (
  SELECT DISTINCT pa.pernr, pa.ename
  FROM ${catalog}.${bronze_schema}.bronze_pa0001 pa
  INNER JOIN (
    SELECT DISTINCT pernr FROM cte_vbpa
    WHERE pernr IS NOT NULL AND pernr <> ''
  ) vbpa_keys ON pa.pernr = vbpa_keys.pernr
  WHERE pa.mandt = '100'
),

-- Nome do GC (Gerente Comercial)
cte_gc_name AS (
  SELECT gc.vbeln, pa.ename AS ename_gc
  FROM cte_vbpa_gc gc
  LEFT JOIN cte_pa0001 pa ON gc.pernr = pa.pernr
),

-- Nome do Assistente de Vendas
cte_av_name AS (
  SELECT av.vbeln, pa.ename AS ename_av
  FROM cte_vbpa_av av
  LEFT JOIN cte_pa0001 pa ON av.pernr = pa.pernr
),

-- ============================================================================
-- SECAO 5: REMESSAS (LIPS / LIKP)
-- ============================================================================

-- LIPS: Itens de remessa — join multi-campo: vgbel+vgpos = ordem+item
-- KB: FOR ALL ENTRIES multi-campo -> INNER JOIN multi-campo
-- lgort da LIPS = Deposito da Remessa (diferente do lgort da VBAP)
cte_lips AS (
  SELECT lips.vbeln, lips.posnr, lips.vgbel, lips.vgpos,
         lips.lfimg, lips.lgort, lips.matnr, lips.werks, lips.vrkme
  FROM ${catalog}.${bronze_schema}.bronze_lips lips
  INNER JOIN (SELECT DISTINCT vbeln, posnr FROM cte_vbap) vbap_keys
    ON lips.vgbel = vbap_keys.vbeln
   AND lips.vgpos = vbap_keys.posnr
  WHERE lips.mandt = '100'
),

-- LIKP: Cabecalho da remessa
-- wadat_ist = data real de saida de mercadoria (goods issue)
cte_likp AS (
  SELECT likp.vbeln, likp.wadat_ist, likp.lfdat, likp.lfart
  FROM ${catalog}.${bronze_schema}.bronze_likp likp
  INNER JOIN (SELECT DISTINCT vbeln FROM cte_lips) lips_keys
    ON likp.vbeln = lips_keys.vbeln
  WHERE likp.mandt = '100'
),

-- ============================================================================
-- SECAO 6: TRANSPORTE (VTTP / VTTK)
-- ============================================================================

-- VTTP: Itens do transporte — liga remessa ao cabecalho do transporte
cte_vttp AS (
  SELECT vttp.tknum, vttp.tpnum, vttp.vbeln
  FROM ${catalog}.${bronze_schema}.bronze_vttp vttp
  INNER JOIN (SELECT DISTINCT vbeln FROM cte_likp) likp_keys
    ON vttp.vbeln = likp_keys.vbeln
  WHERE vttp.mandt = '100'
),

-- VTTK: Cabecalho do transporte
-- tdlnr -> LFA1 para obter o nome da transportadora (coluna 14)
cte_vttk AS (
  SELECT vttk.tknum, vttk.shtyp, vttk.route, vttk.tdlnr
  FROM ${catalog}.${bronze_schema}.bronze_vttk vttk
  INNER JOIN (SELECT DISTINCT tknum FROM cte_vttp) vttp_keys
    ON vttk.tknum = vttp_keys.tknum
  WHERE vttk.mandt = '100'
),

-- ============================================================================
-- SECAO 7: FATURAS (VBRP / VBRK)
-- ============================================================================

-- VBRP: Itens de fatura — join por aubel+aupos (referencia ao pedido)
-- vgbel/vgpos referenciam a remessa (usados no JOIN final via lips)
cte_vbrp AS (
  SELECT vbrp.vbeln, vbrp.posnr, vbrp.aubel, vbrp.aupos,
         vbrp.vgbel, vbrp.vgpos, vbrp.fklmg, vbrp.meins
  FROM ${catalog}.${bronze_schema}.bronze_vbrp vbrp
  INNER JOIN (SELECT DISTINCT vbeln, posnr FROM cte_vbap) vbap_keys
    ON vbrp.aubel = vbap_keys.vbeln
   AND vbrp.aupos = vbap_keys.posnr
  WHERE vbrp.mandt = '100'
),

-- VBRK: Cabecalho da fatura
-- [F1] Filtro NULL-safe: Delta armazena ABAP '' como NULL
--   sfakn nulo/vazio -> sem referencia de reversao (fatura ativa)
--   fksto nulo/diferente de 'X' -> nao cancelada
cte_vbrk AS (
  SELECT vbrk.vbeln, vbrk.waerk, vbrk.fkdat, vbrk.knumv
  FROM ${catalog}.${bronze_schema}.bronze_vbrk vbrk
  INNER JOIN (SELECT DISTINCT vbeln FROM cte_vbrp) vbrp_keys
    ON vbrk.vbeln = vbrp_keys.vbeln
  WHERE vbrk.mandt = '100'
    AND (vbrk.sfakn IS NULL OR vbrk.sfakn = '')
    AND (vbrk.fksto IS NULL OR vbrk.fksto <> 'X')
),

-- ============================================================================
-- SECAO 8: NOTA FISCAL (VBFA -> J_1BNFLIN -> J_1BNFDOC)
-- ============================================================================

-- VBFA: Fluxo de documentos — localiza NF-e vinculadas ao pedido
-- vbtyp_n = 'M' filtra somente documentos do tipo Nota Fiscal
-- KB: FOR ALL ENTRIES -> INNER JOIN; vbelv = pedido, vbeln = NF key
cte_vbfa AS (
  SELECT vbfa.vbelv, vbfa.vbeln
  FROM ${catalog}.${bronze_schema}.bronze_vbfa vbfa
  INNER JOIN (SELECT DISTINCT vbeln FROM cte_vbak) vbak_keys
    ON vbfa.vbelv = vbak_keys.vbeln
  WHERE vbfa.mandt    = '100'
    AND vbfa.vbtyp_n  = 'M'
),

-- J_1BNFLIN: Itens da Nota Fiscal Eletronica
-- refkey liga ao numero do documento via VBFA
-- nfqtd = Quantidade NF (campo gerado no dataset educacional)
cte_j1bnflin AS (
  SELECT nflin.docnum, nflin.itmnum, nflin.refkey,
         nflin.nfqtd, nflin.meins, nflin.cfop
  FROM ${catalog}.${bronze_schema}.bronze_j_1bnflin nflin
  INNER JOIN (SELECT DISTINCT vbeln FROM cte_vbfa) vbfa_keys
    ON nflin.refkey = vbfa_keys.vbeln
  WHERE nflin.mandt = '100'
),

-- J_1BNFDOC: Cabecalho da Nota Fiscal Eletronica
-- docdat = Data de emissao da NF (string YYYYMMDD -> convertido no SELECT)
cte_j1bnfdoc AS (
  SELECT nfdoc.docnum, nfdoc.nfnum, nfdoc.docdat
  FROM ${catalog}.${bronze_schema}.bronze_j_1bnfdoc nfdoc
  INNER JOIN (SELECT DISTINCT docnum FROM cte_j1bnflin) nflin_keys
    ON nfdoc.docnum = nflin_keys.docnum
  WHERE nfdoc.mandt = '100'
),

-- ============================================================================
-- SECAO 9: DADOS CADASTRAIS (KNA1 / LFA1 / MARA)
-- ============================================================================

-- KNA1: Cliente emissor do pedido (VBAK.kunnr)
-- ort01/regio usados como fallback para Municipio/UF Destinatario
cte_kna1 AS (
  SELECT kna1.kunnr, kna1.name1, kna1.ort01, kna1.regio, kna1.mcod1
  FROM ${catalog}.${bronze_schema}.bronze_kna1 kna1
  INNER JOIN (SELECT DISTINCT kunnr FROM cte_vbak) vbak_keys
    ON kna1.kunnr = vbak_keys.kunnr
  WHERE kna1.mandt = '100'
),

-- KNA1: Recebedor de Mercadoria (parceiro WE via VBPA)
-- [v2] Inclui ort01 e regio para colunas Municipio/UF Destinatario (cols 8-9)
cte_kna1_rec AS (
  SELECT kna1.kunnr, kna1.name1, kna1.ort01, kna1.regio
  FROM ${catalog}.${bronze_schema}.bronze_kna1 kna1
  INNER JOIN (SELECT DISTINCT kunnr FROM cte_vbpa_rec) rec_keys
    ON kna1.kunnr = rec_keys.kunnr
  WHERE kna1.mandt = '100'
),

-- LFA1: Transportadora (via cadeia LIPS -> LIKP -> VTTP -> VTTK.tdlnr)
cte_lfa1 AS (
  SELECT lfa1.lifnr, lfa1.name1
  FROM ${catalog}.${bronze_schema}.bronze_lfa1 lfa1
  INNER JOIN (SELECT DISTINCT tdlnr FROM cte_vttk WHERE tdlnr IS NOT NULL) vttk_keys
    ON lfa1.lifnr = vttk_keys.tdlnr
  WHERE lfa1.mandt = '100'
),

-- MARA: Dados do material
-- prdha: chars 1-5 = Linha, chars 6-10 = Familia
-- bismt: numero antigo do material = codigo de Grade  [v2 FIX: nao e prdha_3]
cte_mara AS (
  SELECT mara.matnr, mara.bismt, mara.prdha
  FROM ${catalog}.${bronze_schema}.bronze_mara mara
  INNER JOIN (SELECT DISTINCT matnr FROM cte_vbap) vbap_keys
    ON mara.matnr = vbap_keys.matnr
  WHERE mara.mandt = '100'
),

-- ============================================================================
-- SECAO 10: TEXTOS DE CONFIGURACAO
-- ============================================================================

-- TVSBT: Textos da condicao de expedicao (VBAK.vsbed -> TVSBT.vtext)
-- [v2.3-F5] Fonte correta para coluna "Condicao de Expedicao" (col 31)
--   Erro anterior: T173T.bezei/vbkd.vsart -> esses alimentam a coluna MODAL
--   Correto: fieldcat 'VTEXT' 'TVSBT' via VBAK.vsbed
cte_tvsbt AS (
  SELECT tvsbt.vsbed, tvsbt.vtext
  FROM ${catalog}.${bronze_schema}.bronze_tvsbt tvsbt
  INNER JOIN (
    SELECT DISTINCT vsbed FROM cte_vbak WHERE vsbed IS NOT NULL AND vsbed <> ''
  ) vbak_keys ON tvsbt.vsbed = vbak_keys.vsbed
  WHERE tvsbt.mandt = '100'
    AND tvsbt.spras = 'P'
),

-- ============================================================================
-- SECAO 11: ORDEM DE TRANSPORTE (OIGSI)
-- ============================================================================

-- OIGSI: Numero de OT (Ordem de Transporte) ligado a remessa
-- doc_number = LIKP.vbeln (entrega), doc_typ = 'J'
-- [v2.4-F8] doc_typ='J' confirmado em producao (revertido de '7' em v2.2)
-- Nota: cobertura de dados pode ser esparsa — registros podem nao existir
cte_oigsi AS (
  SELECT oigsi.shnumber, oigsi.doc_number
  FROM ${catalog}.${bronze_schema}.bronze_oigsi oigsi
  INNER JOIN (SELECT DISTINCT vbeln FROM cte_likp) likp_keys
    ON oigsi.doc_number = likp_keys.vbeln
  WHERE oigsi.mandt   = '100'
    AND oigsi.doc_typ = 'J'
)

-- ============================================================================
-- SECAO 12: MONTAGEM FINAL — 34 COLUNAS DO RELATORIO ZF1RSD003
-- ============================================================================
-- Driver: cte_vbap (item de OV) com INNER JOIN em cte_vbak (cabecalho)
-- Todos os demais sao LEFT JOINs (dados opcionais / nem sempre presentes)
--
-- Mapeamento de colunas (nome ALV -> expressao SQL -> fonte):
--   1  Nr da ordem              vbak.vbeln
--   2  Nr da linha da ordem     vbap.posnr
--   3  Emissor da ordem         vbak.kunnr
--   4  Recebedor Mercadoria     COALESCE(vbpa_rec.kunnr, vbak.kunnr)
--   5  Recebedor Nome Fantasia  COALESCE(kna1_rec.name1, kna1.name1)
--   6  OT                       oigsi.shnumber              (OIGSI via LIKP)
--   7  Nome Destinatario        COALESCE(kna1_rec.name1, kna1.name1)
--   8  Municipio Destinatario   COALESCE(kna1_rec.ort01, kna1.ort01)
--   9  UF Destinatario          COALESCE(kna1_rec.regio, kna1.regio)
--  10  Centro Origem            vbap.werks
--  11  Deposito da Remessa      lips.lgort                  (LIPS)
--  12  Deposito da Venda        vbap.lgort                  (VBAP)
--  13  Itiner. OV               vbap.route
--  14  Transportadora           lfa1.name1                  (LFA1 via VTTK)
--  15  Data Emissao NF          nfdoc.docdat -> DATE        (J_1BNFDOC)
--  16  Data do Faturamento      vbrk.fkdat -> DATE          (VBRK)
--  17  Dt. Desej. Entrega       vbak.vdatu -> DATE
--  18  GC                       gc_name.ename_gc            (PA0001 parvw=ZG)
--  19  Assistente de Vendas     av_name.ename_av            (PA0001 parvw=ZA)
--  20  Linha                    SUBSTRING(mara.prdha, 1, 5)
--  21  Familia                  SUBSTRING(mara.prdha, 6, 5)
--  22  Grade                    mara.bismt                  (nao prdha_3)
--  23  Item                     LTRIM(vbap.matnr, '0')      (sem zeros)
--  24  Classificacao do produto vbap.matkl
--  25  Quantidade NF            nflin.nfqtd                 (J_1BNFLIN)
--  26  Unidade de Medida        COALESCE(nflin.meins, vbap.vrkme)
--  27  Tp Carga Email           NULL                        (ZF1TSD009/005)
--  28  Observacao OV            NULL                        (READ_TEXT)
--  29  Tipo de Ordem            vbak.auart
--  30  Condicao de entrega      vbkd.inco1                  (VBKD)
--  31  Condicao de Expedicao    tvsbt.vtext                 (TVSBT via vsbed)
--  32  1a data                  vbep_f.edatu -> DATE        (VBEP rn=1)
--  33  1a data desejada cliente NULL                        (campo Z zzdate_request)
--  34  Dt Referencia Template   NULL                        (campo Z zzdate_template)

SELECT
  -- 1 – Nr da ordem
  vbak.vbeln                                        AS nr_ordem,

  -- 2 – Nr da linha da ordem
  vbap.posnr                                        AS nr_linha_ordem,

  -- 3 – Emissor da ordem (cliente que emitiu o pedido)
  vbak.kunnr                                        AS emissor_ordem,

  -- 4 – Recebedor Mercadoria (parceiro WE; fallback: emissor)
  COALESCE(vbpa_rec.kunnr, vbak.kunnr)              AS receb_mercadoria,

  -- 5 – Recebedor Nome Fantasia (name1 do parceiro WE; fallback: emissor)
  COALESCE(kna1_rec.name1, kna1.name1)              AS receb_nome_fantasia,

  -- 6 – OT (Ordem de Transporte / shnumber de OIGSI)
  -- Cadeia: VBAP -> LIPS -> LIKP -> OIGSI (doc_typ='J')
  -- [v2.4-F8] doc_typ='J' revertido de '7' (confirmado em producao)
  oigsi.shnumber                                    AS ot,

  -- 7 – Nome Destinatario (reutiliza nome do recebedor)
  COALESCE(kna1_rec.name1, kna1.name1)              AS nome_destinatario,

  -- 8 – Municipio Destinatario (ort01 do recebedor WE; fallback: emissor)
  COALESCE(kna1_rec.ort01, kna1.ort01)              AS municipio_destinatario,

  -- 9 – UF Destinatario (regio do recebedor WE; fallback: emissor)
  COALESCE(kna1_rec.regio, kna1.regio)              AS uf_destinatario,

  -- 10 – Centro Origem (planta do item de OV)
  vbap.werks                                        AS centro_origem,

  -- 11 – Deposito da Remessa (lgort da LIPS — local real de saida)
  lips.lgort                                        AS deposito_remessa,

  -- 12 – Deposito da Venda (lgort da VBAP — local planejado no pedido)
  vbap.lgort                                        AS deposito_venda,

  -- 13 – Itiner. OV (rota do item de OV, VBAP.route)
  vbap.route                                        AS itiner_ov,

  -- 14 – Transportadora (LFA1.name1 via cadeia LIPS->LIKP->VTTP->VTTK.tdlnr)
  lfa1.name1                                        AS transportadora,

  -- 15 – Data Emissao NF (J_1BNFDOC.docdat, DATS '00000000'/'99991231' -> NULL)
  CASE
    WHEN nfdoc.docdat IN ('00000000', '99991231') OR nfdoc.docdat IS NULL THEN NULL
    ELSE TO_DATE(nfdoc.docdat, 'yyyyMMdd')
  END                                               AS data_emissao_nf,

  -- 16 – Data do Faturamento (VBRK.fkdat)
  CASE
    WHEN vbrk.fkdat IN ('00000000', '99991231') OR vbrk.fkdat IS NULL THEN NULL
    ELSE TO_DATE(vbrk.fkdat, 'yyyyMMdd')
  END                                               AS data_faturamento,

  -- 17 – Dt. Desejada Entrega (VBAK.vdatu)
  CASE
    WHEN vbak.vdatu IN ('00000000', '99991231') OR vbak.vdatu IS NULL THEN NULL
    ELSE TO_DATE(vbak.vdatu, 'yyyyMMdd')
  END                                               AS dt_desej_entrega,

  -- 18 – GC (Gerente Comercial, parceiro parvw='ZG' -> PA0001.ename)
  gc_name.ename_gc                                  AS gc,

  -- 19 – Assistente de Vendas (parceiro parvw='ZA' -> PA0001.ename)
  av_name.ename_av                                  AS assistente_vendas,

  -- 20 – Linha (hierarquia de produto: chars 1-5 de MARA.prdha)
  TRIM(SUBSTRING(mara.prdha, 1, 5))                 AS linha,

  -- 21 – Familia (hierarquia de produto: chars 6-10 de MARA.prdha)
  TRIM(SUBSTRING(mara.prdha, 6, 5))                 AS familia,

  -- 22 – Grade (MARA.bismt — numero antigo do material = codigo de grade)
  -- [v2 FIX] Nao usa prdha_3/SUBSTRING(prdha,11,18); bismt confirmado pelo ALV
  mara.bismt                                        AS grade,

  -- 23 – Item (matnr sem zeros a esquerda — exibicao SAP padrao MATNR_OUTPUT)
  -- [v2.3-F7] Fieldcat 'MATNR' "ITEM"; maktx alimenta coluna DESCMAT separada
  LTRIM(vbap.matnr, '0')                            AS item,

  -- 24 – Classificacao do produto (VBAP.matkl)
  -- [v2 FIX] ABAP linha 8160: ty_relatorio-matkl = t_vbap-matkl
  vbap.matkl                                        AS classificacao_produto,

  -- 25 – Quantidade NF (J_1BNFLIN.nfqtd — quantidade na Nota Fiscal)
  CAST(nflin.nfqtd AS DECIMAL(17, 3))               AS quantidade_nf,

  -- 26 – Unidade de Medida (unidade da NF; fallback: unidade de venda do pedido)
  COALESCE(nflin.meins, vbap.vrkme)                 AS unidade_medida,

  -- 27 – Tp Carga Email — NULL: ZF1TSD009/ZF1TSD005 nao disponiveis no dataset
  CAST(NULL AS STRING)                              AS tp_carga_email,

  -- 28 – Observacao OV — NULL: READ_TEXT (cluster STXL) nao disponivel em SQL
  CAST(NULL AS STRING)                              AS obs_ov,

  -- 29 – Tipo de Ordem (VBAK.auart)
  vbak.auart                                        AS tipo_ordem,

  -- 30 – Condicao de entrega (VBKD.inco1 — Incoterms parte 1)
  vbkd.inco1                                        AS condicao_entrega,

  -- 31 – Condicao de Expedicao (TVSBT.vtext via VBAK.vsbed)
  -- [v2.3-F5] Fieldcat L8155: 'VTEXT' 'TVSBT' — nao e T173T (que e coluna MODAL)
  tvsbt.vtext                                       AS condicao_expedicao,

  -- 32 – 1a data (VBEP.edatu — data de entrega da 1a linha de programacao)
  -- [v2.3-F6] Fieldcat 'EDATU'; wadat/smerc alimenta coluna SMERC separada
  CASE
    WHEN vbep_f.edatu IN ('00000000', '99991231') OR vbep_f.edatu IS NULL THEN NULL
    ELSE TO_DATE(vbep_f.edatu, 'yyyyMMdd')
  END                                               AS primeira_data,

  -- 33 – 1a data desejada do cliente — NULL: campo Z zzdate_request (Braskem)
  CAST(NULL AS DATE)                                AS primeira_data_desejada,

  -- 34 – Dt Referencia Template — NULL: campo Z zzdate_template (Braskem)
  CAST(NULL AS DATE)                                AS dt_referencia_template,

  current_timestamp()                               AS _processed_time

FROM cte_vbap vbap

-- Cabecalho do pedido (obrigatorio — define o universo de dados)
INNER JOIN cte_vbak vbak
  ON vbap.vbeln = vbak.vbeln

-- Dados comerciais (nivel de cabecalho, posnr='000000')
LEFT JOIN cte_vbkd vbkd
  ON vbap.vbeln = vbkd.vbeln

-- Parceiros do pedido
LEFT JOIN cte_vbpa_rec vbpa_rec  ON vbap.vbeln = vbpa_rec.vbeln
LEFT JOIN cte_gc_name gc_name    ON vbap.vbeln = gc_name.vbeln
LEFT JOIN cte_av_name av_name    ON vbap.vbeln = av_name.vbeln

-- Dados cadastrais do cliente (emissor e recebedor)
LEFT JOIN cte_kna1 kna1
  ON vbak.kunnr = kna1.kunnr
LEFT JOIN cte_kna1_rec kna1_rec
  ON vbpa_rec.kunnr = kna1_rec.kunnr

-- Material
LEFT JOIN cte_mara mara
  ON vbap.matnr = mara.matnr

-- Remessa (0..N por item de OV; pode haver multiplas remessas)
LEFT JOIN cte_lips lips
  ON vbap.vbeln = lips.vgbel
 AND vbap.posnr = lips.vgpos
LEFT JOIN cte_likp likp
  ON lips.vbeln = likp.vbeln

-- Cadeia de transporte: LIPS -> VTTP -> VTTK -> LFA1
LEFT JOIN cte_vttp vttp  ON lips.vbeln  = vttp.vbeln
LEFT JOIN cte_vttk vttk  ON vttp.tknum  = vttk.tknum
LEFT JOIN cte_lfa1 lfa1  ON vttk.tdlnr  = lfa1.lifnr

-- Fatura (via remessa; somente faturas ativas e nao revertidas)
LEFT JOIN cte_vbrp vbrp
  ON lips.vbeln  = vbrp.vgbel
 AND lips.posnr  = vbrp.vgpos
LEFT JOIN cte_vbrk vbrk
  ON vbrp.vbeln = vbrk.vbeln

-- Cadeia NF-e: VBFA (vbtyp_n='M') -> J_1BNFLIN -> J_1BNFDOC
LEFT JOIN cte_vbfa vbfa
  ON vbak.vbeln = vbfa.vbelv
LEFT JOIN cte_j1bnflin nflin
  ON vbfa.vbeln = nflin.refkey
LEFT JOIN cte_j1bnfdoc nfdoc
  ON nflin.docnum = nfdoc.docnum

-- Texto: Condicao de Expedicao (TVSBT via vsbed)
LEFT JOIN cte_tvsbt tvsbt
  ON vbak.vsbed = tvsbt.vsbed

-- Ordem de Transporte (OIGSI via LIKP)
LEFT JOIN cte_oigsi oigsi
  ON likp.vbeln = oigsi.doc_number

-- Programacao de entrega (primeira linha por item — etenr ASC, rn=1)
LEFT JOIN cte_vbep_first vbep_f
  ON vbap.vbeln = vbep_f.vbeln
 AND vbap.posnr = vbep_f.posnr;
