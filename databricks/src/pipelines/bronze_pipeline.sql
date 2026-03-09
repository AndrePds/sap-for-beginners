-- bronze_pipeline.sql  –  SAP Medallion Bronze Layer
--
-- Ingests JSONL files using Auto Loader (cloud_files).
-- One sub-folder per SAP table name under ${source_base_path}.
-- All tables land in the pipeline schema as  bronze_<table>.
--
-- SAP-type → Bronze SQL type convention:
--   CHAR / NUMC / CUKY / CLNT / DATS / TIMS  → STRING  (preserve leading zeros;
--                                                         DATS cast to DATE in Silver)
--   CURR / DEC / QUAN                         → DOUBLE  (declared via schemaHints)
--   INT4 / INT2                               → INT     (declared via schemaHints)
--
-- Pipeline configuration keys (set in resources/pipeline_sap_medallion.yml):
--   source_base_path  – root path; sub-folders match SAP table names
--   schema_location   – base path for Auto Loader schema-evolution checkpoints
--
-- Metadata columns added to every Bronze table:
--   _ingestion_time  –  row ingestion timestamp
--   _source_file     –  full JSONL source path


-- =============================================================================
-- MASTER DATA
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_kna1
  COMMENT 'Raw SAP KNA1 (Customer Master) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'KNA1', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/kna1/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/kna1',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_lfa1
  COMMENT 'Raw SAP LFA1 (Vendor Master) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'LFA1', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/lfa1/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/lfa1',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_mara
  COMMENT 'Raw SAP MARA (General Material Data) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'MARA', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/mara/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/mara',
    'cloudFiles.schemaHints',      'ntgew DOUBLE, brgew DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_makt
  COMMENT 'Raw SAP MAKT (Material Descriptions) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'MAKT', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/makt/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/makt',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_pa0001
  COMMENT 'Raw SAP PA0001 (HR Org Assignment) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'PA0001', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/pa0001/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/pa0001',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_t001w
  COMMENT 'Raw SAP T001W (Plant/Branch) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'T001W', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/t001w/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/t001w',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_knmt
  COMMENT 'Raw SAP KNMT (Customer-Material Info Record) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'KNMT', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/knmt/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/knmt',
    'cloudFiles.inferColumnTypes', 'false'
  )
);


-- =============================================================================
-- SD – SALES ORDER
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_vbak
  COMMENT 'Raw SAP VBAK (Sales Order Header) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VBAK', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vbak/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/vbak',
    'cloudFiles.schemaHints',      'netwr DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_vbap
  COMMENT 'Raw SAP VBAP (Sales Order Items) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VBAP', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vbap/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/vbap',
    'cloudFiles.schemaHints',      'netwr DOUBLE, kwmeng DOUBLE, kbmeng DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_vbep
  COMMENT 'Raw SAP VBEP (Sales Order Schedule Lines) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VBEP', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vbep/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/vbep',
    'cloudFiles.schemaHints',      'wmeng DOUBLE, bmeng DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_vbfa
  COMMENT 'Raw SAP VBFA (Document Flow) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VBFA', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vbfa/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/vbfa',
    'cloudFiles.schemaHints',      'rfmng DOUBLE, rfwrt DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);


-- =============================================================================
-- SD – BILLING
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_vbrk
  COMMENT 'Raw SAP VBRK (Billing Document Header) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VBRK', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vbrk/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/vbrk',
    'cloudFiles.schemaHints',      'netwr DOUBLE, mwsbk DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_vbrp
  COMMENT 'Raw SAP VBRP (Billing Document Items) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VBRP', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vbrp/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/vbrp',
    'cloudFiles.schemaHints',      'netwr DOUBLE, fklmg DOUBLE, mwsbp DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);


-- =============================================================================
-- SD – DELIVERY
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_likp
  COMMENT 'Raw SAP LIKP (Delivery Header) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'LIKP', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/likp/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/likp',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_lips
  COMMENT 'Raw SAP LIPS (Delivery Items) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'LIPS', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/lips/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/lips',
    'cloudFiles.schemaHints',      'lfimg DOUBLE, lgmng DOUBLE, brgew DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);


-- =============================================================================
-- SD – TRANSPORT
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_vttk
  COMMENT 'Raw SAP VTTK (Transport Header) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VTTK', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vttk/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/vttk',
    'cloudFiles.inferColumnTypes', 'false'
  )
);


-- =============================================================================
-- MM – PURCHASING
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_ekko
  COMMENT 'Raw SAP EKKO (Purchase Order Header) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'EKKO', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/ekko/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/ekko',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_ekpo
  COMMENT 'Raw SAP EKPO (Purchase Order Items) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'EKPO', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/ekpo/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/ekpo',
    'cloudFiles.schemaHints',      'menge DOUBLE, netpr DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_eket
  COMMENT 'Raw SAP EKET (PO Schedule Lines) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'EKET', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/eket/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/eket',
    'cloudFiles.schemaHints',      'menge DOUBLE, wamng DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_ekbe
  COMMENT 'Raw SAP EKBE (PO History / GR-IR) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'EKBE', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/ekbe/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/ekbe',
    'cloudFiles.schemaHints',      'zekkn INT, wrbtr DOUBLE, menge DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);


-- =============================================================================
-- FI-NF – NOTA FISCAL (Brazil)
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_j_1bnfdoc
  COMMENT 'Raw SAP J_1BNFDOC (Nota Fiscal Header) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'J_1BNFDOC', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/j_1bnfdoc/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/j_1bnfdoc',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_j_1bnflin
  COMMENT 'Raw SAP J_1BNFLIN (Nota Fiscal Line Items) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'J_1BNFLIN', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/j_1bnflin/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/j_1bnflin',
    'cloudFiles.schemaHints',      'nfqtd DOUBLE, nfpric DOUBLE, nfval DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);

CREATE OR REFRESH STREAMING TABLE bronze_j_1bnfstx
  COMMENT 'Raw SAP J_1BNFSTX (Nota Fiscal Taxes) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'J_1BNFSTX', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/j_1bnfstx/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/j_1bnfstx',
    'cloudFiles.schemaHints',      'taxval DOUBLE, taxrat DOUBLE',
    'cloudFiles.inferColumnTypes', 'false'
  )
);


-- =============================================================================
-- FI – FINANCIAL ACCOUNTING
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_bkpf
  COMMENT 'Raw SAP BKPF (FI Document Header) – append-only, no business logic.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'BKPF', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/bkpf/', 'json',
  map(
    'cloudFiles.schemaLocation',   '${schema_location}/bkpf',
    'cloudFiles.inferColumnTypes', 'false'
  )
);


-- =============================================================================
-- CONFIG / TEXT TABLES  (Auto Loader schema inference – variable structure)
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_dd07t
  COMMENT 'Raw SAP DD07T (Domain Values/Texts) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'DD07T', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/dd07t/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/dd07t', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_dd07v
  COMMENT 'Raw SAP DD07V (Domain Fixed Values) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'DD07V', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/dd07v/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/dd07v', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_t173t
  COMMENT 'Raw SAP T173T (Shipping Condition Texts) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'T173T', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/t173t/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/t173t', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_tvagt
  COMMENT 'Raw SAP TVAGT (Reason for Rejection Texts) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'TVAGT', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/tvagt/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/tvagt', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_tvaut
  COMMENT 'Raw SAP TVAUT (Order Reason Texts) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'TVAUT', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/tvaut/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/tvaut', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_tvkggt
  COMMENT 'Raw SAP TVKGGT (Sales Group Texts) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'TVKGGT', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/tvkggt/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/tvkggt', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_tvrot
  COMMENT 'Raw SAP TVROT (Route Texts) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'TVROT', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/tvrot/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/tvrot', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_tvsbt
  COMMENT 'Raw SAP TVSBT (Shipping Condition Texts) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'TVSBT', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/tvsbt/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/tvsbt', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_tvstt
  COMMENT 'Raw SAP TVSTT (Shipping Type Texts) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'TVSTT', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/tvstt/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/tvstt', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_konv
  COMMENT 'Raw SAP KONV (Pricing Conditions) – inferred schema (variable condition types).'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'KONV', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/konv/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/konv', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_vbkd
  COMMENT 'Raw SAP VBKD (Sales Order Business Data) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VBKD', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vbkd/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/vbkd', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_vbpa
  COMMENT 'Raw SAP VBPA (Sales Document Partners) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VBPA', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vbpa/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/vbpa', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_vbuk
  COMMENT 'Raw SAP VBUK (Sales Document Header Status) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VBUK', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vbuk/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/vbuk', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_vbup
  COMMENT 'Raw SAP VBUP (Sales Document Item Status) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VBUP', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vbup/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/vbup', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_vttp
  COMMENT 'Raw SAP VTTP (Transport Item) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'VTTP', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/vttp/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/vttp', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_oigs
  COMMENT 'Raw SAP OIGS (Shipment Integration) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'OIGS', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/oigs/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/oigs', 'cloudFiles.inferColumnTypes', 'true')
);

CREATE OR REFRESH STREAMING TABLE bronze_oigsi
  COMMENT 'Raw SAP OIGSI (Shipment Integration Items) – inferred schema.'
  TBLPROPERTIES ('quality' = 'bronze', 'sap.table' = 'OIGSI', 'sap.mandt' = '100')
AS SELECT *, current_timestamp() AS _ingestion_time, input_file_name() AS _source_file
FROM cloud_files(
  '${source_base_path}/oigsi/', 'json',
  map('cloudFiles.schemaLocation', '${schema_location}/oigsi', 'cloudFiles.inferColumnTypes', 'true')
);
