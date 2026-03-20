# 📗 SAP — Módulo 8: Integração com Azure
### ADF + SAP Connector | Databricks + pyrfc | Delta Lake com Dados SAP

---

> **Objetivo do módulo:** Construir pipelines de dados de ponta a ponta entre SAP e Azure — usando Azure Data Factory para orquestração, Databricks para transformação e Delta Lake como camada de armazenamento analítico.

---

## 1. Arquitetura de Referência — SAP → Azure

```
┌─────────────────────────────────────────────────────────────────┐
│                        SAP (on-premise)                         │
│  S/4HANA / ECC                                                  │
│  ├── Tabelas (EKKO, VBAK, ACDOCA...)                            │
│  ├── BAPIs / RFC Functions                                      │
│  ├── OData APIs (SAP Gateway)                                   │
│  └── IDocs                                                      │
└────────────────────┬────────────────────────────────────────────┘
                     │
         ┌───────────▼───────────┐
         │  Self-Hosted IR (SHIR) │  ← Máquina on-prem com
         │  ou SAP HANA Connector │    agente ADF instalado
         └───────────┬───────────┘
                     │  (rede privada / VPN / ExpressRoute)
┌────────────────────▼────────────────────────────────────────────┐
│                     Microsoft Azure                              │
│                                                                  │
│  Azure Data Factory (ADF)                                        │
│  ├── Linked Services (conexões)                                  │
│  ├── Datasets (mapeamento de tabelas/entidades)                  │
│  ├── Pipelines (orquestração)                                    │
│  └── Triggers (agendamento / event-based)                        │
│                   ↓                                              │
│  Azure Data Lake Storage Gen2 (ADLS)                             │
│  ├── raw/     → dados brutos SAP (JSON, Parquet, CSV)            │
│  ├── bronze/  → dados ingeridos, schema aplicado                 │
│  ├── silver/  → dados limpos e transformados                     │
│  └── gold/    → dados analíticos / agregados                     │
│                   ↓                                              │
│  Azure Databricks                                                │
│  ├── Notebooks PySpark                                           │
│  ├── Delta Live Tables / Lakeflow Pipelines                      │
│  └── Unity Catalog                                               │
│                   ↓                                              │
│  Consumo analítico                                               │
│  ├── Power BI                                                    │
│  ├── Azure Synapse Analytics                                     │
│  └── SAP Analytics Cloud                                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Azure Data Factory — Conectores SAP

### 2.1 Conectores SAP disponíveis no ADF

| Conector | Protocolo | Melhor para |
|----------|-----------|-------------|
| **SAP Table** | RFC / BAPI RFC_READ_TABLE | Leitura direta de tabelas SAP |
| **SAP ODP** | Operational Data Provisioning | Delta extraction com DataSources BW |
| **SAP ECC** | RFC / BAPIs customizadas | ECC com RFC genérico |
| **SAP HANA** | JDBC / ODBC | Conexão direta ao banco HANA |
| **SAP BW** | BEx / OLAP BAPI | InfoCubes e QueryCubes |
| **SAP Cloud for Customer** | OData | SAP C4C |
| **OData genérico** | OData v2/v4 | SAP Gateway, BTP APIs |

### 2.2 Configuração do SAP Table Connector

#### Pré-requisitos
- **Self-Hosted Integration Runtime (SHIR)** instalado em máquina com acesso à rede SAP
- Biblioteca **SAP NCo 3.0** (SAP .NET Connector) instalada no SHIR
- Usuário SAP com perfil de leitura RFC

#### Linked Service — SAP Table

```json
{
  "name": "LS_SAP_ECC_PRODUCAO",
  "type": "SapTable",
  "properties": {
    "connectVia": {
      "referenceName": "SHIR_OnPrem",
      "type": "IntegrationRuntimeReference"
    },
    "typeProperties": {
      "server": "sap-app-server.braskem.com.br",
      "systemNumber": "00",
      "clientId": "100",
      "userName": "SVC_ADF_EXTRACTOR",
      "password": {
        "type": "AzureKeyVaultSecret",
        "store": { "referenceName": "AKV_Braskem", "type": "LinkedServiceReference" },
        "secretName": "sap-extractor-password"
      },
      "sncMode": "1",
      "sncMyName": "p:CN=SVC_ADF, OU=...",
      "sncPartnerName": "p:CN=SAP_SERVER, OU=..."
    }
  }
}
```

> 💡 **Segurança:** NUNCA armazenar senha em plain text no Linked Service. Sempre usar **Azure Key Vault** para credenciais SAP.

#### Dataset — Tabela SAP

```json
{
  "name": "DS_SAP_EKKO",
  "type": "SapTableResource",
  "properties": {
    "linkedServiceName": {
      "referenceName": "LS_SAP_ECC_PRODUCAO",
      "type": "LinkedServiceReference"
    },
    "typeProperties": {
      "tableName": "EKKO"
    }
  }
}
```

#### Copy Activity — Extraindo EKKO para ADLS

```json
{
  "name": "Copy_EKKO_to_ADLS",
  "type": "Copy",
  "source": {
    "type": "SapTableSource",
    "rfcTableFields": "EBELN,LIFNR,EKORG,BEDAT,NETWR,WAERS,BSTYP",
    "rfcTableOptions": "BEDAT GE '20240101' AND BSTYP EQ 'F'",
    "batchSize": 2000,
    "parallelCopies": 4
  },
  "sink": {
    "type": "ParquetSink",
    "storeSettings": {
      "type": "AzureBlobFSWriteSettings"
    }
  },
  "enableStaging": false
}
```

### 2.3 SAP ODP Connector — Extração Delta

O conector **SAP ODP** é o mais indicado para **cargas incrementais** pois usa o mecanismo delta nativo do SAP:

```json
{
  "name": "Copy_ODP_PO_Delta",
  "type": "Copy",
  "source": {
    "type": "SapOdpSource",
    "extractionMode": "Delta",
    "backfillAfterLastDateTime": "2024-01-01T00:00:00Z",
    "selection": [
      {
        "fieldName": "EKORG",
        "sign": "I",
        "option": "EQ",
        "low": "BRAS"
      }
    ]
  },
  "sink": {
    "type": "ParquetSink"
  }
}
```

#### Modos de extração ODP

| Modo | Comportamento |
|------|--------------|
| `FullLoad` | Extração completa — ignora deltas anteriores |
| `Delta` | Apenas registros novos/alterados desde última extração |
| `Recovery` | Re-executa o último delta sem avançar o ponteiro |

### 2.4 Pipeline ADF completo — Ingestão SAP incremental

```json
{
  "name": "PL_SAP_Ingestao_Incremental",
  "activities": [
    {
      "name": "Get_Watermark",
      "type": "Lookup",
      "source": {
        "type": "AzureSqlSource",
        "sqlReaderQuery": "SELECT MAX(ultima_extracao) FROM ctrl.watermark WHERE tabela = 'EKKO'"
      }
    },
    {
      "name": "Copy_EKKO_Delta",
      "type": "Copy",
      "dependsOn": [{ "activity": "Get_Watermark", "dependencyConditions": ["Succeeded"] }],
      "source": {
        "type": "SapTableSource",
        "rfcTableOptions": "@concat('AEDAT GE ''', formatDateTime(activity('Get_Watermark').output.firstRow.ultima_extracao, 'yyyyMMdd'), '''')"
      },
      "sink": {
        "type": "ParquetSink",
        "fileNamePrefix": "@concat('ekko_', formatDateTime(utcNow(), 'yyyyMMdd_HHmmss'))"
      }
    },
    {
      "name": "Update_Watermark",
      "type": "SqlServerStoredProcedure",
      "dependsOn": [{ "activity": "Copy_EKKO_Delta", "dependencyConditions": ["Succeeded"] }],
      "storedProcedureName": "ctrl.usp_update_watermark",
      "storedProcedureParameters": {
        "tabela": { "value": "EKKO", "type": "String" },
        "data_extracao": { "value": "@utcNow()", "type": "String" }
      }
    }
  ]
}
```

---

## 3. Databricks + pyrfc — Extração Direta via RFC

### 3.1 Configuração do pyrfc no Databricks

```python
# Cluster init script para instalar pyrfc
# Arquivo: /dbfs/FileStore/init_scripts/install_pyrfc.sh

#!/bin/bash
# Instalar SAP NW RFC Library (requer download do SAP Support Portal)
pip install pyrfc

# Verificar instalação
python -c "import pyrfc; print('pyrfc OK')"
```

```python
# Notebook Databricks: Conexão SAP via RFC
import pyrfc
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Configuração de conexão (usar Databricks Secrets)
SAP_CONFIG = {
    'ashost': dbutils.secrets.get(scope='sap', key='app-server'),
    'sysnr':  dbutils.secrets.get(scope='sap', key='system-number'),
    'client': dbutils.secrets.get(scope='sap', key='client'),
    'user':   dbutils.secrets.get(scope='sap', key='username'),
    'passwd': dbutils.secrets.get(scope='sap', key='password'),
    'lang':   'PT'
}

def get_sap_connection():
    """Cria conexão RFC ao SAP"""
    return pyrfc.Connection(**SAP_CONFIG)
```

### 3.2 Extrator genérico via RFC_READ_TABLE

```python
def extract_sap_table(
    table_name: str,
    fields: list[str],
    where_clause: str = None,
    max_rows: int = 0,
    delimiter: str = '|'
) -> pd.DataFrame:
    """
    Extrai dados de qualquer tabela SAP via RFC_READ_TABLE.

    Args:
        table_name:   Nome da tabela SAP (ex: 'EKKO')
        fields:       Lista de campos a extrair
        where_clause: Condição WHERE (máx 72 chars por linha)
        max_rows:     0 = sem limite
        delimiter:    Separador dos campos retornados

    Returns:
        DataFrame pandas com os dados
    """
    with get_sap_connection() as conn:

        # Preparar campos
        field_list = [{'FIELDNAME': f} for f in fields]

        # Preparar WHERE (RFC_READ_TABLE limita 72 chars por linha)
        options = []
        if where_clause:
            # Quebrar em linhas de 72 chars
            for i in range(0, len(where_clause), 72):
                options.append({'TEXT': where_clause[i:i+72]})

        # Chamar RFC
        result = conn.call(
            'RFC_READ_TABLE',
            QUERY_TABLE=table_name,
            DELIMITER=delimiter,
            ROWCOUNT=max_rows,
            FIELDS=field_list,
            OPTIONS=options
        )

        # Processar resultado
        col_names = [f['FIELDNAME'] for f in result['FIELDS']]
        col_lengths = [int(f['LENGTH']) for f in result['FIELDS']]

        rows = []
        for data_row in result['DATA']:
            raw = data_row['WA']
            row = raw.split(delimiter)
            # Trim de cada campo
            row = [v.strip() for v in row]
            rows.append(dict(zip(col_names, row)))

        return pd.DataFrame(rows)


# Uso
df_ekko = extract_sap_table(
    table_name='EKKO',
    fields=['EBELN', 'LIFNR', 'EKORG', 'BEDAT', 'NETWR', 'WAERS'],
    where_clause="BEDAT >= '20240101' AND BSTYP = 'F'"
)

print(f"Registros extraídos: {len(df_ekko)}")
display(df_ekko.head(10))
```

### 3.3 Extrator via BAPI com pyrfc

```python
def get_po_details(po_number: str) -> dict:
    """Extrai detalhes de uma PO via BAPI"""
    with get_sap_connection() as conn:
        result = conn.call(
            'BAPI_PO_GETDETAIL',
            PURCHASEORDER=po_number,
            ITEMS='X',
            ACCOUNT='X',
            SCHEDULES='X'
        )
        return {
            'header':    result['POHEADER'],
            'items':     result['POITEM'],
            'schedules': result['POSCHEDULE'],
            'accounts':  result['POACCOUNT']
        }

def batch_extract_pos(po_list: list[str]) -> pd.DataFrame:
    """Extrai múltiplas POs em paralelo"""
    from concurrent.futures import ThreadPoolExecutor
    import json

    all_items = []

    def extract_one(po):
        try:
            details = get_po_details(po)
            for item in details['items']:
                item['EBELN'] = po
                all_items.append(item)
        except Exception as e:
            print(f"Erro ao extrair PO {po}: {e}")

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(extract_one, po_list)

    return pd.DataFrame(all_items)
```

---

## 4. Delta Lake com Dados SAP

### 4.1 Arquitetura Medallion para dados SAP

```python
# ============================================================
# CAMADA BRONZE: Ingestão bruta (schema aplicado, sem transformação)
# ============================================================

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import *

def ingest_to_bronze(df_raw: pd.DataFrame, table_name: str, date_partition: str):
    """Ingere dados SAP brutos na camada Bronze"""

    # Converter pandas → Spark
    df_spark = spark.createDataFrame(df_raw)

    # Adicionar metadados de ingestão
    df_with_meta = df_spark \
        .withColumn('_sap_table',       F.lit(table_name)) \
        .withColumn('_ingestion_ts',    F.current_timestamp()) \
        .withColumn('_source_system',   F.lit('SAP_ECC_PRD')) \
        .withColumn('_partition_date',  F.lit(date_partition))

    # Escrever em Delta com particionamento
    (df_with_meta.write
        .format('delta')
        .mode('append')
        .partitionBy('_partition_date')
        .option('mergeSchema', 'true')
        .save(f'abfss://bronze@datalake.dfs.core.windows.net/sap/{table_name.lower()}')
    )

    print(f"✅ Bronze: {len(df_raw)} registros gravados em sap/{table_name.lower()}")
```

```python
# ============================================================
# CAMADA SILVER: Limpeza, tipagem e MERGE (upsert)
# ============================================================

def process_to_silver_ekko():
    """Transforma EKKO Bronze → Silver com MERGE"""

    # Ler bronze
    df_bronze = spark.read.format('delta') \
        .load('abfss://bronze@datalake.dfs.core.windows.net/sap/ekko') \
        .where(F.col('_partition_date') == F.current_date().cast('string'))

    # Transformações Silver
    df_silver = df_bronze \
        .withColumn('EBELN',  F.col('EBELN').cast(StringType())) \
        .withColumn('LIFNR',  F.col('LIFNR').cast(StringType())) \
        .withColumn('BEDAT',  F.to_date(F.col('BEDAT'), 'yyyyMMdd')) \
        .withColumn('NETWR',  F.col('NETWR').cast(DecimalType(15, 2))) \
        .withColumn('WAERS',  F.col('WAERS').cast(StringType())) \
        .withColumn('BSTYP',  F.col('BSTYP').cast(StringType())) \
        .withColumn('_updated_at', F.current_timestamp()) \
        .filter(F.col('EBELN').isNotNull()) \
        .dropDuplicates(['EBELN'])

    silver_path = 'abfss://silver@datalake.dfs.core.windows.net/sap/purchase_order_header'

    # MERGE (upsert) para idempotência
    if DeltaTable.isDeltaTable(spark, silver_path):
        dt_silver = DeltaTable.forPath(spark, silver_path)

        dt_silver.alias('target').merge(
            df_silver.alias('source'),
            'target.EBELN = source.EBELN'
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        print(f"✅ Silver MERGE concluído")
    else:
        # Primeira carga: criar a tabela Delta
        df_silver.write \
            .format('delta') \
            .partitionBy('WAERS') \
            .save(silver_path)
        print(f"✅ Silver criada com {df_silver.count()} registros")
```

```python
# ============================================================
# CAMADA GOLD: Modelo dimensional para analytics
# ============================================================

def build_gold_po_analytics():
    """Constrói tabela Gold de análise de compras"""

    # Ler Silver
    df_po_header = spark.read.format('delta') \
        .load('abfss://silver@datalake.dfs.core.windows.net/sap/purchase_order_header')
    df_po_items  = spark.read.format('delta') \
        .load('abfss://silver@datalake.dfs.core.windows.net/sap/purchase_order_item')
    df_supplier  = spark.read.format('delta') \
        .load('abfss://silver@datalake.dfs.core.windows.net/sap/supplier_master')
    df_material  = spark.read.format('delta') \
        .load('abfss://silver@datalake.dfs.core.windows.net/sap/material_master')

    # Construir fato de compras
    df_gold = df_po_items \
        .join(df_po_header, 'EBELN', 'left') \
        .join(df_supplier,  'LIFNR', 'left') \
        .join(df_material,  'MATNR', 'left') \
        .select(
            # Chaves
            F.col('EBELN').alias('purchase_order'),
            F.col('EBELP').alias('purchase_order_item'),
            # Dimensões
            F.col('LIFNR').alias('supplier_id'),
            F.col('NAME1').alias('supplier_name'),
            F.col('MATNR').alias('material_id'),
            F.col('MAKTX').alias('material_description'),
            F.col('WERKS').alias('plant'),
            F.col('EKORG').alias('purchasing_org'),
            # Tempo
            F.col('BEDAT').alias('order_date'),
            F.year('BEDAT').alias('order_year'),
            F.month('BEDAT').alias('order_month'),
            # Medidas
            F.col('MENGE').alias('quantity_ordered'),
            F.col('MEINS').alias('unit_of_measure'),
            F.col('NETPR').alias('net_price'),
            F.col('NETWR').alias('net_value'),
            F.col('WAERS').alias('currency'),
            # Metadados
            F.current_timestamp().alias('_updated_at')
        )

    # Salvar Gold
    (df_gold.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .partitionBy('order_year', 'order_month')
        .save('abfss://gold@datalake.dfs.core.windows.net/sap/fact_purchase_orders')
    )

    print(f"✅ Gold: {df_gold.count()} registros na fact_purchase_orders")
```

---

## 5. Lakeflow Declarative Pipelines com Dados SAP

### 5.1 Pipeline declarativo completo SAP → Delta Lake

```python
# Notebook: SAP_P2P_Pipeline (Lakeflow / Delta Live Tables)
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ============================================================
# BRONZE: Ingestão de tabelas SAP (via ADF → ADLS)
# ============================================================

@dlt.table(
    name="bronze_sap_ekko",
    comment="Purchase Order Headers — raw from SAP ECC",
    table_properties={"quality": "bronze"}
)
def bronze_ekko():
    return (
        spark.readStream
            .format("cloudFiles")                          # Auto Loader
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.inferColumnTypes", "true")
            .load("abfss://landing@datalake.dfs.core.windows.net/sap/ekko/")
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_source", F.lit("SAP_ECC"))
    )

@dlt.table(
    name="bronze_sap_ekpo",
    comment="Purchase Order Items — raw from SAP ECC",
    table_properties={"quality": "bronze"}
)
def bronze_ekpo():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.inferColumnTypes", "true")
            .load("abfss://landing@datalake.dfs.core.windows.net/sap/ekpo/")
            .withColumn("_ingestion_ts", F.current_timestamp())
    )

# ============================================================
# SILVER: Limpeza, tipagem e qualidade
# ============================================================

@dlt.expect_or_drop("ebeln_not_null", "EBELN IS NOT NULL")
@dlt.expect_or_drop("valid_date",     "BEDAT >= '2000-01-01'")
@dlt.expect("valid_amount",           "NETWR >= 0")

@dlt.table(
    name="silver_purchase_order_header",
    comment="Purchase Orders — cleaned and typed",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)
def silver_po_header():
    return (
        dlt.read_stream("bronze_sap_ekko")
            .withColumn("EBELN",  F.col("EBELN").cast(StringType()))
            .withColumn("LIFNR",  F.col("LIFNR").cast(StringType()))
            .withColumn("BEDAT",  F.to_date(F.col("BEDAT"), "yyyyMMdd"))
            .withColumn("NETWR",  F.col("NETWR").cast(DecimalType(15, 2)))
            .withColumn("WAERS",  F.col("WAERS").cast(StringType()))
            .dropDuplicates(["EBELN"])
            .drop("_ingestion_ts")
    )

@dlt.expect_or_drop("ebeln_not_null", "EBELN IS NOT NULL")
@dlt.expect_or_drop("ebelp_not_null", "EBELP IS NOT NULL")
@dlt.expect("valid_quantity",         "MENGE >= 0")

@dlt.table(
    name="silver_purchase_order_item",
    comment="Purchase Order Items — cleaned and typed",
    table_properties={"quality": "silver"}
)
def silver_po_item():
    return (
        dlt.read_stream("bronze_sap_ekpo")
            .withColumn("EBELN",  F.col("EBELN").cast(StringType()))
            .withColumn("EBELP",  F.col("EBELP").cast(StringType()))
            .withColumn("MATNR",  F.col("MATNR").cast(StringType()))
            .withColumn("WERKS",  F.col("WERKS").cast(StringType()))
            .withColumn("MENGE",  F.col("MENGE").cast(DecimalType(13, 3)))
            .withColumn("NETPR",  F.col("NETPR").cast(DecimalType(11, 2)))
            .withColumn("NETWR",  F.col("NETWR").cast(DecimalType(13, 2)))
            .dropDuplicates(["EBELN", "EBELP"])
    )

# ============================================================
# GOLD: Modelo analítico para P2P
# ============================================================

@dlt.table(
    name="gold_fact_purchase_orders",
    comment="Fact table: Purchase Orders P2P — ready for analytics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "order_date,plant"
    }
)
def gold_fact_po():
    df_header = dlt.read("silver_purchase_order_header")
    df_items  = dlt.read("silver_purchase_order_item")

    return (
        df_items.alias("i")
            .join(df_header.alias("h"), "EBELN", "left")
            .select(
                F.col("i.EBELN").alias("purchase_order"),
                F.col("i.EBELP").alias("line_item"),
                F.col("h.LIFNR").alias("supplier_id"),
                F.col("h.EKORG").alias("purchasing_org"),
                F.col("h.BUKRS").alias("company_code"),
                F.col("i.MATNR").alias("material_id"),
                F.col("i.WERKS").alias("plant"),
                F.col("h.BEDAT").alias("order_date"),
                F.year("h.BEDAT").alias("order_year"),
                F.month("h.BEDAT").alias("order_month"),
                F.col("i.MENGE").alias("quantity"),
                F.col("i.MEINS").alias("uom"),
                F.col("i.NETPR").alias("unit_price"),
                F.col("i.NETWR").alias("net_value"),
                F.col("h.WAERS").alias("currency"),
                F.current_timestamp().alias("_pipeline_ts")
            )
    )
```

---

## 6. Monitoramento e Qualidade de Dados SAP

### 6.1 Validações de qualidade específicas para dados SAP

```python
# Notebook: SAP Data Quality Checks

from pyspark.sql import functions as F

def check_sap_data_quality(df, table_name: str) -> dict:
    """Executa checks de qualidade em dados SAP"""

    results = {}
    total = df.count()

    # 1. Integridade de MANDT
    mandts = df.select('MANDT').distinct().collect()
    results['mandantes_distintos'] = [r['MANDT'] for r in mandts]
    if len(mandts) > 1:
        print(f"⚠️ ALERTA: Múltiplos mandantes detectados: {results['mandantes_distintos']}")

    # 2. Completude de chaves primárias
    pk_nulls = df.filter(F.col('EBELN').isNull()).count()
    results['chave_nula_pct'] = round(pk_nulls / total * 100, 2)
    if pk_nulls > 0:
        print(f"❌ {pk_nulls} registros sem chave primária ({results['chave_nula_pct']}%)")

    # 3. Valores negativos onde não deveriam existir
    neg_values = df.filter(F.col('NETWR') < 0).count()
    results['valores_negativos'] = neg_values
    if neg_values > 0:
        print(f"⚠️ {neg_values} registros com NETWR negativo")

    # 4. Datas fora do range esperado
    future_dates = df.filter(F.col('BEDAT') > F.current_date()).count()
    old_dates = df.filter(F.col('BEDAT') < F.lit('2000-01-01').cast('date')).count()
    results['datas_futuras']  = future_dates
    results['datas_antigas']  = old_dates

    # 5. Score geral de qualidade
    issues = pk_nulls + neg_values + future_dates + old_dates
    results['quality_score'] = round((1 - issues / total) * 100, 1)

    print(f"\n📊 Qualidade {table_name}: {results['quality_score']}% ({total} registros)")
    return results
```

### 6.2 Reconciliação SAP vs. Data Lake

```python
def reconcile_sap_vs_lake(
    sap_table: str,
    lake_path: str,
    key_field: str,
    value_field: str
) -> None:
    """Compara totais entre SAP e Delta Lake para validar ingestão"""

    # Total no Delta Lake
    df_lake = spark.read.format('delta').load(lake_path)
    lake_count = df_lake.count()
    lake_sum   = df_lake.agg(F.sum(value_field)).collect()[0][0]

    # Total no SAP via RFC
    df_sap = extract_sap_table(
        table_name=sap_table,
        fields=[key_field, value_field],
        where_clause=f"MANDT = '{CLIENT}'"
    )
    sap_count = len(df_sap)
    sap_sum   = df_sap[value_field].astype(float).sum()

    # Comparar
    delta_count = abs(sap_count - lake_count)
    delta_value = abs(sap_sum   - lake_sum)

    print(f"\n🔍 Reconciliação: {sap_table}")
    print(f"   SAP:  {sap_count:,} registros | Soma: {sap_sum:,.2f}")
    print(f"   Lake: {lake_count:,} registros | Soma: {lake_sum:,.2f}")
    print(f"   Δ Count: {delta_count} | Δ Value: {delta_value:.2f}")

    if delta_count > 0 or delta_value > 0.01:
        raise Exception(f"❌ Divergência detectada em {sap_table}!")
    else:
        print(f"   ✅ Dados consistentes")
```

---

## 7. Padrões de Governança para Dados SAP no Azure

### 7.1 Unity Catalog — Estrutura recomendada

```sql
-- Estrutura Unity Catalog para dados SAP
CREATE CATALOG IF NOT EXISTS sap_data;

CREATE SCHEMA IF NOT EXISTS sap_data.bronze;
CREATE SCHEMA IF NOT EXISTS sap_data.silver;
CREATE SCHEMA IF NOT EXISTS sap_data.gold;

-- Tabela com Column Masking para campos sensíveis
CREATE TABLE sap_data.silver.supplier_master
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
AS SELECT * FROM ...;

-- Masking de CNPJ/dados fiscais do fornecedor
CREATE OR REPLACE FUNCTION sap_data.silver.mask_cnpj(cnpj STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('sap_financial_team') THEN cnpj
  ELSE CONCAT(SUBSTR(cnpj, 1, 2), '.***.***/***-**')
END;

ALTER TABLE sap_data.silver.supplier_master
ALTER COLUMN STCD1                               -- Campo CNPJ no SAP
SET MASK sap_data.silver.mask_cnpj;
```

### 7.2 Lineage — Rastreabilidade ponta a ponta

```
SAP Table (EKKO)
    ↓ [ADF Copy Activity]
ADLS bronze/sap/ekko/*.parquet
    ↓ [Lakeflow Auto Loader]
sap_data.bronze.bronze_sap_ekko
    ↓ [Lakeflow Pipeline]
sap_data.silver.silver_purchase_order_header
    ↓ [Lakeflow Pipeline]
sap_data.gold.gold_fact_purchase_orders
    ↓ [Power BI DirectQuery / Synapse]
Dashboard de Compras
```

O Unity Catalog captura este lineage automaticamente — navegável via **Catalog Explorer** no Databricks.

---

## ✅ Checklist de Aprendizado — Módulo 8

- [ ] Entender a arquitetura de referência SAP → SHIR → ADF → ADLS → Databricks
- [ ] Configurar um Linked Service SAP Table com Key Vault para credenciais
- [ ] Criar pipeline ADF com watermark incremental para tabelas SAP
- [ ] Usar pyrfc para extração direta de tabelas e BAPIs via RFC
- [ ] Implementar as três camadas Medallion (Bronze/Silver/Gold) com Delta Lake
- [ ] Criar um pipeline Lakeflow com qualidade de dados declarativa
- [ ] Escrever MERGE (upsert) em Delta Lake para garantir idempotência
- [ ] Implementar reconciliação SAP vs. Data Lake para validação de ingestão
- [ ] Configurar Unity Catalog com column masking para campos sensíveis SAP
- [ ] Entender lineage de dados ponta a ponta no Unity Catalog

---

## 🎓 Conclusão do Programa Completo — SAP Zero to Hero (Extended)

Parabéns! Você completou todos os 8 módulos do programa:

| # | Módulo | Competência |
|---|--------|-------------|
| 1 | Básico | Estrutura SAP, mandante, dados mestres e transacionais |
| 2 | Intermediário | P2P, O2C, FI/CO, PP — fluxo de documentos e tabelas |
| 3 | Avançado | Change Docs, IDocs, BAPIs, BW, ODP, extração avançada |
| 4 | BDC | SHDB, BDCDATA, CALL TRANSACTION, SESSION METHOD |
| 5 | ABAP para DE | SELECT avançado, internal tables, performance tuning |
| 6 | S/4HANA | ACDOCA, CDS Views, AMDP, Virtual Data Models |
| 7 | BTP & OData | APIs OData, Integration Suite, delta tokens, OAuth |
| 8 | Azure Integration | ADF, pyrfc, Delta Lake, Lakeflow, Unity Catalog |

### Stack completa dominada

```
SAP (origem)
  └── Tabelas, BAPIs, OData, IDocs, CDS Views

Extração
  └── ADF (SAP Table / ODP Connector) + pyrfc (Python direto)

Armazenamento
  └── ADLS Gen2 + Delta Lake (formato)

Transformação
  └── Databricks (PySpark) + Lakeflow Declarative Pipelines

Governança
  └── Unity Catalog (lineage, masking, RBAC)

Consumo analítico
  └── Power BI | Synapse | SAP Analytics Cloud
```

---

*Documento gerado para uso interno — Programa de Capacitação SAP*
*Versão 1.0 | Módulo 8 de 8 — Programa Completo*
