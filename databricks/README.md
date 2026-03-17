# Databricks — Pipeline SAP Medallion

Este diretório contém todos os artefatos do pipeline de dados SAP na arquitetura **Medallion** (Bronze → Silver → Gold), implantado no Databricks usando **Delta Live Tables (DLT)** e gerenciado pelo **Databricks Asset Bundles (DAB)**.

---

## Estrutura de Diretórios

```
databricks/
├── databricks.yml                        # Bundle DAB: variáveis, targets dev/prd
├── resources/
│   └── pipeline_sap_medallion.yml        # Definição do pipeline DLT
└── src/
    ├── pipelines/
    │   ├── bronze_pipeline.sql           # Camada Bronze: ingestão via Auto Loader
    │   ├── silver_pipeline.sql           # Camada Silver: tipagem e modelos de domínio
    │   └── gold_pipeline.sql             # Camada Gold: KPIs e relatórios
    └── schemas/
        ├── silver_schemas.py             # Referência: colunas de todas as tabelas Silver
        └── gold_schemas.py               # Referência: colunas de todos os KPIs Gold
```

---

## `databricks.yml` — Configuração do Bundle

O arquivo raiz do **Databricks Asset Bundle**. Define o nome do projeto, inclui os recursos e declara variáveis configuráveis por ambiente.

### Variáveis

| Variável | Padrão (dev) | Padrão (prd) | Descrição |
|----------|-------------|-------------|-----------|
| `catalog` | `workspace` | `prd` | Unity Catalog de destino |
| `bronze_schema` | `sap_raw` | `sap_raw` | Schema da camada Bronze |
| `silver_schema` | `sap_silver` | `sap_silver` | Schema da camada Silver |
| `gold_schema` | `sap_gold` | `sap_gold` | Schema da camada Gold |
| `source_base_path` | `/Volumes/workspace/sap_raw/sap-files/files/` | `/Volumes/prd/sap/raw` | Caminho raiz dos arquivos JSONL |
| `schema_location` | `/Volumes/workspace/sap_raw/_autoloader_checkpoints` | `/Volumes/prd/sap/_autoloader_checkpoints` | Checkpoints do Auto Loader |

### Targets (Ambientes)

```yaml
targets:
  dev:   # padrão; mode: development
  prd:   # mode: production
```

Para sobrescrever uma variável na linha de comando:

```bash
databricks bundle deploy --var="catalog=meu_catalog" --var="gold_schema=sap_gold_v2"
```

---

## `resources/pipeline_sap_medallion.yml` — Pipeline DLT

Define o pipeline **Delta Live Tables** chamado `SAP Medallion - Bronze / Silver / Gold`.

### Configuração do Pipeline

| Propriedade | Valor | Descrição |
|-------------|-------|-----------|
| `catalog` | `${var.catalog}` | Unity Catalog de destino |
| `schema` | `${var.bronze_schema}` | Schema padrão (tabelas sem prefixo explícito vão aqui) |
| `channel` | `CURRENT` | Canal de runtime DLT |
| `continuous` | `false` | Pipeline triggered (não streaming contínuo) |
| `serverless` | `true` | Executa em compute serverless do Databricks |

### Ordem das Libraries

O DLT resolve o grafo de dependências automaticamente, mas a ordem explicita a intenção:

```
1. bronze_pipeline.sql   →  Ingestão (Auto Loader)
2. silver_pipeline.sql   →  Transformação e tipagem
3. gold_pipeline.sql     →  Agregações e KPIs
```

### Variáveis de Configuração SQL

As variáveis a seguir são injetadas no contexto SQL dos pipelines via `configuration:`:

| Variável SQL | Referenciada como | Uso |
|-------------|-------------------|-----|
| `catalog` | `${catalog}` | Prefixo de três partes nas tabelas Silver/Gold |
| `silver_schema` | `${silver_schema}` | Schema nas tabelas Silver |
| `gold_schema` | `${gold_schema}` | Schema nas tabelas Gold |
| `source_base_path` | `${source_base_path}` | Caminho dos arquivos no Auto Loader |
| `schema_location` | `${schema_location}` | Diretório de checkpoints do Auto Loader |

> **Atenção:** `schema: ${var.bronze_schema}` define apenas o schema padrão do pipeline DLT — ele **não** cria uma variável SQL `${bronze_schema}` acessível nos arquivos SQL. Para referenciar tabelas Bronze dentro do mesmo pipeline, use a sintaxe `LIVE.<nome_da_tabela>`.

---

## `src/pipelines/` — Arquivos SQL do Pipeline

### `bronze_pipeline.sql` — Camada Bronze

Responsável pela **ingestão incremental** dos arquivos JSONL gerados pelo ShadowTraffic.

- Usa **Auto Loader** (`cloud_files`) com `format = 'json'`
- Cria tabelas do tipo `STREAMING TABLE` no schema Bronze (`sap_raw`)
- Nomes de tabelas: `bronze_<tabela_sap>` (ex: `bronze_vbak`, `bronze_kna1`)
- Colunas em nomes originais SAP (ex: `vbeln`, `matnr`, `kunnr`)
- Adiciona coluna `_rescued_data` para capturar campos não mapeados
- Nenhuma transformação de tipo — dados chegam como `STRING`

**Tabelas Bronze criadas** (uma por tabela SAP):
`bronze_kna1`, `bronze_lfa1`, `bronze_mara`, `bronze_makt`, `bronze_pa0001`, `bronze_t001w`, `bronze_knmt`, `bronze_vbak`, `bronze_vbap`, `bronze_vbep`, `bronze_vbfa`, `bronze_vbrk`, `bronze_vbrp`, `bronze_likp`, `bronze_lips`, `bronze_vttk`, `bronze_ekko`, `bronze_ekpo`, `bronze_eket`, `bronze_ekbe`, `bronze_j_1bnfdoc`, `bronze_j_1bnflin`, `bronze_j_1bnfstx`, `bronze_bkpf`, `bronze_dd07t`, `bronze_dd07v`, `bronze_t173t`, `bronze_tvagt`, `bronze_tvaut`, `bronze_tvkggt`, `bronze_tvrot`, `bronze_tvsbt`, `bronze_tvstt`, `bronze_konv`, `bronze_vbkd`, `bronze_vbpa`, `bronze_vbuk`, `bronze_vbup`, `bronze_vttp`, `bronze_oigs`, `bronze_oigsi`

### `silver_pipeline.sql` — Camada Silver

Responsável pela **transformação, tipagem e modelagem de domínio**.

Possui dois tipos de objetos:

#### L1 — Streaming Tables (18 tabelas)
- Leem de `LIVE.bronze_*`
- Aplicam `CAST` para tipos corretos (datas, decimais, inteiros)
- Renomeiam colunas para nomes de negócio em português
- Filtram `WHERE mandt = '100'`
- Nomes: `silver_<domínio>` (ex: `silver_sales_orders`, `silver_materials`)

#### L2 — Materialized Views (8 views)
- Leem de `LIVE.silver_*`
- Cruzam múltiplas tabelas Silver (joins de domínio)
- Expõem modelos prontos para consumo analítico
- Exemplo: `silver_order_delivery_chain` — une OV, remessa e fatura

### `gold_pipeline.sql` — Camada Gold

Responsável pelos **KPIs de negócio e relatórios analíticos**.

Todos os objetos são `MATERIALIZED VIEW` criadas em `${catalog}.${gold_schema}`.

| KPI | Tabela Gold | Descrição |
|-----|-------------|-----------|
| 1 | `kpi_sales_overview` | Visão geral de vendas por OV |
| 2 | `kpi_delivery_performance` | Performance de entregas |
| 3 | `kpi_billing_summary` | Resumo de faturamento |
| 4 | `kpi_purchase_orders` | Ordens de compra MM |
| 5 | `zf1rsd003_pedidos_abertos_faturados` | Replica do relatório ZF1RSD003: pedidos em aberto e faturados (34 colunas ALV, grain: vbeln + posnr) |

> **KPI 5 — ZF1RSD003:** Esta view reproduz o relatório ABAP `ZF1RSD003` ("Pedidos em Aberto e Faturados") usando 27 CTEs que navegam toda a cadeia documental SAP: VBAK → VBAP → VBEP → VBKD → VBPA → LIPS → LIKP → VTTP → VTTK → VBRP → VBRK → J_1BNFDOC → J_1BNFLIN → KNA1 → LFA1 → MARA → TVSBT → OIGSI.

---

## `src/schemas/` — Arquivos de Referência

Estes arquivos **não são executados pelo pipeline**. São referências Python que documentam o esquema esperado de cada camada.

### `silver_schemas.py`

Lista todas as colunas de cada tabela Silver L1 e L2. Útil para:
- Verificar quais colunas estão disponíveis antes de escrever uma query Gold
- Documentar o contrato de interface entre Silver e Gold
- Revisar renomeações de colunas (nomes SAP → nomes de negócio)

### `gold_schemas.py`

Lista todas as colunas de cada KPI Gold (KPIs 1–4). Útil para:
- Documentar o schema de saída de cada Materialized View Gold
- Orientar equipes de BI que consomem as tabelas Gold

---

## Como Implantar

### Pré-requisitos

1. Databricks CLI instalado: `pip install databricks-cli` ou `brew install databricks`
2. Autenticado no workspace: `databricks configure`
3. Arquivos de dados carregados no Volume (veja abaixo)

### Carregar os Dados

Antes de executar o pipeline, carregue os arquivos JSONL do `datasets/generator/sample_data/` no Volume do Databricks:

```bash
# Estrutura esperada no Volume:
# /Volumes/workspace/sap_raw/sap-files/files/
#   kna1/customers-0.jsonl
#   vbak/orders-0.jsonl
#   ...

databricks fs cp -r datasets/generator/sample_data/ \
  dbfs:/Volumes/workspace/sap_raw/sap-files/files/
```

### Deploy (Ambiente de Desenvolvimento)

```bash
cd databricks
databricks bundle deploy          # usa target "dev" por padrão
databricks bundle run sap_medallion  # executa o pipeline
```

### Deploy (Produção)

```bash
cd databricks
databricks bundle deploy --target prd
databricks bundle run sap_medallion --target prd
```

### Verificar o Pipeline

Após o deploy, o pipeline aparece na UI do Databricks em:
**Workflows → Delta Live Tables → SAP Medallion - Bronze / Silver / Gold [dev]**

---

## Arquitetura Medallion — Fluxo de Dados

```
Azure Blob Storage (JSONL)
        │
        ▼  Auto Loader (cloud_files)
┌──────────────────┐
│   BRONZE LAYER   │  bronze.sap_raw.*
│  STREAMING TABLE │  Nomes SAP originais, tudo STRING
│  (41 tabelas)    │  Sem filtros, sem transforms
└──────────────────┘
        │  LIVE.<bronze_table>
        ▼  CAST + RENAME + WHERE mandt='100'
┌──────────────────┐
│   SILVER LAYER   │  workspace.sap_silver.*
│  STREAMING TABLE │  L1: tipagem e nomes de negócio
│  + MAT. VIEW     │  L2: joins de domínio (order chain)
│  (18 + 8)        │
└──────────────────┘
        │  LIVE.<silver_table>
        ▼  CTEs + JOINs + Aggregations
┌──────────────────┐
│    GOLD LAYER    │  workspace.sap_gold.*
│  MAT. VIEW (5)   │  KPIs de negócio e relatórios ALV
│                  │  Consumido por BI / Power BI / SQL
└──────────────────┘
```

---

## Padrões de SQL nos Pipelines

### Referências Entre Tabelas do Mesmo Pipeline

Dentro do pipeline DLT, **sempre use `LIVE.<nome>`** para referenciar outras tabelas:

```sql
-- Correto: referência intra-pipeline
SELECT * FROM LIVE.bronze_vbak WHERE mandt = '100'

-- Incorreto: não usar caminho de três partes para bronze dentro do mesmo pipeline
-- SELECT * FROM workspace.sap_raw.bronze_vbak  -- causará erro de variável não resolvida
```

### Datas SAP (DATS)

SAP armazena datas como string `YYYYMMDD`. Conversão para tipo `DATE`:

```sql
CASE WHEN erdat NOT IN ('00000000','99991231') THEN TO_DATE(erdat, 'yyyyMMdd') END AS data_criacao
```

### NULL vs String Vazia (ABAP)

ABAP armazena campos char vazios como `''`. No Delta Lake, podem chegar como `NULL`:

```sql
WHERE (col IS NULL OR col = '')    -- equivale ao ABAP: col = ''
WHERE (col IS NULL OR col != 'X')  -- equivale ao ABAP: col <> 'X'
```

### MANDT Filter

Sempre filtre `WHERE mandt = '100'` nas leituras de Bronze — todo o dataset sintético usa mandante 100.
