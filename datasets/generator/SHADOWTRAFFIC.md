# ShadowTraffic — Geração de Dados Sintéticos SAP

Este documento explica como o dataset sintético SAP é gerado usando o **ShadowTraffic**, uma ferramenta declarativa de geração de dados. O arquivo de configuração principal para o ambiente Azure é `schemas/sap-synthetic-data-azure.json`.

---

## O que é o ShadowTraffic?

[ShadowTraffic](https://shadowtraffic.io) é uma ferramenta de geração de dados sintéticos baseada em configuração JSON. Em vez de escrever código, você declara:

- **O que** gerar (campos e seus tipos)
- **Quanto** gerar (`maxEvents` para execução local)
- **Para onde** enviar (arquivo local, Azure Blob Storage, Kafka, etc.)
- **Como manter integridade referencial** (via `lookup`)

O ShadowTraffic lê a configuração e emite registros em formato **JSONL** (JSON Lines — um objeto JSON por linha).

---

## Como Executar

### Modo Local (desenvolvimento)

Usa o arquivo `sap-synthetic-data-local.json`. Os dados são gravados em subdiretórios dentro de `/tmp/sap-synthetic/`.

```bash
shadowtraffic run --config datasets/generator/schemas/sap-synthetic-data-local.json
```

Exemplo de saída:
```
/tmp/sap-synthetic/kna1/customers-0.jsonl
/tmp/sap-synthetic/vbak/orders-0.jsonl
...
```

### Modo Azure (produção)

Usa o arquivo `sap-synthetic-data-azure.json`. Os dados são gravados no Azure Blob Storage, container `sap`, com prefixos de chave por tabela.

```bash
shadowtraffic run --config datasets/generator/schemas/sap-synthetic-data-azure.json
```

**Pré-requisito:** edite a seção `connections.azure.connectionConfigs.connectionString` no JSON com sua connection string real do Azure Storage.

Exemplo de saída no Blob Storage:
```
Container: sap
  kna1/customers-0.jsonl
  vbak/orders-0.jsonl
  vbrk/billing-header-0.jsonl
  ...
```

---

## Estrutura da Configuração (`sap-synthetic-data-azure.json`)

O arquivo tem três seções principais:

```json
{
  "generators": [ ... ],   // Array de geradores — um por tabela SAP
  "connections": { ... }   // Configuração de destino (Azure Blob Storage)
}
```

### Estrutura de um Gerador

Cada gerador define uma tabela:

```json
{
  "container": "sap",                        // Container Azure (sempre "sap")
  "containerConfigs": {
    "format": "jsonl",                       // Formato de saída
    "keyPrefix": "vbak/orders-"             // Prefixo do blob (tabela/nome-)
  },
  "localConfigs": { "maxEvents": 5000 },    // Limite para execução local
  "data": {
    "campo": { "_gen": "...", ... }         // Definição de cada coluna
  }
}
```

---

## Primitivos de Geração (`_gen`)

O ShadowTraffic usa o campo `_gen` para declarar como um valor é gerado.

| `_gen`                | Descrição | Exemplo |
|-----------------------|-----------|---------|
| `constant`            | Valor fixo | `{ "_gen": "constant", "x": "100" }` |
| `digitString`         | String de dígitos numéricos com `n` caracteres | `{ "_gen": "digitString", "n": 10 }` |
| `oneOf`               | Sorteia aleatoriamente de uma lista | `{ "_gen": "oneOf", "choices": ["SP","BA","RS"] }` |
| `weightedOneOf`       | Sorteia com pesos de probabilidade | ver abaixo |
| `cycle`               | Itera em sequência, repetindo | `{ "_gen": "cycle", "sequence": ["000010","000020"] }` |
| `uniformDistribution` | Número aleatório uniforme entre dois limites | `{ "_gen": "uniformDistribution", "bounds": [1, 9999], "decimals": 3 }` |
| `normalDistribution`  | Número aleatório com distribuição normal | `{ "_gen": "normalDistribution", "mean": 15000, "sd": 8000, "decimals": 2 }` |
| `formatDateTime`      | Data/hora formatada | `{ "_gen": "formatDateTime", "ms": { "_gen": "now" }, "format": "yyyyMMdd" }` |
| `now`                 | Timestamp atual em milissegundos | `{ "_gen": "now" }` |
| `string`              | Texto usando expressões Faker | `{ "_gen": "string", "expr": "#{Company.name}" }` |
| `lookup`              | Busca valor de outro gerador já executado | ver seção abaixo |

### `weightedOneOf` — Probabilidades Controladas

Permite simular distribuições realistas, como campos opcionais:

```json
"sfakn": {
  "_gen": "weightedOneOf",
  "choices": [
    { "weight": 90, "value": null },
    { "weight": 10, "value": { "_gen": "digitString", "n": 10 } }
  ]
}
```

Neste exemplo, `sfakn` (nota fiscal substituta em VBRK) é `null` em 90% dos casos e preenchida em 10% — refletindo o comportamento real do SAP.

### `lookup` — Integridade Referencial

O `lookup` é o mecanismo central para garantir que chaves estrangeiras sejam válidas. Ele lê um valor já gerado por outro gerador:

```json
"vbeln": {
  "_gen": "lookup",
  "container": "sap",
  "keyPrefix": "vbak/orders-",
  "path": ["data", "vbeln"]
}
```

Isso garante que `vbap.vbeln` sempre referencie um `vbak.vbeln` existente — equivalente a uma foreign key.

**Importante:** o gerador que é referenciado pelo `lookup` **deve aparecer antes** no array `generators`. A ordem dos geradores define a ordem de execução.

---

## Cadeia de Dependências das Tabelas

A ordem dos geradores no arquivo segue a cadeia de dependências SAP. Tabelas "folha" (sem dependências) vêm primeiro.

```
Dados Mestre (sem dependências):
  kna1 (1.000)  →  clientes
  lfa1 (200)    →  fornecedores / transportadoras
  mara (500)    →  materiais
  pa0001 (100)  →  funcionários
  t001w (20)    →  plantas

Textos e Configuração:
  makt (500)    →  textos de material       [lookup: mara.matnr]
  tvsbt (10)    →  condições de expedição
  t173t (8)     →  tipos de transporte
  tvrot (10)    →  rotas
  tvstt (6)     →  pontos de expedição
  tvaut (10)    →  motivos de pedido
  tvagt (15)    →  motivos de rejeição
  tvkggt (20)   →  grupos de clientes
  dd07t (20)    →  textos de domínio SAP
  dd07v (30)    →  valores de domínio SAP

Cadeia SD (Sales & Distribution):
  vbak (5.000)  →  cabeçalho OV             [lookup: kna1.kunnr]
  vbap (15.000) →  itens OV                 [lookup: vbak.vbeln, mara.matnr]
  vbep (15.000) →  linhas de programação    [lookup: vbap.vbeln/posnr]
  vbkd (5.000)  →  dados comerciais         [lookup: vbak.vbeln]
  vbpa (10.000) →  parceiros OV             [lookup: vbap.vbeln, kna1.kunnr, pa0001.pernr]
  vbuk (5.000)  →  status cabeçalho         [lookup: vbak.vbeln]
  knmt (2.000)  →  material por cliente     [lookup: kna1.kunnr, mara.matnr]
  konv (30.000) →  condições de preço       [lookup: vbak.knumv]

Remessa (Delivery):
  lips (15.000) →  itens de remessa         [lookup: vbap.vbeln/posnr/matnr]
  likp (5.000)  →  cabeçalho remessa        [lookup: lips.vbeln, lfa1.lifnr]
  vbup (15.000) →  status itens             [lookup: lips.vbeln/posnr]
  oigsi (3.000) →  integração embarque      [lookup: likp.vbeln]
  oigs (1.500)  →  cabeçalho embarque OT    [lookup: likp.vbeln]

Transporte:
  vttp (5.000)  →  itens de transporte      [lookup: likp.vbeln]
  vttk (2.000)  →  cabeçalho transporte     [lookup: vttp.tknum, lfa1.lifnr]

Faturamento (Billing):
  vbrp (12.000) →  itens de fatura          [lookup: lips.vbeln/posnr, vbap.vbeln/posnr]
  vbrk (4.000)  →  cabeçalho fatura         [lookup: vbrp.vbeln, vbak.knumv]

Nota Fiscal (Brasil):
  j_1bnfdoc (4.000)  →  cabeçalho NF-e      [lookup: vbrk.vbeln]
  j_1bnflin (12.000) →  itens NF-e          [lookup: j_1bnfdoc.docnum, mara.matnr]
  j_1bnfstx (36.000) →  impostos NF-e       [lookup: j_1bnflin.docnum/itmnum]

Contabilidade (FI):
  bkpf (4.000)  →  documento contábil       [lookup: vbrk.vbeln]

Compras (MM):
  ekko (500)    →  cabeçalho pedido compra  [lookup: lfa1.lifnr]
  ekpo (2.000)  →  itens pedido compra      [lookup: ekko.ebeln, mara.matnr]
  eket (2.500)  →  programação pedido       [lookup: ekpo.ebeln/ebelp]
  ekbe (3.000)  →  histórico pedido         [lookup: ekpo.ebeln/ebelp]

Fluxo de Documentos:
  vbfa (20.000) →  fluxo de docs SD         [lookup: vbak.vbeln, vbap.posnr]
```

**Total aproximado de eventos gerados:** ~190.000 registros distribuídos em 35 tabelas.

---

## Convenções do Dataset

Todas as tabelas seguem estas convenções fixas:

| Campo | Valor | Significado |
|-------|-------|-------------|
| `mandt` | `"100"` | Mandante SAP (sempre usar `WHERE mandt = '100'` nas queries) |
| `land1` | `"BR"` | País Brasil |
| `spras` / `ddlanguage` | `"P"` | Idioma Português |
| `waerk` / `waers` | `"BRL"` | Moeda Real Brasileiro |
| Datas (DATS) | `"yyyyMMdd"` | Ex: `"20250317"` |
| Unidades org. | Prefixo `BR` | Ex: `BR01`, `BR10`, `BR51` |
| `posnr` | `"000010"`, `"000020"`, ... | Numeração de itens SAP |

---

## Configuração de Conexão Azure

A seção `connections` define o destino dos dados:

```json
"connections": {
  "azure": {
    "kind": "azureBlobStorage",
    "connectionConfigs": {
      "connectionString": "DefaultEndpointsProtocol=https;AccountName=YOURSTORAGE;AccountKey=YOURKEY==;EndpointSuffix=core.windows.net"
    },
    "batchConfigs": {
      "lingerMs": 2000,        // Aguarda 2s antes de fechar um batch
      "batchElements": 10000,  // Máximo de registros por arquivo blob
      "batchBytes": 5242880    // Máximo de bytes por arquivo (5 MB)
    }
  }
}
```

**Para usar:** substitua `YOURSTORAGE` e `YOURKEY==` pelas credenciais reais do seu Azure Storage Account.

---

## Diferença: Local vs Azure

| Aspecto | Local (`sap-synthetic-data-local.json`) | Azure (`sap-synthetic-data-azure.json`) |
|---------|----------------------------------------|-----------------------------------------|
| Destino | Sistema de arquivos local | Azure Blob Storage |
| Config de saída | `"connection": "local"`, `"fileName": "..."` | `"container": "sap"`, `"keyPrefix": "tabela/nome-"` |
| Limite de eventos | `localConfigs.maxEvents` (bounded) | Roda até ser interrompido (unbounded) |
| Uso | Desenvolvimento e testes locais | Carga no Databricks via Auto Loader |

---

## Sample Data (Dados Pré-Gerados)

O diretório `datasets/generator/sample_data/` contém arquivos JSONL pré-gerados organizados por tabela SAP:

```
sample_data/
  kna1/       → customers-0.jsonl
  vbak/       → orders-0.jsonl
  vbap/       → order-items-0.jsonl
  ...
```

Estes arquivos podem ser carregados diretamente no pipeline Databricks sem necessidade de executar o ShadowTraffic. Veja `databricks/README.md` para instruções de carregamento.

---

## Tabelas Geradas — Resumo Rápido

| Tabela SAP | Key Prefix Azure | Eventos | Descrição |
|------------|-----------------|---------|-----------|
| `KNA1` | `kna1/customers-` | 1.000 | Clientes |
| `LFA1` | `lfa1/vendors-` | 200 | Fornecedores |
| `MARA` | `mara/materials-` | 500 | Materiais |
| `MAKT` | `makt/material-texts-` | 500 | Textos de materiais |
| `PA0001` | `pa0001/employees-` | 100 | Funcionários |
| `T001W` | `t001w/plants-` | 20 | Plantas |
| `KNMT` | `knmt/customer-material-` | 2.000 | Material info por cliente |
| `VBAK` | `vbak/orders-` | 5.000 | Cabeçalho OV |
| `VBAP` | `vbap/order-items-` | 15.000 | Itens OV |
| `VBEP` | `vbep/schedule-lines-` | 15.000 | Linhas de programação |
| `VBKD` | `vbkd/commercial-` | 5.000 | Dados comerciais |
| `VBPA` | `vbpa/partners-` | 10.000 | Parceiros |
| `VBUK` | `vbuk/order-status-` | 5.000 | Status cabeçalho OV |
| `VBUP` | `vbup/item-status-` | 15.000 | Status item OV |
| `VBFA` | `vbfa/doc-flow-` | 20.000 | Fluxo de documentos |
| `KONV` | `konv/pricing-` | 30.000 | Condições de preço |
| `LIPS` | `lips/delivery-items-` | 15.000 | Itens de remessa |
| `LIKP` | `likp/delivery-header-` | 5.000 | Cabeçalho remessa |
| `VTTP` | `vttp/transport-items-` | 5.000 | Itens de transporte |
| `VTTK` | `vttk/transport-header-` | 2.000 | Cabeçalho transporte |
| `OIGSI` | `oigsi/shipments-` | 3.000 | Integração embarque (TB_OIGSI) |
| `OIGS` | `oigs/ot-header-` | 1.500 | Cabeçalho embarque OT |
| `VBRP` | `vbrp/invoice-items-` | 12.000 | Itens de fatura |
| `VBRK` | `vbrk/billing-header-` | 4.000 | Cabeçalho fatura |
| `J_1BNFDOC` | `j_1bnfdoc/nf-header-` | 4.000 | Cabeçalho NF-e |
| `J_1BNFLIN` | `j_1bnflin/nf-items-` | 12.000 | Itens NF-e |
| `J_1BNFSTX` | `j_1bnfstx/nf-taxes-` | 36.000 | Impostos NF-e |
| `BKPF` | `bkpf/accounting-` | 4.000 | Documento contábil |
| `EKKO` | `ekko/po-header-` | 500 | Cabeçalho pedido compra |
| `EKPO` | `ekpo/po-items-` | 2.000 | Itens pedido compra |
| `EKET` | `eket/po-schedule-` | 2.500 | Programação pedido compra |
| `EKBE` | `ekbe/po-history-` | 3.000 | Histórico pedido compra |
| `TVSBT` | `tvsbt/shipping-conditions-` | 10 | Condições de expedição |
| `T173T` | `t173t/shipping-types-` | 8 | Tipos de transporte |
| `TVROT` | `tvrot/routes-` | 10 | Rotas |
| `TVSTT` | `tvstt/shipping-points-` | 6 | Pontos de expedição |
| `TVAUT` | `tvaut/order-reasons-` | 10 | Motivos de pedido |
| `TVAGT` | `tvagt/rejection-reasons-` | 15 | Motivos de rejeição |
| `TVKGGT` | `tvkggt/customer-groups-` | 20 | Grupos de clientes |
| `DD07T` | `dd07t/domain-texts-` | 20 | Textos de domínio (CMGST, ZZ_ST_DEAL, OIG_SSTSF) |
| `DD07V` | `dd07v/domain-values-` | 30 | Valores de domínio |
