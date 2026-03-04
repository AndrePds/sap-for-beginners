# 📕 SAP — Módulo 3: Avançado
### Programa de Estudo: Zero to Hero em Dados SAP

---

> **Objetivo do módulo:** Dominar a camada profunda de dados do SAP — rastreabilidade de mudanças, integração via IDocs/BAPIs, extração com JOINs complexos, performance de leitura e introdução à camada analítica SAP BW.

---

## 1. Change Documents — Rastreabilidade de Alterações

### 1.1 O que são Change Documents?

O SAP registra automaticamente **toda alteração feita em dados mestres e documentos** através do mecanismo de Change Documents. Isso garante auditoria completa: quem alterou, quando, de qual valor para qual valor.

```
Usuário altera campo NETPR (preço) em uma PO
        ↓
SAP grava automaticamente em:
  CDHDR  →  Cabeçalho: quem, quando, qual objeto
  CDPOS  →  Detalhe: qual campo, valor antigo, valor novo
```

### 1.2 Tabelas CDHDR e CDPOS

#### CDHDR — Cabeçalho do Change Document

| Campo | Descrição |
|-------|-----------|
| `CHANGENR` | Número do Change Document (chave) |
| `OBJECTCLAS` | Classe do objeto alterado (ex: `EINKBELEG` = PO) |
| `OBJECTID` | ID do objeto (ex: número da PO) |
| `UDATE` | Data da alteração |
| `UTIME` | Hora da alteração |
| `USERNAME` | Usuário que realizou a alteração |
| `TCODE` | Transação usada |

#### CDPOS — Detalhe das Alterações

| Campo | Descrição |
|-------|-----------|
| `CHANGENR` | FK → CDHDR |
| `TABNAME` | Tabela alterada (ex: EKPO) |
| `FNAME` | Campo alterado (ex: NETPR) |
| `VALUE_OLD` | Valor anterior |
| `VALUE_NEW` | Novo valor |
| `CHNGIND` | Tipo: `U`=Update, `I`=Insert, `D`=Delete |

### 1.3 Classes de Objeto mais relevantes

| OBJECTCLAS | Objeto |
|------------|--------|
| `EINKBELEG` | Purchase Order |
| `VERKBELEG` | Sales Order |
| `MATERIAL` | Material Master |
| `DEBITOR` | Customer Master |
| `KREDITOR` | Vendor Master |
| `LIEF` | Delivery |

```sql
-- Histórico de alterações de preço em POs
SELECT
    h.CHANGENR,
    h.OBJECTID    AS nr_po,
    h.UDATE       AS data_alteracao,
    h.UTIME       AS hora_alteracao,
    h.USERNAME    AS usuario,
    p.TABNAME     AS tabela,
    p.FNAME       AS campo,
    p.VALUE_OLD   AS valor_anterior,
    p.VALUE_NEW   AS novo_valor
FROM CDHDR h
JOIN CDPOS p ON h.MANDT = p.MANDT AND h.CHANGENR = p.CHANGENR
WHERE h.MANDT      = '100'
  AND h.OBJECTCLAS = 'EINKBELEG'
  AND p.FNAME      = 'NETPR'         -- Campo de preço
  AND h.UDATE BETWEEN '20240101' AND '20241231'
ORDER BY h.UDATE DESC, h.UTIME DESC
```

---

## 2. Número de Documento e Ano Fiscal como Chaves de Negócio

### 2.1 Por que o ano fiscal é sempre chave?

No SAP, documentos FI são **renumerados a cada ano fiscal**. Isso significa que o documento `1000000001` de 2023 é diferente do `1000000001` de 2024. Por isso, a chave é **sempre tripla**:

```
BUKRS (Company Code) + BELNR (Número) + GJAHR (Ano Fiscal)
```

> ⚠️ **Erro clássico:** Fazer JOIN em documentos FI usando apenas `BELNR` sem incluir `GJAHR` resulta em dados duplicados ou cruzados entre anos.

### 2.2 Padrão de chaves por área

| Área | Chave do Documento |
|------|--------------------|
| FI (BKPF/BSEG) | `BUKRS + BELNR + GJAHR` |
| MM Material Doc (MKPF/MSEG) | `MBLNR + MJAHR` |
| PP Ordem (AUFK) | `AUFNR` (único, não renumera por ano) |
| SD (VBAK/VBRK) | `VBELN` (único global) |
| MM PO (EKKO) | `EBELN` (único global) |

### 2.3 Exercício Fiscal vs. Calendário

O SAP suporta anos fiscais que **não coincidem com o ano calendário**. No Brasil, o ano fiscal normalmente é Jan–Dez, mas em multinacionais pode ser Apr–Mar (fiscal year variant).

```sql
-- Consulta correta: sempre incluir GJAHR em documentos FI
SELECT
    b.BUKRS,
    b.BELNR,
    b.GJAHR,
    b.BUDAT,
    s.HKONT,
    s.DMBTR,
    s.SHKZG
FROM BKPF b
JOIN BSEG s ON s.MANDT = b.MANDT
           AND s.BUKRS  = b.BUKRS
           AND s.BELNR  = b.BELNR
           AND s.GJAHR  = b.GJAHR   -- ← NUNCA omitir esta linha
WHERE b.MANDT = '100'
  AND b.BUKRS = 'BRAS'
  AND b.GJAHR = '2024'
```

---

## 3. Extração Avançada com JOINs Complexos

### 3.1 Query End-to-End: Custo Real de Produção

Esta query une PP + MM + FI/CO para mostrar o custo real de uma ordem de produção:

```sql
-- Custo real de ordens de produção: componentes consumidos + valor FI
SELECT
    op.AUFNR                        AS nr_ordem,
    op.MATNR                        AS produto_acabado,
    op.WERKS                        AS planta,
    comp.MATNR                      AS componente,
    comp.MENGE                      AS qtd_consumida,
    comp.DMBTR                      AS valor_consumo,
    fi.BELNR                        AS doc_fi,
    fi.BUDAT                        AS data_lancamento
FROM AFPO op
-- Componentes consumidos (saída para OP)
JOIN MSEG comp ON comp.MANDT  = op.MANDT
              AND comp.AUFNR  = op.AUFNR
              AND comp.BWART  = '261'    -- Saída p/ OP
-- Documento FI gerado pela movimentação
JOIN MKPF mh  ON mh.MANDT   = comp.MANDT
              AND mh.MBLNR   = comp.MBLNR
              AND mh.MJAHR   = comp.MJAHR
JOIN BKPF fi  ON fi.MANDT   = mh.MANDT
              AND fi.AWKEY   = CONCAT(mh.MBLNR, mh.MJAHR)
              AND fi.AWTYP   = 'MKPF'
WHERE op.MANDT = '100'
  AND op.WERKS = '1000'
  AND mh.BUDAT BETWEEN '20240101' AND '20241231'
```

### 3.2 Query: Rentabilidade por Pedido de Venda

```sql
-- Receita (Billing) vs. Custo (Saída de Estoque) por SO
SELECT
    so.VBELN                          AS nr_so,
    so.KUNNR                          AS cliente,
    si.MATNR                          AS material,
    si.KWMENG                         AS qtd_pedida,
    bk.NETWR                          AS receita_bruta,
    bk.FKDAT                          AS data_faturamento,
    ABS(custo.DMBTR)                  AS custo_mercadoria
FROM VBAK so
JOIN VBAP si   ON so.MANDT = si.MANDT AND so.VBELN = si.VBELN
-- Billing
JOIN VBRP bi   ON bi.MANDT = si.MANDT
              AND bi.AUBEL = si.VBELN
              AND bi.AUPOS = si.POSNR
JOIN VBRK bk   ON bk.MANDT = bi.MANDT AND bk.VBELN = bi.VBELN
-- Custo da saída de mercadoria (BWART 601)
JOIN MSEG custo ON custo.MANDT  = si.MANDT
               AND custo.VBELN  = bk.VBELN   -- billing ref
               AND custo.BWART  = '601'
WHERE so.MANDT = '100'
  AND bk.FKDAT >= '20240101'
  AND bk.RFBSK = 'C'    -- Transferido para FI
```

### 3.3 Tabela AWKEY — A cola entre módulos e FI

O campo `AWKEY` na `BKPF` é a **chave de referência de origem** do documento FI. Ele indica qual documento logístico gerou aquele lançamento contábil:

| AWTYP | Origem | AWKEY contém |
|-------|--------|--------------|
| `MKPF` | Material Document (GR/GI) | MBLNR + MJAHR |
| `VBRK` | Billing SD | VBELN (Billing) |
| `RMRP` | Invoice MM (MIRO) | BELNR + GJAHR |
| `AUFK` | Liquidação de OP | AUFNR |

```sql
-- Encontrar o documento FI gerado por um Material Document específico
SELECT BELNR, GJAHR, BUDAT, BLART
FROM BKPF
WHERE MANDT = '100'
  AND AWTYP = 'MKPF'
  AND AWKEY = '50000123002024'  -- MBLNR(10) + MJAHR(4)
```

---

## 4. IDocs — Intermediate Documents

### 4.1 O que são IDocs?

IDocs são o **mecanismo padrão de integração do SAP com sistemas externos**. São estruturas de dados padronizadas que trafegam entre sistemas via ALE (Application Link Enabling) ou EDI.

```
Sistema Externo / Parceiro EDI
        ↓  (IDoc Inbound)
SAP processa e cria documentos internos

SAP gera documento (ex: NF de venda)
        ↓  (IDoc Outbound)
Sistema Externo recebe os dados
```

### 4.2 Estrutura de um IDoc

```
IDoc
├── Control Record (1 registro)   →  Tabela EDIDC
│     Quem enviou, quem recebeu, tipo de IDoc, status
│
├── Data Records (N registros)    →  Tabela EDID4
│     Segmentos com os dados do negócio
│
└── Status Records (N registros)  →  Tabela EDIDS
      Histórico de processamento e erros
```

### 4.3 Tabelas de IDoc

| Tabela | Descrição |
|--------|-----------|
| `EDIDC` | Control Record — cabeçalho do IDoc |
| `EDID4` | Data Records — segmentos de dados |
| `EDIDS` | Status Records — log de status |
| `EDIMSG` | Tipos de mensagem configurados |

#### EDIDC — Campos principais

| Campo | Descrição |
|-------|-----------|
| `DOCNUM` | Número do IDoc (chave) |
| `MESTYP` | Tipo de mensagem (ORDERS, DESADV, INVOIC...) |
| `IDOCTP` | Tipo básico do IDoc |
| `DIRECT` | Direção: `1`=Outbound, `2`=Inbound |
| `STATUS` | Status de processamento |
| `SNDPOR` | Porta remetente |
| `RCVPOR` | Porta destinatária |
| `CREDAT` | Data de criação |

#### Status de IDoc — Os mais relevantes

| Status | Descrição |
|--------|-----------|
| `01` | IDoc gerado |
| `02` | Erro ao enviar para ALE |
| `03` | Dados transmitidos ao parceiro |
| `12` | Despachado para aplicação |
| `51` | **Erro na aplicação** (principal status de erro) |
| `53` | **Processado com sucesso** |

```sql
-- IDocs com erro para análise
SELECT
    d.DOCNUM,
    d.MESTYP,
    d.DIRECT,
    d.STATUS,
    d.CREDAT,
    s.STATXT    AS descricao_status,
    s.STAMQU    AS mensagem_erro
FROM EDIDC d
JOIN EDIDS s ON d.MANDT = s.MANDT AND d.DOCNUM = s.DOCNUM
WHERE d.MANDT  = '100'
  AND d.STATUS = '51'              -- Erro na aplicação
  AND d.CREDAT >= '20240101'
ORDER BY d.CREDAT DESC
```

### 4.4 Tipos de Mensagem IDoc comuns

| MESTYP | Processo |
|--------|----------|
| `ORDERS` | Purchase Order (recebimento de pedido) |
| `ORDRSP` | Confirmação de pedido |
| `DESADV` | Aviso de expedição (ASN) |
| `INVOIC` | Fatura eletrônica |
| `MATMAS` | Material Master |
| `DEBMAS` | Customer Master |
| `CREMAS` | Vendor Master |
| `WMMBXY` | Movimentação de estoque WM |

---

## 5. BAPIs — Business Application Programming Interfaces

### 5.1 O que são BAPIs?

BAPIs são **funções RFC padronizadas** que expõem funcionalidades do SAP para integração programática. Permitem criar, ler e modificar dados SAP de forma segura e documentada.

```
Sistema Externo / Script / Job
        ↓  (chamada RFC)
BAPI executa a lógica de negócio SAP
        ↓
Dados criados/lidos com validação completa
```

### 5.2 Categorias de BAPIs

| Categoria | Descrição | Exemplo |
|-----------|-----------|---------|
| `GetList` | Lista registros | `BAPI_MATERIAL_GETLIST` |
| `GetDetail` | Detalha um registro | `BAPI_PO_GETDETAIL` |
| `Create` | Cria novo documento | `BAPI_PO_CREATE1` |
| `Change` | Altera documento | `BAPI_PO_CHANGE` |
| `SaveReplica` | Replica dado mestre | `BAPI_MATERIAL_SAVEREPLICA` |

### 5.3 BAPIs mais utilizadas para dados

| BAPI | Módulo | Função |
|------|--------|--------|
| `BAPI_PO_GETDETAIL` | MM | Lê detalhes de uma PO |
| `BAPI_PO_CREATE1` | MM | Cria Purchase Order |
| `BAPI_GOODSMVT_CREATE` | MM | Cria movimentação de estoque |
| `BAPI_SALESORDER_GETLIST` | SD | Lista Sales Orders |
| `BAPI_SALESORDER_CREATEFROMDAT2` | SD | Cria Sales Order |
| `BAPI_ACC_DOCUMENT_POST` | FI | Posta documento contábil |
| `BAPI_MATERIAL_GETLIST` | MM | Lista materiais |
| `RFC_READ_TABLE` | Basis | Lê qualquer tabela via RFC |

### 5.4 RFC_READ_TABLE — A BAPI universal de extração

A `RFC_READ_TABLE` é a função mais usada por ferramentas de extração (ADF, Python, etc.) para ler dados SAP remotamente:

```python
# Exemplo de chamada via Python (pyrfc / PyRFC)
import pyrfc

conn = pyrfc.Connection(
    ashost='sap-host',
    sysnr='00',
    client='100',
    user='usuario',
    passwd='senha'
)

result = conn.call(
    'RFC_READ_TABLE',
    QUERY_TABLE='EKKO',
    DELIMITER='|',
    FIELDS=[
        {'FIELDNAME': 'EBELN'},
        {'FIELDNAME': 'LIFNR'},
        {'FIELDNAME': 'BEDAT'},
        {'FIELDNAME': 'NETWR'}
    ],
    OPTIONS=[
        {'TEXT': "BEDAT >= '20240101'"}
    ]
)

for row in result['DATA']:
    print(row['WA'].split('|'))
```

> ⚠️ **Limitação:** RFC_READ_TABLE tem limite de ~512 caracteres por linha. Para tabelas com muitas colunas largas, use `/BODS/RFC_READ_TABLE` (versão SAP BusinessObjects) ou a BAPI `BBP_RFC_READ_TABLE`.

---

## 6. SAP Table Buffering — Performance de Leitura

### 6.1 O que é Table Buffering?

O SAP mantém um **buffer em memória** para tabelas frequentemente lidas e raramente alteradas. Isso evita acessos repetidos ao banco de dados.

### 6.2 Tipos de Buffer

| Tipo | Descrição | Quando usar |
|------|-----------|-------------|
| **Full Buffering** | Toda a tabela fica em memória | Tabelas pequenas de customizing (T001, T023) |
| **Generic Buffering** | Apenas registros com certa chave | Tabelas médias com acesso por chave parcial |
| **Single Record** | Apenas registro acessado | Tabelas grandes, acesso pontual |
| **Not Buffered** | Sem buffer | Tabelas transacionais volumosas |

### 6.3 Como verificar se uma tabela é bufferizada

```
SE11 → nome da tabela → aba "Technical Settings"
Campo: "Buffering" → mostra o tipo de buffer configurado
```

### 6.4 Impacto na extração de dados

```
Tabela COM buffer:
  Leitura rápida, mas pode trazer dados "stale" (desatualizados por milissegundos)
  Ideal para dados de configuração

Tabela SEM buffer (transacionais):
  Leitura direto do banco → sempre atual
  Sempre use filtros eficientes (campos da chave primária)
  NUNCA faça full scan em produção (BSEG, MSEG, EKPO...)
```

### 6.5 Boas práticas de performance em queries SAP

```sql
-- ✅ BOM: Filtra pelos campos da chave primária
SELECT * FROM BSEG
WHERE MANDT = '100'
  AND BUKRS = 'BRAS'
  AND BELNR = '1000000001'
  AND GJAHR = '2024'

-- ❌ RUIM: Filtra por campo não indexado em tabela grande
SELECT * FROM BSEG
WHERE MANDT = '100'
  AND HKONT = '0000400000'   -- Sem filtro de empresa/doc → full scan!

-- ✅ BOM: Use BSAS/BSIS (views particionadas) para G/L accounts
SELECT * FROM BSIS
WHERE MANDT = '100'
  AND BUKRS = 'BRAS'
  AND HKONT = '0000400000'
  AND GJAHR = '2024'
```

---

## 7. Introdução ao SAP BW — Camada Analítica

### 7.1 O que é SAP BW?

O **SAP BW (Business Warehouse)** é a plataforma de Data Warehouse da SAP. Ele extrai dados do SAP ERP (e outros sistemas), transforma e carrega em modelos analíticos otimizados para relatórios.

```
SAP ERP (dados transacionais)
        ↓  (via DataSources / Extractors)
SAP BW (dados analíticos)
        ↓
BEx Queries / Analysis for Office / SAC
        ↓
Usuários de negócio
```

### 7.2 Arquitetura de Dados no BW

```
PSA (Persistent Staging Area)
  ↓  (dados brutos, exatamente como vieram do ERP)
DataStore Objects — DSO / ADSO
  ↓  (dados granulares, histórico completo)
InfoCubes / CompositeProviders
  ↓  (dados agregados, modelo estrela)
BEx Queries / Relatórios
```

### 7.3 Objetos principais do BW

| Objeto | Descrição |
|--------|-----------|
| `InfoObject` | Unidade mínima de dado (equivale a um campo SAP) |
| `DSO/ADSO` | Tabela analítica com histórico de alterações |
| `InfoCube` | Modelo dimensional (fatos + dimensões) |
| `DataSource` | Extrator que conecta ERP → BW |
| `Transformation` | Regras de transformação (ETL) |
| `DTP` | Data Transfer Process — executa a carga |
| `Process Chain` | Orquestrador de cargas (equivale a um pipeline) |

### 7.4 DataSources padrão SAP (os mais usados)

| DataSource | Origem | Descrição |
|------------|--------|-----------|
| `2LIS_02_ITM` | MM | Itens de Purchase Order |
| `2LIS_02_SCL` | MM | Cronograma de PO |
| `2LIS_03_BF` | MM | Movimentos de material |
| `2LIS_11_VAITM` | SD | Itens de Sales Order |
| `2LIS_13_VDHDR` | SD | Cabeçalho de Billing |
| `0FI_GL_4` | FI | G/L Account Line Items |
| `0CO_OM_OPA_1` | CO | Ordens internas — dados reais |
| `0PP_C01` | PP | Ordens de produção |

### 7.5 Delta Extraction — Como o BW captura mudanças

O BW usa mecanismos de **delta** para extrair apenas registros novos ou alterados desde a última carga:

| Tipo Delta | Descrição |
|------------|-----------|
| `AIM` (After Image) | Envia o estado atual do registro alterado |
| `BIM` (Before Image) | Envia o estado anterior (para reverter) |
| `ADD` | Soma valores (para dados de movimento/quantidade) |
| `ABR` | Reversal (estorno) — subtrai o valor |

```
Extração Full (1ª vez):
  Todos os registros históricos → PSA → DSO

Extração Delta (diária):
  Apenas novos/alterados desde última carga → PSA → DSO (merge/upsert)
```

---

## 8. Padrões de Extração para Data Platforms Modernas

### 8.1 Estratégias de extração do SAP para Azure/Databricks

| Estratégia | Ferramenta | Melhor para |
|------------|------------|-------------|
| **RFC direto** | Python pyrfc, Azure Data Factory SAP Connector | Tabelas específicas, baixo volume |
| **IDoc streaming** | Azure Service Bus, Kafka | Eventos em tempo real |
| **BW DataSources** | SAP BW → ADF → ADLS | Grandes volumes com delta gerenciado |
| **ODP (Operational Data Provisioning)** | ADF SAP ODP Connector | Extração delta nativa sem BW |
| **SLT (SAP Landscape Transformation)** | SAP LT Replication | CDC em tempo real para qualquer tabela |

### 8.2 ODP — O método mais moderno

O **ODP (Operational Data Provisioning)** é o framework mais recente da SAP para extração de dados, substituindo gradualmente o BW DataSource tradicional.

```
SAP ERP
  └── ODP Framework
        ├── ODP-SAPI   →  DataSources BW tradicionais
        ├── ODP-CDS    →  CDS Views (SAP S/4HANA)
        └── ODP-BW     →  InfoProviders do BW

Azure Data Factory
  └── SAP ODP Connector
        ├── Full Load (init)
        └── Delta Load (incremental)
```

### 8.3 Tabelas de controle de delta no SAP

| Tabela | Descrição |
|--------|-----------|
| `ROOSOURCE` | Catálogo de DataSources disponíveis |
| `RODELTAM` | Configuração de delta por DataSource |
| `RSO2` | Transação para gerenciar DataSources |

---

## ✅ Checklist de Aprendizado — Módulo 3

Ao final deste módulo, você deve ser capaz de:

- [ ] Consultar CDHDR/CDPOS para rastrear qualquer alteração em dados mestres ou transações
- [ ] Entender por que `GJAHR` é sempre obrigatório em JOINs com documentos FI
- [ ] Usar o campo `AWKEY`/`AWTYP` para conectar documentos FI com sua origem logística
- [ ] Explicar a estrutura de um IDoc (Control, Data, Status Records)
- [ ] Identificar IDocs com erro via EDIDC/EDIDS
- [ ] Chamar a `RFC_READ_TABLE` via Python para extrair dados remotamente
- [ ] Aplicar boas práticas de performance (usar views de BSEG, filtrar por chave primária)
- [ ] Descrever a arquitetura do SAP BW (PSA → DSO → InfoCube)
- [ ] Explicar a diferença entre extração Full e Delta
- [ ] Identificar a estratégia de extração mais adequada para cada cenário (ODP, SLT, RFC, IDoc)

---

## 📚 Próximo Módulo

➡️ **Módulo 4 — SAP BDC (Batch Data Communication):** Automação de entrada de dados em massa via simulação de telas, gravação com SHDB, BDCDATA, CALL TRANSACTION vs SESSION METHOD, tratamento de erros e boas práticas para cargas massivas.

---

*Documento gerado para uso interno — Programa de Capacitação SAP*
*Versão 1.0 | Módulo 3 de 4*
