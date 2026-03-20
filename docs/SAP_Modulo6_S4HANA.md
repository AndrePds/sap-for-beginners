# 📙 SAP — Módulo 6: SAP S/4HANA
### CDS Views, AMDP e Virtual Data Models

---

> **Objetivo do módulo:** Entender como o S/4HANA revoluciona a arquitetura de dados SAP — eliminando agregados, introduzindo CDS Views como camada semântica, e usando o poder do HANA in-memory para cálculos que antes exigiam BW.

---

## 1. O que mudou com o S/4HANA?

O SAP S/4HANA é a geração atual do ERP SAP, construída sobre o banco de dados **SAP HANA** (in-memory). Isso muda fundamentalmente como os dados são armazenados, acessados e modelados.

### 1.1 Principais mudanças arquiteturais

| Aspecto | SAP ECC (legado) | SAP S/4HANA |
|---------|-----------------|-------------|
| Banco de dados | Qualquer (Oracle, SQL Server, DB2) | **SAP HANA exclusivo** |
| Tabelas de agregado | KONV, GLT0, BSID/BSAD separadas | **Eliminadas** — calculadas on-the-fly |
| Modelo FI | BSEG + views separadas | **ACDOCA** — tabela universal de lançamentos |
| Modelo CO | COEP, COSP, COSS separados | **ACDOCA** — unificado com FI |
| Modelo MM | Estoque em MARD, MCHB... | **MATDOC** + views de estoque simplificadas |
| Acesso analítico | BW separado obrigatório | **CDS Views** diretamente no ERP |
| Linguagem de acesso | ABAP Open SQL | **ABAP Open SQL + CDS + AMDP** |

### 1.2 SAP HANA — Por que muda tudo?

```
SAP HANA armazena dados em COLUNAS (não em linhas):

Banco Tradicional (row-store):
  Linha 1: [EBELN=4500001] [LIFNR=FORN1] [BEDAT=20240101] [NETWR=10000]
  Linha 2: [EBELN=4500002] [LIFNR=FORN2] [BEDAT=20240102] [NETWR=20000]

SAP HANA (column-store):
  EBELN: [4500001, 4500002, ...]
  LIFNR: [FORN1, FORN2, ...]
  NETWR: [10000, 20000, ...]   ← Soma/agregação em microssegundos!
```

**Consequências práticas:**
- Agregações (SUM, COUNT, AVG) são **ordens de magnitude mais rápidas**
- Compressão de dados chega a **10x** (colunas com valores repetidos comprimem muito bem)
- Índices secundários **quase desnecessários** para leitura analítica
- BW perde parte de sua razão de existir — o ERP já processa analítica

---

## 2. ACDOCA — A Tabela Universal de Lançamentos

### 2.1 O que é ACDOCA?

No S/4HANA, a tabela **ACDOCA** (Universal Journal Entry Line Items) substitui e unifica:

```
ECC (múltiplas tabelas):          S/4HANA (uma tabela):
  BSEG  (FI line items)    ─┐
  COEP  (CO actual)        ─┤
  COSP  (CO plan period)   ─┼──►  ACDOCA
  COSS  (CO plan year)     ─┤    (Universal Journal)
  FAGLFLEXA (New GL)       ─┤
  MLCD  (Material Ledger)  ─┘
```

### 2.2 Campos principais da ACDOCA

| Campo | Descrição |
|-------|-----------|
| `RLDNR` | Ledger (0L = Leading Ledger) |
| `RBUKRS` | Company Code |
| `GJAHR` | Ano fiscal |
| `BELNR` | Número do documento |
| `DOCLN` | Número da linha |
| `BUDAT` | Data de lançamento |
| `BLART` | Tipo de documento |
| `RACCT` | Conta contábil (G/L Account) |
| `RCOSTCENTER` | Centro de custo |
| `RPRCTR` | Centro de lucro |
| `RSEGMENT` | Segmento |
| `RBUSA` | Área de negócio |
| `PRCTR` | Centro de lucro |
| `MATNR` | Material (para MM) |
| `WERKS` | Planta |
| `HSL` | Valor em moeda da empresa |
| `KSL` | Valor em moeda do grupo |
| `TSL` | Valor em moeda da transação |
| `MENGE` | Quantidade |
| `MEINS` | Unidade de medida |

```sql
-- Consulta básica ACDOCA — substitui BKPF + BSEG
SELECT rbukrs, gjahr, belnr, budat, blart,
       racct, rcostcenter, rprctr,
       hsl, ksl, menge
  FROM acdoca
  INTO TABLE @DATA(lt_journal)
  WHERE rldnr  = '0L'              -- Leading Ledger
    AND rbukrs = 'BRAS'
    AND gjahr  = '2024'
    AND budat BETWEEN '20240101' AND '20241231'
    AND blart  IN ('RE', 'WE').    -- Invoice + GR
```

### 2.3 MATDOC — Novo modelo de movimentação MM

No S/4HANA, as tabelas MKPF/MSEG são substituídas (ou complementadas) pela **MATDOC**:

| ECC | S/4HANA |
|-----|---------|
| MKPF (cabeçalho) + MSEG (itens) | **MATDOC** (tabela única) |

```sql
-- Movimentações de material no S/4HANA
SELECT mblnr, mjahr, zeile, budat,
       bwart, matnr, werks, lgort,
       menge, meins, dmbtr
  FROM matdoc
  INTO TABLE @DATA(lt_mov)
  WHERE mandt  = @sy-mandt
    AND werks  = '1000'
    AND bwart  = '101'
    AND budat BETWEEN '20240101' AND '20241231'.
```

---

## 3. CDS Views — Core Data Services

### 3.1 O que são CDS Views?

**CDS (Core Data Services)** é o framework de modelagem de dados do S/4HANA. CDS Views são **views semânticas** definidas em DDL (Data Definition Language) e executadas diretamente no banco HANA.

```
Tabelas físicas (ACDOCA, EKKO, EKPO...)
        ↓
CDS Views (camada semântica)
        ├── Associações entre entidades
        ├── Anotações de negócio (@Semantics, @Analytics)
        └── Cálculos e derivações
        ↓
Consumidores
  ├── ABAP programs
  ├── OData APIs (Fiori/BTP)
  ├── Analytics Cloud (SAC)
  └── Ferramentas externas (ADF, Power BI)
```

### 3.2 Hierarquia de CDS Views (VDM)

O SAP organiza as CDS Views em camadas chamadas **Virtual Data Model (VDM)**:

```
Interface Views (I_*)          ← Camada de interface pública estável
        ↑
Composite Views (C_*)          ← Composições para casos de uso específicos
        ↑
Basic/Restricted Views (R_*)   ← Acesso direto às tabelas físicas (uso interno SAP)
        ↑
Tabelas físicas (ACDOCA, EKKO, VBAK...)
```

### 3.3 Sintaxe de uma CDS View

```abap
" Exemplo: CDS View para Purchase Orders
@AbapCatalog.sqlViewName: 'ZV_PO_HEADER'
@AbapCatalog.compiler.compareFilter: true
@AccessControl.authorizationCheck: #CHECK
@EndUserText.label: 'Purchase Order Header'
@Analytics.dataCategory: #FACT

define view Z_PO_HEADER
  as select from ekko as PO

  -- Associações (lazy join — só executado se o campo for usado)
  association [0..1] to lfa1 as _Supplier
    on $projection.Supplier = _Supplier.lifnr

  association [0..*] to ekpo as _Items
    on $projection.PurchaseOrder = _Items.ebeln

{
  -- Chaves
  key PO.ebeln                    as PurchaseOrder,

  -- Dimensões
      PO.lifnr                    as Supplier,
      PO.ekorg                    as PurchasingOrganization,
      PO.ekgrp                    as PurchasingGroup,
      PO.bukrs                    as CompanyCode,
      PO.bsart                    as PurchaseOrderType,

  -- Datas
      PO.bedat                    as PurchaseOrderDate,

  -- Medidas
      @Semantics.amount.currencyCode: 'Currency'
      PO.netwr                    as NetAmount,
      PO.waers                    as Currency,

  -- Exposição de associações
      _Supplier,
      _Items
}
```

### 3.4 Consumindo CDS Views em ABAP

```abap
" CDS Views são acessadas como tabelas normais em ABAP
SELECT PurchaseOrder,
       Supplier,
       PurchaseOrderDate,
       NetAmount,
       Currency
  FROM Z_PO_HEADER                   " Nome da CDS View
  INTO TABLE @DATA(lt_po)
  WHERE CompanyCode      = 'BRAS'
    AND PurchaseOrderDate >= '20240101'.

" Navegar pela associação (path expression)
SELECT PurchaseOrder,
       _Supplier-name1 AS SupplierName,    " Acessa campo via associação
       NetAmount
  FROM Z_PO_HEADER
  INTO TABLE @DATA(lt_po_enriched)
  WHERE CompanyCode = 'BRAS'.
```

### 3.5 CDS Views SAP padrão — As mais importantes

#### Financeiro (FI/CO)

| CDS View | Descrição |
|----------|-----------|
| `I_JournalEntry` | Universal Journal (ACDOCA) — visão principal |
| `I_JournalEntryItem` | Itens do journal com enriquecimento |
| `I_GLAccountLineItem` | G/L Account Line Items |
| `I_CostCenter` | Centro de custo |
| `I_ProfitCenter` | Centro de lucro |
| `I_AccountingDocument` | Documento contábil |

#### Compras (MM)

| CDS View | Descrição |
|----------|-----------|
| `I_PurchaseOrder` | Purchase Order header |
| `I_PurchaseOrderItem` | Purchase Order items |
| `I_GoodsMovement` | Movimentações de material |
| `I_MaterialStock` | Posição de estoque atual |
| `I_SupplierInvoice` | Fatura de fornecedor |

#### Vendas (SD)

| CDS View | Descrição |
|----------|-----------|
| `I_SalesOrder` | Sales Order header |
| `I_SalesOrderItem` | Sales Order items |
| `I_BillingDocument` | Billing document |
| `I_DeliveryDocument` | Delivery |

```abap
" Usando CDS View padrão SAP — I_PurchaseOrderItem
SELECT PurchaseOrder,
       PurchaseOrderItem,
       Material,
       Plant,
       OrderQuantity,
       NetPriceAmount,
       NetPriceQuantity,
       DocumentCurrency
  FROM I_PurchaseOrderItem
  INTO TABLE @DATA(lt_items)
  WHERE CompanyCode = 'BRAS'
    AND CreationDate >= '20240101'.
```

---

## 4. AMDP — ABAP Managed Database Procedures

### 4.1 O que são AMDPs?

**AMDP (ABAP Managed Database Procedures)** são procedimentos escritos em **SQLScript** (linguagem nativa do HANA) mas gerenciados e chamados a partir do ABAP. Permitem executar lógica complexa **diretamente no banco HANA**, sem mover dados para a camada de aplicação.

```
Sem AMDP:
  HANA → [todos os dados] → Servidor de Aplicação ABAP → processa → resultado

Com AMDP:
  HANA → [processa no banco] → [apenas resultado] → Servidor de Aplicação ABAP
```

### 4.2 Quando usar AMDP?

- Cálculos complexos em **grandes volumes** que seriam lentos em ABAP
- Lógica com **funções nativas HANA** não disponíveis em ABAP Open SQL
- **Transformações analíticas**: janelas temporais, percentis, outliers
- Substituição de **BW transformations** pesadas

### 4.3 Estrutura de uma AMDP

```abap
CLASS zcl_amdp_analise_estoque DEFINITION PUBLIC.
  PUBLIC SECTION.
    INTERFACES if_amdp_marker_hdb.    " Marca como AMDP

    " Declaração do método
    CLASS-METHODS get_estoque_critico
      IMPORTING
        VALUE(iv_werks) TYPE werks_d
        VALUE(iv_perc)  TYPE p DECIMALS 2    " % abaixo do mínimo
      EXPORTING
        VALUE(et_result) TYPE STANDARD TABLE.

ENDCLASS.

CLASS zcl_amdp_analise_estoque IMPLEMENTATION.

  METHOD get_estoque_critico
    BY DATABASE PROCEDURE                  " Executa no banco HANA
    FOR HDB                                " For HANA Database
    LANGUAGE SQLSCRIPT                     " Linguagem: SQLScript
    OPTIONS READ-ONLY                      " Somente leitura
    USING mard marc.                       " Tabelas SAP usadas

    " SQLScript — código executado diretamente no HANA
    DECLARE lv_cutoff DOUBLE;

    -- Calcular estoque atual vs. ponto de reposição
    lt_estoque = SELECT
        m.matnr,
        m.werks,
        m.lgort,
        m.labst                                          AS estoque_disponivel,
        mc.minbe                                         AS estoque_minimo,
        CASE
          WHEN mc.minbe > 0
          THEN (m.labst / mc.minbe) * 100.0
          ELSE NULL
        END                                              AS perc_do_minimo
      FROM mard AS m
      INNER JOIN marc AS mc
        ON m.mandt = mc.mandt
       AND m.matnr = mc.matnr
       AND m.werks = mc.werks
      WHERE m.mandt = session_context('CLIENT')
        AND m.werks = :iv_werks
        AND mc.minbe > 0;

    -- Filtrar apenas abaixo do percentual crítico
    et_result = SELECT *
      FROM :lt_estoque
      WHERE perc_do_minimo <= :iv_perc
      ORDER BY perc_do_minimo ASC;

  ENDMETHOD.

ENDCLASS.

" Chamada do AMDP em programa ABAP
DATA lt_criticos TYPE STANDARD TABLE OF ...

zcl_amdp_analise_estoque=>get_estoque_critico(
  EXPORTING
    iv_werks = '1000'
    iv_perc  = 20          " Materiais com menos de 20% do estoque mínimo
  IMPORTING
    et_result = lt_criticos
).
```

---

## 5. Virtual Data Models (VDM) — A Camada Semântica SAP

### 5.1 Conceito de VDM

O **VDM** é o conjunto completo de CDS Views que o SAP entrega como parte do S/4HANA. É a camada que:

- **Abstrai** a complexidade das tabelas físicas
- **Garante estabilidade** — APIs públicas que não mudam entre versões
- **Agrega semântica** — campos com nomes de negócio (não técnicos)
- **Habilita OData** — cada C_ View pode ser exposta como API REST

### 5.2 Como navegar no VDM

```
Transação: SEGW         → Gateway Service Builder
Transação: /IWFND/MAINT_SERVICE → Gerenciar serviços OData
Eclipse ADT:            → CDS source browser, onde ver todas as views
SAP API Business Hub:   → api.sap.com → documentação das C_ Views públicas
```

### 5.3 Descobrindo CDS Views disponíveis

```abap
" Listar todas as CDS Views ativas no sistema
SELECT ddlname, ddtext
  FROM dd25l
  INTO TABLE @DATA(lt_views)
  WHERE ddlname LIKE 'I_PURCHASE%'    " Filtrar por prefixo
    AND as4local = 'A'.               " Apenas ativas

" Ou via SE11 → informar nome da view → "Where-Used List"
```

### 5.4 Anotações CDS mais importantes para dados

```abap
" Anotações que impactam como os dados são expostos:

@Analytics.dataCategory: #FACT        " Tabela de fatos (tem medidas)
@Analytics.dataCategory: #DIMENSION   " Tabela de dimensão

@Semantics.amount.currencyCode: 'Currency'   " Campo é valor monetário
@Semantics.quantity.unitOfMeasure: 'Unit'    " Campo é quantidade
@Semantics.calendar.date: true               " Campo é data

@ObjectModel.foreignKey.association: '_Material'  " Chave estrangeira
@ObjectModel.text.association: '_MaterialText'    " Texto/descrição

" Essas anotações são lidas por:
" - SAP Analytics Cloud (SAC) para montar modelos automaticamente
" - Fiori Elements para gerar UIs
" - ADF SAP ODP Connector para mapear campos
```

---

## 6. Comparativo de Acesso a Dados: ECC vs. S/4HANA

### 6.1 Consulta de saldo contábil

```abap
" ❌ ECC: precisa de múltiplas tabelas
" Saldo G/L = BSAS (cleared) + BSIS (open) + GLT0 (totais período)
SELECT SUM( dmbtr * CASE shkzg WHEN 'S' THEN 1 ELSE -1 END )
  FROM bsis WHERE bukrs = 'BRAS' AND hkont = '0000400000' AND gjahr = '2024'
UNION ALL
SELECT SUM( dmbtr * CASE shkzg WHEN 'S' THEN 1 ELSE -1 END )
  FROM bsas WHERE bukrs = 'BRAS' AND hkont = '0000400000' AND gjahr = '2024'.

" ✅ S/4HANA: direto na ACDOCA
SELECT SUM( hsl )
  FROM acdoca
  INTO @DATA(lv_saldo)
  WHERE rldnr  = '0L'
    AND rbukrs = 'BRAS'
    AND racct  = '0000400000'
    AND gjahr  = '2024'.
```

### 6.2 Posição de estoque atual

```abap
" ❌ ECC: MARD + MCHB + MSKU + ... (vários tipos de estoque)
SELECT SUM( labst ) FROM mard
  WHERE matnr = 'MAT-001' AND werks = '1000'.

" ✅ S/4HANA: CDS View I_MaterialStock
SELECT Material, Plant, StorageLocation,
       MatlWrhsStkQtyInMatlBaseUnit AS Quantity
  FROM I_MaterialStock
  INTO TABLE @DATA(lt_stock)
  WHERE Material = 'MAT-001'
    AND Plant    = '1000'.
```

---

## ✅ Checklist de Aprendizado — Módulo 6

- [ ] Explicar as principais diferenças arquiteturais entre ECC e S/4HANA
- [ ] Entender por que o HANA column-store acelera análises
- [ ] Consultar a ACDOCA como substituta de BKPF+BSEG+COEP
- [ ] Ler e interpretar a sintaxe de uma CDS View
- [ ] Consumir CDS Views padrão SAP (I_PurchaseOrderItem, I_JournalEntryItem) em ABAP
- [ ] Entender a hierarquia VDM (R_ → I_ → C_)
- [ ] Criar uma AMDP simples com SQLScript para processamento analítico no HANA
- [ ] Navegar no VDM para descobrir views disponíveis para um processo de negócio

---

## 📚 Próximo Módulo

➡️ **Módulo 7 — SAP BTP:** Extração de dados para cloud via APIs OData, Integration Suite e conexão com serviços Azure/AWS.

---

*Documento gerado para uso interno — Programa de Capacitação SAP*
*Versão 1.0 | Módulo 6 de 8*
