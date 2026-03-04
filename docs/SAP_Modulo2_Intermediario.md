# 📙 SAP — Módulo 2: Intermediário
### Programa de Estudo: Zero to Hero em Dados SAP

---

> **Objetivo do módulo:** Entender como os dados fluem dentro de cada módulo SAP, quais documentos são gerados em cada etapa do processo e como extrair informações de ponta a ponta de uma cadeia de negócio.

---

## 1. Conceito de Fluxo de Documentos (*Document Flow*)

No SAP, cada processo de negócio gera uma **cadeia de documentos encadeados**. Um documento referencia o anterior, criando um rastro completo e auditável.

```
COMPRAS:
  Requisição → Purchase Order → Entrada de Mercadoria → Fatura (Invoice)

VENDAS:
  Cotação → Sales Order → Delivery → Transferência → Billing (NF)

FINANÇAS:
  Qualquer documento logístico → Documento FI (lançamento contábil automático)
```

### Princípio fundamental
> 💡 **Todo movimento que tem valor financeiro gera automaticamente um documento FI.** Isso significa que dados de MM, SD e PP sempre reverberam no módulo FI/CO.

---

## 2. MM — Materials Management

### 2.1 O Processo Procure-to-Pay (P2P)

```
PR (Purchase Requisition)
        ↓
PO (Purchase Order)  ←── EKKO / EKPO
        ↓
GR (Goods Receipt — Entrada de Mercadoria)  ←── MKPF / MSEG
        ↓
IV (Invoice Verification — Verificação de Fatura)  ←── RBKP / RSEG
        ↓
Pagamento ao Fornecedor  ←── BKPF / BSEG
```

---

### 2.2 Purchase Order — Tabelas EKKO e EKPO

#### EKKO — Cabeçalho da PO

| Campo | Descrição |
|-------|-----------|
| `EBELN` | Número da Purchase Order (chave) |
| `BUKRS` | Company Code |
| `LIFNR` | Número do fornecedor |
| `EKORG` | Organização de compras |
| `EKGRP` | Grupo de compras |
| `BEDAT` | Data da PO |
| `BSART` | Tipo de documento (NB = Standard PO) |
| `WAERS` | Moeda |

#### EKPO — Itens da PO

| Campo | Descrição |
|-------|-----------|
| `EBELN` | Número da PO (FK → EKKO) |
| `EBELP` | Número do item |
| `MATNR` | Material |
| `WERKS` | Planta |
| `MENGE` | Quantidade pedida |
| `MEINS` | Unidade de medida |
| `NETPR` | Preço líquido unitário |
| `PEINH` | Por quantidade (preço por 1, 10, 100...) |
| `NETWR` | Valor líquido total do item |
| `ELIKZ` | Indicador de entrega completa |
| `EINDT` | Data de entrega |

```sql
-- Consulta: POs abertas com fornecedor e material
SELECT
    k.EBELN,
    k.LIFNR,
    k.BEDAT,
    p.EBELP,
    p.MATNR,
    p.MENGE,
    p.NETPR,
    p.NETWR
FROM EKKO k
JOIN EKPO p ON k.MANDT = p.MANDT AND k.EBELN = p.EBELN
WHERE k.MANDT = '100'
  AND k.BSTYP = 'F'        -- Categoria F = Purchase Order
  AND p.ELIKZ = ''          -- Entrega não encerrada
  AND k.BEDAT >= '20240101'
```

---

### 2.3 Goods Receipt — Tabelas MKPF e MSEG

Quando a mercadoria chega na planta, é criado um **Documento de Material** composto por:

- **MKPF** = Cabeçalho do documento de material
- **MSEG** = Itens/movimentações do documento

#### Tipos de Movimento (campo BWART em MSEG) — os mais comuns

| BWART | Descrição |
|-------|-----------|
| `101` | Entrada de mercadoria contra PO |
| `102` | Estorno de entrada contra PO |
| `201` | Saída de material para centro de custo |
| `261` | Saída de material para ordem de produção |
| `301` | Transferência entre plantas |
| `311` | Transferência entre depósitos |
| `501` | Entrada sem referência (sem PO) |
| `601` | Saída por entrega (SD) |

#### MSEG — Campos principais

| Campo | Descrição |
|-------|-----------|
| `MBLNR` | Número do documento de material |
| `MJAHR` | Ano do documento |
| `ZEILE` | Número do item |
| `BWART` | Tipo de movimento |
| `MATNR` | Material movimentado |
| `WERKS` | Planta |
| `LGORT` | Depósito |
| `MENGE` | Quantidade movimentada |
| `MEINS` | Unidade de medida |
| `DMBTR` | Valor em moeda local |
| `EBELN` | Referência à PO (se aplicável) |
| `EBELP` | Referência ao item da PO |

```sql
-- Consulta: Entradas de mercadoria contra PO no período
SELECT
    m.MBLNR,
    m.MJAHR,
    h.BUDAT,         -- Data de lançamento
    m.MATNR,
    m.WERKS,
    m.LGORT,
    m.MENGE,
    m.DMBTR,
    m.EBELN,
    m.EBELP
FROM MSEG m
JOIN MKPF h ON m.MANDT = h.MANDT
           AND m.MBLNR = h.MBLNR
           AND m.MJAHR = h.MJAHR
WHERE m.MANDT = '100'
  AND m.BWART = '101'          -- Entrada contra PO
  AND h.BUDAT BETWEEN '20240101' AND '20241231'
```

---

### 2.4 Invoice Verification — Tabelas RBKP e RSEG

Após a entrada de mercadoria, o departamento financeiro registra a fatura do fornecedor:

| Tabela | Descrição |
|--------|-----------|
| `RBKP` | Cabeçalho do documento de fatura (Invoice) |
| `RSEG` | Itens do documento de fatura |

#### RBKP — Campos principais

| Campo | Descrição |
|-------|-----------|
| `BELNR` | Número do documento de fatura |
| `GJAHR` | Ano fiscal |
| `BLDAT` | Data da fatura |
| `BUDAT` | Data de lançamento |
| `LIFNR` | Fornecedor |
| `RMWWR` | Valor bruto da fatura |
| `WAERS` | Moeda |
| `RBSTAT` | Status do documento |

---

### 2.5 Visão Completa P2P em uma Query

```sql
-- Rastreio completo: PO → GR → Invoice
SELECT
    po.EBELN          AS nr_po,
    po.LIFNR          AS fornecedor,
    pi.MATNR          AS material,
    pi.MENGE          AS qtd_pedida,
    gr.MENGE          AS qtd_recebida,
    gr.DMBTR          AS valor_recebido,
    inv.BELNR         AS nr_fatura,
    inv.RMWWR         AS valor_fatura
FROM EKKO po
JOIN EKPO pi  ON po.MANDT = pi.MANDT AND po.EBELN = pi.EBELN
LEFT JOIN MSEG gr  ON gr.MANDT = pi.MANDT
                  AND gr.EBELN = pi.EBELN
                  AND gr.EBELP = pi.EBELP
                  AND gr.BWART = '101'
LEFT JOIN RSEG ri  ON ri.MANDT = pi.MANDT
                  AND ri.EBELN = pi.EBELN
                  AND ri.EBELP = pi.EBELP
LEFT JOIN RBKP inv ON inv.MANDT = ri.MANDT
                  AND inv.BELNR = ri.BELNR
                  AND inv.GJAHR = ri.GJAHR
WHERE po.MANDT = '100'
  AND po.BEDAT >= '20240101'
```

---

## 3. SD — Sales & Distribution

### 3.1 O Processo Order-to-Cash (O2C)

```
Inquiry / Quotation (Consulta/Cotação)
        ↓
Sales Order  ←── VBAK / VBAP
        ↓
Delivery (Remessa)  ←── LIKP / LIPS
        ↓
Transfer Order / Goods Issue (Saída de Mercadoria)  ←── MSEG (BWART 601)
        ↓
Billing Document (Faturamento / NF)  ←── VBRK / VBRP
        ↓
Recebimento do Cliente  ←── BKPF / BSEG
```

---

### 3.2 Sales Order — Tabelas VBAK e VBAP

#### VBAK — Cabeçalho do Sales Order

| Campo | Descrição |
|-------|-----------|
| `VBELN` | Número do Sales Order (chave) |
| `AUART` | Tipo de ordem (OR = Standard Order) |
| `VKORG` | Organização de vendas |
| `VTWEG` | Canal de distribuição |
| `SPART` | Divisão |
| `KUNNR` | Cliente (sold-to) |
| `AUDAT` | Data de criação |
| `NETWR` | Valor líquido total |
| `WAERK` | Moeda |

#### VBAP — Itens do Sales Order

| Campo | Descrição |
|-------|-----------|
| `VBELN` | Número do SO (FK → VBAK) |
| `POSNR` | Número do item |
| `MATNR` | Material |
| `WERKS` | Planta (de onde sai) |
| `KWMENG` | Quantidade solicitada |
| `VRKME` | Unidade de venda |
| `NETPR` | Preço líquido unitário |
| `NETWR` | Valor líquido do item |
| `WAERK` | Moeda |
| `ABGRU` | Motivo de rejeição (se rejeitado) |
| `LGORT` | Depósito |

---

### 3.3 Delivery — Tabelas LIKP e LIPS

| Tabela | Descrição |
|--------|-----------|
| `LIKP` | Cabeçalho da remessa (Delivery) |
| `LIPS` | Itens da remessa |

#### LIKP — Campos principais

| Campo | Descrição |
|-------|-----------|
| `VBELN` | Número da Delivery |
| `VSTEL` | Ponto de expedição |
| `KUNNR` | Cliente |
| `WADAT` | Data de saída de mercadoria |
| `LFART` | Tipo de entrega |

#### LIPS — Campos principais

| Campo | Descrição |
|-------|-----------|
| `VBELN` | Número da Delivery |
| `POSNR` | Item da Delivery |
| `MATNR` | Material |
| `LFIMG` | Quantidade entregue |
| `VGBEL` | Referência ao Sales Order (VBAK.VBELN) |
| `VGPOS` | Referência ao item do SO |

---

### 3.4 Billing — Tabelas VBRK e VBRP

| Tabela | Descrição |
|--------|-----------|
| `VBRK` | Cabeçalho do Billing (Faturamento/NF) |
| `VBRP` | Itens do Billing |

#### VBRK — Campos principais

| Campo | Descrição |
|-------|-----------|
| `VBELN` | Número do Billing |
| `FKART` | Tipo de fatura |
| `FKDAT` | Data do faturamento |
| `KUNRG` | Cliente pagador |
| `NETWR` | Valor líquido |
| `WAERK` | Moeda |
| `RFBSK` | Status de transferência para FI |

```sql
-- Consulta: Rastreio SO → Delivery → Billing
SELECT
    so.VBELN     AS nr_so,
    so.KUNNR     AS cliente,
    si.MATNR     AS material,
    si.KWMENG    AS qtd_pedida,
    li.LFIMG     AS qtd_entregue,
    bi.VBELN     AS nr_billing,
    bk.FKDAT     AS data_fatura,
    bk.NETWR     AS valor_faturado
FROM VBAK so
JOIN VBAP si  ON so.MANDT = si.MANDT AND so.VBELN = si.VBELN
JOIN LIPS li  ON li.MANDT = si.MANDT
             AND li.VGBEL = si.VBELN
             AND li.VGPOS = si.POSNR
JOIN LIKP lk  ON lk.MANDT = li.MANDT AND lk.VBELN = li.VBELN
JOIN VBRP bi  ON bi.MANDT = li.MANDT AND bi.VGBEL = li.VBELN
JOIN VBRK bk  ON bk.MANDT = bi.MANDT AND bk.VBELN = bi.VBELN
WHERE so.MANDT = '100'
  AND so.AUDAT >= '20240101'
```

---

## 4. FI/CO — Financial Accounting & Controlling

### 4.1 Como o FI recebe dados dos outros módulos

Toda movimentação com valor financeiro gera automaticamente um **documento FI**:

```
GR (entrada de mercadoria)  →  FI: Débito Estoque / Crédito GR/IR
Invoice (fatura fornecedor) →  FI: Débito GR/IR / Crédito Contas a Pagar
Billing SD (nota fiscal)    →  FI: Débito Contas a Receber / Crédito Receita
```

### 4.2 Documento FI — Tabelas BKPF e BSEG

#### BKPF — Cabeçalho do Documento Contábil

| Campo | Descrição |
|-------|-----------|
| `BUKRS` | Company Code |
| `BELNR` | Número do documento FI |
| `GJAHR` | Ano fiscal |
| `BLART` | Tipo de documento (RE=Invoice, WE=GR, RV=Billing...) |
| `BUDAT` | Data de lançamento |
| `BLDAT` | Data do documento |
| `WAERS` | Moeda |
| `BKTXT` | Texto do cabeçalho |
| `USNAM` | Usuário que criou |

> ⚠️ **Atenção:** A chave do documento FI é **sempre tripla**: `BUKRS + BELNR + GJAHR`

#### BSEG — Segmentos/Linhas do Documento Contábil

| Campo | Descrição |
|-------|-----------|
| `BUKRS` | Company Code |
| `BELNR` | Número do documento |
| `GJAHR` | Ano fiscal |
| `BUZEI` | Número da linha |
| `KOART` | Tipo de conta (S=GL, D=Cliente, K=Fornecedor) |
| `HKONT` | Conta contábil (G/L Account) |
| `KUNNR` | Cliente (se KOART = D) |
| `LIFNR` | Fornecedor (se KOART = K) |
| `DMBTR` | Valor em moeda local |
| `SHKZG` | Débito (S) ou Crédito (H) |
| `KOSTL` | Centro de custo |
| `AUFNR` | Ordem interna |
| `PRCTR` | Centro de lucro |

> ⚠️ **Performance:** BSEG é uma das tabelas mais volumosas do SAP. Prefira usar as **views de BSEG**: BSAK (fornecedores quitados), BSIK (fornecedores em aberto), BSAD (clientes quitados), BSID (clientes em aberto), BSIS (GL em aberto), BSAS (GL quitado).

### 4.3 Views de BSEG — Guia de uso

| View/Tabela | Tipo de conta | Status |
|-------------|---------------|--------|
| `BSIK` | Fornecedor (K) | Em aberto (Open Items) |
| `BSAK` | Fornecedor (K) | Liquidado (Cleared) |
| `BSID` | Cliente (D) | Em aberto |
| `BSAD` | Cliente (D) | Liquidado |
| `BSIS` | G/L (S) | Em aberto |
| `BSAS` | G/L (S) | Liquidado |

```sql
-- Contas a pagar em aberto (fornecedores)
SELECT
    i.BUKRS,
    i.BELNR,
    i.GJAHR,
    i.LIFNR,
    i.BLDAT,
    i.DMBTR,
    i.SHKZG
FROM BSIK i
JOIN BKPF h ON h.MANDT = i.MANDT
           AND h.BUKRS = i.BUKRS
           AND h.BELNR = i.BELNR
           AND h.GJAHR = i.GJAHR
WHERE i.MANDT = '100'
  AND i.BUKRS = 'BRAS'
  AND i.BUDAT >= '20240101'
```

---

## 5. PP — Production Planning

### 5.1 O Processo de Produção

```
Planejamento (MRP)
        ↓
Planned Order (Ordem Planejada)
        ↓
Production Order (Ordem de Produção)  ←── AUFK / AFKO / AFPO
        ↓
Goods Issue (Saída de componentes)  ←── MSEG (BWART 261)
        ↓
Confirmação de Operações  ←── AFRU
        ↓
Goods Receipt (Entrada do produto acabado)  ←── MSEG (BWART 101 / 131)
        ↓
Settlement (Liquidação de custos)  ←── CO
```

### 5.2 Tabelas de Ordem de Produção

| Tabela | Descrição |
|--------|-----------|
| `AUFK` | Dados mestres da ordem (cabeçalho geral) |
| `AFKO` | Cabeçalho da ordem de produção (PP-específico) |
| `AFPO` | Itens da ordem de produção (produto final) |
| `AFVC` | Operações da ordem (roteiro) |
| `AFRU` | Confirmações de operação |
| `RESB` | Componentes reservados (lista de materiais consumidos) |

#### AUFK — Campos principais

| Campo | Descrição |
|-------|-----------|
| `AUFNR` | Número da ordem de produção |
| `AUART` | Tipo de ordem (PP01, PP02...) |
| `WERKS` | Planta |
| `ERDAT` | Data de criação |
| `GSTRP` | Data de início planejada |
| `GLTRP` | Data de término planejada |
| `OBJNR` | Número de objeto CO (ligação com custos) |
| `KOSTL` | Centro de custo responsável |

#### AFPO — Item da Ordem (produto produzido)

| Campo | Descrição |
|-------|-----------|
| `AUFNR` | Número da ordem |
| `POSNR` | Número do item |
| `MATNR` | Material produzido |
| `WERKS` | Planta |
| `PSMNG` | Quantidade planejada |
| `WEMNG` | Quantidade já recebida (GR) |
| `MEINS` | Unidade de medida |

```sql
-- Consulta: Ordens de produção com status de entrega
SELECT
    a.AUFNR,
    a.WERKS,
    a.ERDAT,
    a.GSTRP,
    a.GLTRP,
    p.MATNR,
    p.PSMNG    AS qtd_planejada,
    p.WEMNG    AS qtd_recebida,
    (p.PSMNG - p.WEMNG) AS saldo_pendente
FROM AUFK a
JOIN AFPO p ON a.MANDT = p.MANDT AND a.AUFNR = p.AUFNR
WHERE a.MANDT = '100'
  AND a.WERKS = '1000'
  AND a.ERDAT >= '20240101'
  AND a.AUART IN ('PP01', 'PP02')
```

---

## 6. Relacionamento Entre Módulos — Visão Integrada

O ponto mais poderoso do SAP é que **todos os módulos são interligados**. Veja como um processo de venda completo gera dados em múltiplos módulos:

```
CLIENTE FAZ PEDIDO
        │
        ▼
[SD] Sales Order (VBAK/VBAP)
        │
        ▼
[PP] Ordem de Produção criada via MRP (AUFK/AFPO)
        │
        ├──► [MM] Saída de Componentes (MSEG BWART 261)
        │         └──► [FI] Lançamento contábil (BKPF/BSEG)
        │                   └──► [CO] Custo debitado na OP (COEP)
        │
        ▼
[PP] Confirmação + Entrada do Produto Acabado (MSEG BWART 101)
        │
        ▼
[SD] Delivery / Saída de Mercadoria (LIKP/LIPS + MSEG BWART 601)
        │         └──► [FI] Baixa de Estoque (BKPF/BSEG)
        │
        ▼
[SD] Billing / Nota Fiscal (VBRK/VBRP)
        │         └──► [FI] Receita + Contas a Receber (BKPF/BSEG)
        │
        ▼
[FI] Recebimento do pagamento do cliente → Liquidação
```

### Tabela de Integração — Referências Cruzadas

| De | Para | Campo de ligação |
|----|------|-----------------|
| MSEG (GR) | EKPO (PO) | MSEG.EBELN = EKPO.EBELN |
| MSEG (GI) | AUFK (OP) | MSEG.AUFNR = AUFK.AUFNR |
| LIPS (Delivery) | VBAP (SO) | LIPS.VGBEL = VBAP.VBELN |
| VBRP (Billing) | LIPS | VBRP.VGBEL = LIPS.VBELN |
| BKPF (FI Doc) | MKPF (Mat Doc) | via AWKEY (object key) |
| BSEG | VBRK (Billing) | BSEG.VBELN = VBRK.VBELN |

---

## 7. Ferramentas de Exploração de Dados

### SE16N — Para consultas rápidas em tabelas
```
Transação: SE16N
→ Informar o nome da tabela
→ Definir filtros pelos campos-chave
→ Selecionar campos de saída
→ Executar (F8)
```

### SE11 — Para entender a estrutura de uma tabela
```
Transação: SE11
→ Informar nome da tabela
→ Ver campos, tipos, comprimentos e chaves primárias
→ Ver relacionamentos (Foreign Keys)
```

### ST05 — SQL Trace (para ver queries que o SAP executa)
```
Transação: ST05
→ Ativar o trace
→ Executar a transação desejada
→ Desativar e visualizar o SQL gerado
→ Revela EXATAMENTE quais tabelas e filtros são usados
```

> 💡 **Dica de ouro:** Use o ST05 para "espionar" o que o SAP faz internamente quando você executa uma transação. É a melhor forma de descobrir quais tabelas usar para uma extração específica.

---

## ✅ Checklist de Aprendizado — Módulo 2

Ao final deste módulo, você deve ser capaz de:

- [ ] Descrever o fluxo P2P (MM): PR → PO → GR → Invoice → Pagamento
- [ ] Descrever o fluxo O2C (SD): Cotação → SO → Delivery → Billing → Recebimento
- [ ] Identificar as tabelas de cada etapa: EKKO/EKPO, MKPF/MSEG, RBKP/RSEG, VBAK/VBAP, LIKP/LIPS, VBRK/VBRP
- [ ] Explicar como o FI recebe lançamentos automáticos dos outros módulos
- [ ] Usar as views de BSEG (BSIK, BSAK, BSID, BSAD) em vez da tabela direta
- [ ] Construir queries de rastreio ponta a ponta entre módulos
- [ ] Usar ST05 para descobrir quais tabelas uma transação SAP consulta

---

## 📚 Próximo Módulo

➡️ **Módulo 3 — Avançado:** Change Documents, rastreabilidade de alterações, extração com JOINs complexos, IDocs, BAPIs, e introdução ao SAP BW como camada analítica.

---

*Documento gerado para uso interno — Programa de Capacitação SAP*
*Versão 1.0 | Módulo 2 de 4*
