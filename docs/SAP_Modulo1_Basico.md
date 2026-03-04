# 📘 SAP — Módulo 1: Básico
### Programa de Estudo: Zero to Hero em Dados SAP

---

> **Objetivo do módulo:** Entender o ecossistema SAP, como ele organiza sua estrutura organizacional e como os dados são classificados, armazenados e relacionados internamente.

---

## 1. O que é SAP?

**SAP** (Systems, Applications and Products in Data Processing) é o maior sistema ERP (*Enterprise Resource Planning*) do mundo, desenvolvido pela empresa alemã SAP SE.

Um ERP centraliza todos os processos de negócio de uma empresa em um único sistema integrado, eliminando silos de informação entre departamentos.

### Por que SAP importa para dados?
- Cada transação realizada no SAP **gera registros em tabelas de banco de dados**
- Todas as áreas da empresa (compras, vendas, finanças, produção) **compartilham os mesmos dados mestres**
- O dado nasce uma vez e é reutilizado por todos os módulos — princípio do **"single source of truth"**

### Principais módulos SAP

| Sigla | Nome | O que faz |
|-------|------|-----------|
| **MM** | Materials Management | Compras, estoque, recebimento de mercadorias |
| **SD** | Sales & Distribution | Vendas, entregas, faturamento |
| **FI** | Financial Accounting | Contabilidade, contas a pagar/receber |
| **CO** | Controlling | Custos, centros de custo, lucratividade |
| **PP** | Production Planning | Planejamento e ordens de produção |
| **QM** | Quality Management | Inspeções e controle de qualidade |
| **PM** | Plant Maintenance | Manutenção de equipamentos |
| **HR/HCM** | Human Resources | Folha de pagamento, gestão de pessoas |
| **WM/EWM** | Warehouse Management | Gestão avançada de armazém |

---

## 2. Conceito de Mandante (Client)

O **Mandante** (em inglês: *Client*) é o conceito mais fundamental da arquitetura SAP.

### O que é?
É uma **unidade organizacional independente** dentro do sistema SAP. Cada mandante tem seus próprios dados, configurações e usuários — completamente isolados dos demais.

```
Sistema SAP
├── Mandante 100  →  Produção (PRD)
├── Mandante 200  →  Qualidade (QAS)
└── Mandante 300  →  Desenvolvimento (DEV)
```

### Regra de ouro
> ⚠️ **Dados de um mandante NUNCA são visíveis em outro mandante.** Isso garante isolamento total entre ambientes.

### Na prática para dados
- Ao extrair dados, você sempre extrai **dentro de um mandante específico**
- A coluna `MANDT` aparece como **primeira coluna em praticamente todas as tabelas SAP**
- Em queries SQL/ABAP, o filtro `WHERE MANDT = '100'` é padrão implícito

---

## 3. Estrutura Organizacional SAP

A estrutura organizacional do SAP define **como a empresa está configurada** dentro do sistema. Ela impacta diretamente como os dados são segmentados e relacionados.

### Visão hierárquica

```
Client (Mandante)
└── Company Code (Empresa)            ← Entidade legal/fiscal
    └── Controlling Area (Área CO)    ← Agrupamento de custos
        └── Plant (Planta)            ← Unidade fabril/logística
            ├── Storage Location      ← Local físico de estoque
            └── Sales Organization    ← Estrutura comercial
                └── Distribution Channel
                    └── Division
```

### Detalhamento de cada nível

#### 🏢 Company Code — Empresa
- Representa uma **entidade jurídica** (CNPJ no Brasil)
- Todos os documentos financeiros (NF, pagamentos) são gerados por Company Code
- Tabela relevante: **T001**
- Exemplo: `BRAS` = Braskem Brasil

#### 🏭 Plant — Planta
- Representa uma **unidade de negócio física** (fábrica, CD, filial)
- É o nível onde ocorre: recebimento de materiais, produção, expedição
- Tabela relevante: **T001W**
- Exemplo: `1000` = Planta Camaçari

#### 📦 Storage Location — Depósito
- Subdivisão da planta para **controle físico de estoque**
- Tabela relevante: **T001L**
- Exemplo: `0001` = Armazém Principal

#### 💰 Sales Organization — Organização de Vendas
- Responsável pela venda de produtos a clientes
- Define condições comerciais, moeda, incoterms
- Tabela relevante: **TVKO**

---

## 4. Tipos de Dados no SAP

Todo dado no SAP se enquadra em duas grandes categorias:

### 📋 Dados Mestres (*Master Data*)

São dados **estáticos e de referência**, criados uma vez e reutilizados em múltiplas transações.

| Tipo | Descrição | Transação | Tabela Principal |
|------|-----------|-----------|-----------------|
| Material Master | Cadastro de produtos/materiais | MM03 | MARA, MAKT |
| Customer Master | Cadastro de clientes | XD03 | KNA1, KNB1 |
| Vendor Master | Cadastro de fornecedores | XK03 | LFA1, LFB1 |
| Chart of Accounts | Plano de contas contábil | FS03 | SKA1, SKB1 |
| Cost Center | Centro de custo | KS03 | CSKS |

> 💡 **Regra prática:** Dados mestres têm **validade longa** e mudam com pouca frequência. São a espinha dorsal de qualquer extração analítica.

### 📄 Dados Transacionais (*Transactional Data*)

São dados **gerados pelo movimento do negócio**, criados a cada operação realizada.

| Tipo | Descrição | Exemplo |
|------|-----------|---------|
| Purchase Order | Ordem de compra gerada pelo Compras | PO 4500012345 |
| Goods Movement | Movimentação de estoque | Entrada de 100 kg de produto X |
| Sales Order | Pedido de venda do cliente | SO 0000123456 |
| FI Document | Lançamento contábil | Pagamento de NF fornecedor |
| Production Order | Ordem de produção | OP 000100001234 |

> 💡 **Regra prática:** Dados transacionais são **volumosos, imutáveis e rastreáveis**. Todo documento tem número, data, usuário e timestamp.

---

## 5. Principais Dados Mestres em Detalhe

### 📦 Material Master — O mais importante

O Material Master é o dado mestre mais complexo do SAP. Ele possui **visões (views)** organizadas por módulo:

```
Material Master (MATNR = número do material)
├── Basic Data 1 & 2     → Descrição, unidade base, grupo de material
├── Classification        → Características técnicas
├── Sales Views           → Dados de venda (org. vendas, divisão)
├── MRP Views             → Parâmetros de planejamento
├── Plant Data / Storage  → Dados por planta e depósito
├── Purchasing View       → Dados de compra (grupo de compras)
├── Accounting View       → Preço padrão, preço médio
└── Costing View          → Dados de custeio
```

**Tabelas principais do Material Master:**

| Tabela | Conteúdo |
|--------|----------|
| `MARA` | Dados gerais do material (nível cliente) |
| `MAKT` | Descrição/texto do material |
| `MARC` | Dados por planta (MRP, produção) |
| `MARD` | Estoque por depósito |
| `MBEW` | Avaliação/valorização do material |
| `MVKE` | Dados de venda (org. vendas) |

### 👤 Customer Master

```
Customer Master (KUNNR = número do cliente)
├── General Data     → Nome, endereço, CNPJ (tabela KNA1)
├── Company Code     → Conta contábil, condições pagamento (KNB1)
└── Sales Area       → Incoterms, grupo cliente (KNVV)
```

### 🏪 Vendor Master

```
Vendor Master (LIFNR = número do fornecedor)
├── General Data     → Nome, endereço, CNPJ (tabela LFA1)
├── Company Code     → Conta contábil, condições pagamento (LFB1)
└── Purchasing Org   → Dados de compra por org. compras (LFM1)
```

---

## 6. Navegação Básica — Transações Essenciais

No SAP, toda ação é executada através de **transações** (códigos de 4 letras digitados no campo de comando).

### Transações de visualização de dados

| Transação | Função |
|-----------|--------|
| `SE16` | Browser de tabelas (visualizar conteúdo de qualquer tabela) |
| `SE16N` | Versão melhorada do SE16 |
| `SE11` | ABAP Dictionary — ver estrutura das tabelas |
| `SE37` | Function Modules |
| `MM03` | Visualizar Material Master |
| `XD03` | Visualizar Customer Master |
| `XK03` | Visualizar Vendor Master |
| `ME23N` | Visualizar Purchase Order |
| `VA03` | Visualizar Sales Order |
| `MB03` | Visualizar documento de movimentação de material |

### Como usar o SE16N (essencial para dados)

```
1. Digite SE16N no campo de comando → Enter
2. No campo "Table": informe o nome da tabela (ex: MARA)
3. Clique em "Display Content" (óculos)
4. Use os filtros disponíveis (MANDT, campos-chave)
5. Execute (F8)
```

> ⚠️ **Atenção:** Sempre filtre por `MANDT` e pelos campos-chave. Tabelas como BSEG podem ter **bilhões de registros** em produção — um SELECT sem filtro pode travar o sistema.

---

## 7. Tabelas Fundamentais — Guia de Referência

### Tabelas de Configuração / Customizing

| Tabela | Descrição |
|--------|-----------|
| `T001` | Company Codes (Empresas) |
| `T001W` | Plants (Plantas) |
| `T001L` | Storage Locations (Depósitos) |
| `TVKO` | Sales Organizations |
| `T023` | Material Groups (Grupos de material) |
| `T156` | Movement Types (Tipos de movimento de estoque) |

### Tabelas de Dados Mestres

| Tabela | Módulo | Descrição |
|--------|--------|-----------|
| `MARA` | MM | Material — dados gerais |
| `MARC` | MM | Material — dados por planta |
| `MARD` | MM | Material — estoque por depósito |
| `KNA1` | SD | Cliente — dados gerais |
| `LFA1` | MM | Fornecedor — dados gerais |
| `SKA1` | FI | Plano de contas |
| `CSKS` | CO | Centro de custo |

### Tabelas de Dados Transacionais

| Tabela | Módulo | Descrição |
|--------|--------|-----------|
| `EKKO` | MM | Cabeçalho de Purchase Order |
| `EKPO` | MM | Itens de Purchase Order |
| `MSEG` | MM | Segmentos de movimentação de material |
| `MKPF` | MM | Cabeçalho de documento de material |
| `VBAK` | SD | Cabeçalho de Sales Order |
| `VBAP` | SD | Itens de Sales Order |
| `BKPF` | FI | Cabeçalho de documento contábil |
| `BSEG` | FI | Segmentos de documento contábil |
| `AUFK` | PP | Cabeçalho de ordem de produção |

---

## 8. Conceitos-Chave para Extração de Dados

### Chaves primárias no SAP

Todo registro SAP é identificado por uma **chave primária composta**. Exemplo:

```
Tabela EKPO (Itens de PO):
  Chave = MANDT + EBELN (nº PO) + EBELP (nº item)

Tabela VBAP (Itens de Sales Order):
  Chave = MANDT + VBELN (nº documento) + POSNR (nº item)
```

### Relacionamento Cabeçalho x Item

O SAP segue consistentemente o padrão **Header/Item**:

```
EKKO (Cabeçalho PO)  ←→  EKPO (Itens PO)
  EBELN = EBELN

VBAK (Cabeçalho SO)  ←→  VBAP (Itens SO)
  VBELN = VBELN

BKPF (Cabeçalho FI)  ←→  BSEG (Itens FI)
  BUKRS + BELNR + GJAHR = BUKRS + BELNR + GJAHR
```

### O campo MANDT em todas as tabelas

```sql
-- Exemplo de JOIN correto entre tabelas SAP
SELECT
    a.EBELN,   -- Número da PO
    a.LIFNR,   -- Fornecedor
    b.MATNR,   -- Material
    b.MENGE,   -- Quantidade
    b.NETPR    -- Preço líquido
FROM EKKO a
JOIN EKPO b
  ON a.MANDT = b.MANDT
  AND a.EBELN = b.EBELN
WHERE a.MANDT = '100'
  AND a.AEDAT >= '20240101'  -- Data de alteração
```

---

## ✅ Checklist de Aprendizado — Módulo 1

Ao final deste módulo, você deve ser capaz de:

- [ ] Explicar o que é SAP e citar os principais módulos
- [ ] Entender o conceito de Mandante e sua importância para dados
- [ ] Descrever a estrutura organizacional: Client → Company Code → Plant → Storage Location
- [ ] Diferenciar Dados Mestres de Dados Transacionais
- [ ] Identificar as tabelas principais: MARA, KNA1, LFA1, EKKO, VBAK, BKPF
- [ ] Navegar no SAP usando transações SE16/SE16N para visualizar dados
- [ ] Construir um JOIN simples entre tabelas cabeçalho e item

---

## 📚 Próximo Módulo

➡️ **Módulo 2 — Intermediário:** Fluxo de dados por módulo (MM, SD, FI/CO, PP), documentos gerados em cada processo e como extrair dados de ponta a ponta de uma cadeia de negócio.

---

*Documento gerado para uso interno — Programa de Capacitação SAP*
*Versão 1.0 | Módulo 1 de 4*
