# 📒 SAP — Módulo 7: SAP BTP e APIs OData
### Extração de Dados para Cloud

---

> **Objetivo do módulo:** Entender a plataforma SAP BTP (Business Technology Platform), como consumir dados SAP via APIs OData, usar o Integration Suite para orquestrar fluxos de dados e conectar o SAP ao ecossistema cloud.

---

## 1. O que é SAP BTP?

**SAP BTP (Business Technology Platform)** é a plataforma cloud da SAP que combina:

```
SAP BTP
├── Application Development    → Desenvolvimento de apps cloud-native
├── Integration Suite          → iPaaS — integração de sistemas
├── Data & Analytics           → SAP HANA Cloud, Datasphere, SAC
├── AI & Machine Learning      → AI Foundation, Document Intelligence
├── Automation                 → SAP Build Process Automation (RPA)
└── Extension Suite            → Extensões ao S/4HANA sem modificações
```

### 1.1 Por que o BTP importa para dados?

```
SAP S/4HANA (on-premise ou cloud)
        ↓  OData APIs
SAP BTP Integration Suite
        ↓  Conectores nativos
Plataformas de dados externas
  ├── Azure Data Lake Storage
  ├── Databricks
  ├── Snowflake
  └── AWS S3
```

O BTP é a **camada oficial SAP** para expor e consumir dados SAP em arquiteturas modernas de dados.

---

## 2. OData — O Protocolo de Dados SAP

### 2.1 O que é OData?

**OData (Open Data Protocol)** é um protocolo REST padronizado pela OASIS, baseado em HTTP. O SAP adotou OData como o **protocolo padrão de exposição de dados** no S/4HANA e BTP.

```
Cliente (ADF, Python, Power BI...)
        ↓  HTTP GET/POST/PATCH/DELETE
OData Service (SAP Gateway / BTP)
        ↓
CDS View / BAPI / Function Module
        ↓
Dados SAP (tabelas)
```

### 2.2 Anatomia de uma URL OData

```
https://host:port/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV
│                               │   │   └── Nome do serviço
│                               │   └── Namespace (sap = padrão SAP)
│                               └── Prefixo OData SAP
└── Host SAP Gateway

Exemplos de endpoints:

# Lista de entidades (EntitySet)
GET /API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrder

# Registro específico (EntityKey)
GET /API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrder('4500000001')

# Entidade relacionada (Navigation)
GET /API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrder('4500000001')/to_PurchaseOrderItem

# Metadados do serviço
GET /API_PURCHASEORDER_PROCESS_SRV/$metadata
```

### 2.3 Parâmetros de query OData

| Parâmetro | Função | Exemplo |
|-----------|--------|---------|
| `$filter` | Filtro de registros | `$filter=CompanyCode eq 'BRAS'` |
| `$select` | Campos a retornar | `$select=PurchaseOrder,Supplier,NetPaymentDays` |
| `$expand` | Expande navegação | `$expand=to_PurchaseOrderItem` |
| `$top` | Limite de registros | `$top=1000` |
| `$skip` | Paginação (offset) | `$skip=1000` |
| `$orderby` | Ordenação | `$orderby=CreationDate desc` |
| `$count` | Contar registros | `$count=true` |
| `$format` | Formato de resposta | `$format=json` |

```
# Exemplo completo: POs do fornecedor X, com itens, últimas 100
GET /API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrder
  ?$filter=Supplier eq 'FORN001' and CompanyCode eq 'BRAS'
  &$select=PurchaseOrder,Supplier,PurchaseOrderDate,NetAmount
  &$expand=to_PurchaseOrderItem($select=PurchaseOrderItem,Material,OrderQuantity)
  &$top=100
  &$orderby=PurchaseOrderDate desc
  &$format=json
```

### 2.4 Consumindo OData via Python

```python
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

class SAPODataClient:
    """Cliente para APIs OData SAP"""

    def __init__(self, base_url: str, user: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.auth = HTTPBasicAuth(user, password)
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def _get_csrf_token(self, service_url: str) -> str:
        """CSRF token obrigatório para operações de escrita (POST/PATCH/DELETE)"""
        response = requests.get(
            service_url + '/$metadata',
            auth=self.auth,
            headers={'x-csrf-token': 'fetch'}
        )
        return response.headers.get('x-csrf-token', '')

    def get_all_pages(
        self,
        entity_set: str,
        service: str,
        filters: str = None,
        select: str = None,
        expand: str = None,
        page_size: int = 1000
    ) -> pd.DataFrame:
        """Extrai todos os registros com paginação automática"""

        url = f"{self.base_url}/sap/opu/odata/sap/{service}/{entity_set}"
        params = {
            '$format': 'json',
            '$top': page_size,
            '$skip': 0,
            '$count': 'true'
        }

        if filters: params['$filter'] = filters
        if select:  params['$select'] = select
        if expand:  params['$expand'] = expand

        all_records = []
        total = None

        while True:
            response = requests.get(
                url,
                params=params,
                auth=self.auth,
                headers=self.headers,
                timeout=120
            )
            response.raise_for_status()
            data = response.json()

            # Extrair registros
            results = data.get('d', {}).get('results', [])
            all_records.extend(results)

            # Verificar total na primeira chamada
            if total is None:
                total = int(data.get('d', {}).get('__count', 0))
                print(f"Total de registros: {total}")

            print(f"Extraídos: {len(all_records)} / {total}")

            # Verificar se há mais páginas
            if len(all_records) >= total or not results:
                break

            params['$skip'] += page_size

        return pd.DataFrame(all_records)


# Uso
client = SAPODataClient(
    base_url='https://my-sap-system.example.com',
    user='EXTRACTOR_USER',
    password='senha_segura'
)

# Extrair Purchase Orders
df_po = client.get_all_pages(
    entity_set='A_PurchaseOrder',
    service='API_PURCHASEORDER_PROCESS_SRV',
    filters="CompanyCode eq 'BRAS' and PurchaseOrderDate ge datetime'2024-01-01T00:00:00'",
    select='PurchaseOrder,Supplier,PurchaseOrderDate,NetAmount,DocumentCurrency',
    expand='to_PurchaseOrderItem',
    page_size=500
)

print(df_po.head())
df_po.to_parquet('purchase_orders.parquet', index=False)
```

---

## 3. SAP API Business Hub — Catálogo de APIs

### 3.1 O que é o API Business Hub?

O **SAP API Business Hub** (api.sap.com) é o catálogo oficial de todas as APIs SAP:

```
api.sap.com
├── APIs OData (S/4HANA, BTP, ECC)
├── Documentação completa de cada entidade e campo
├── Sandbox para teste sem sistema SAP
├── Geração de código em múltiplas linguagens
└── Especificações OpenAPI/Swagger para download
```

### 3.2 APIs OData mais relevantes para dados

#### Compras (MM)

| API | Serviço | Conteúdo |
|-----|---------|----------|
| Purchase Order | `API_PURCHASEORDER_PROCESS_SRV` | POs, itens, condições |
| Purchase Requisition | `API_PURCHASEREQ_PROCESS_SRV` | PRs e itens |
| Goods Movement | `API_MATERIAL_DOCUMENT_SRV` | Movimentos de material |
| Supplier Invoice | `API_SUPPLIERINVOICE_PROCESS_SRV` | Faturas de fornecedor |
| Material Stock | `API_MATERIAL_STOCK_SRV` | Posição de estoque |

#### Vendas (SD)

| API | Serviço | Conteúdo |
|-----|---------|----------|
| Sales Order | `API_SALES_ORDER_SRV` | Pedidos de venda |
| Delivery | `API_OUTBOUND_DELIVERY_SRV` | Remessas |
| Billing | `API_BILLING_DOCUMENT_SRV` | Faturamento |
| Customer | `API_BUSINESS_PARTNER` | Clientes e fornecedores |

#### Financeiro (FI)

| API | Serviço | Conteúdo |
|-----|---------|----------|
| Journal Entry | `API_JOURNALENTRYITEMBASIC_SRV` | Lançamentos contábeis |
| GL Account | `API_GLACCOUNTINCHARTOFACCOUNTS_SRV` | Plano de contas |

---

## 4. SAP Integration Suite

### 4.1 O que é o Integration Suite?

O **SAP Integration Suite** (antigo SAP Cloud Platform Integration — CPI) é o **iPaaS** (Integration Platform as a Service) da SAP. Permite criar fluxos de integração entre SAP e sistemas externos sem código extenso.

```
SAP S/4HANA
    ↓  (OData, RFC, IDoc, SOAP)
SAP Integration Suite
    ├── Message Mapping
    ├── Transformação de dados
    ├── Roteamento de mensagens
    ├── Error handling
    └── Monitoramento
    ↓  (REST, SFTP, AMQP, Kafka, JDBC...)
Sistema de destino
  ├── Azure Data Lake
  ├── Databricks
  ├── Salesforce
  └── Qualquer REST API
```

### 4.2 Componentes principais

| Componente | Função |
|------------|--------|
| **Cloud Integration** | Orquestração de mensagens e ETL |
| **API Management** | Gateway e governança de APIs |
| **Open Connectors** | +170 conectores pré-construídos |
| **Integration Advisor** | Mapeamento B2B automatizado |
| **Event Mesh** | Publish/Subscribe para eventos SAP |

### 4.3 Padrão de iFlow — SAP para Azure Data Lake

```
[SAP S/4HANA] → OData Poll
      ↓
[Cloud Integration iFlow]
      ├── Timer Start (agendamento)
      ├── Request Reply → OData (buscar dados)
      ├── Message Mapping (transformar para JSON/CSV)
      ├── Filter (remover registros irrelevantes)
      └── Request Reply → Azure Blob Storage (HTTP adapter)
      ↓
[Azure Data Lake Storage Gen2]
      ↓
[Azure Data Factory / Databricks]
```

### 4.4 Event-Driven com SAP Event Mesh

Para dados em tempo real (near-real-time), o SAP oferece o **Event Mesh**:

```
SAP S/4HANA
  └── Business Events (eventos de negócio)
        ├── PurchaseOrderChanged
        ├── GoodsMovementCreated
        ├── SalesOrderCreated
        └── InvoicePosted
        ↓  (publica no Event Mesh)
SAP Event Mesh (Message Broker)
        ↓  (subscribe)
  ├── Azure Event Hub → Databricks Streaming
  ├── Azure Service Bus → Azure Functions
  └── Kafka → Qualquer consumidor
```

```python
# Consumidor Python para SAP Event Mesh
import json
from azure.servicebus import ServiceBusClient

# Após SAP Event Mesh rotear para Azure Service Bus
conn_str = "Endpoint=sb://..."
queue_name = "sap-goods-movement"

with ServiceBusClient.from_connection_string(conn_str) as client:
    with client.get_queue_receiver(queue_name) as receiver:
        for msg in receiver:
            event = json.loads(str(msg))

            # Evento de movimentação de material
            print(f"Movimento: {event['bwart']} | Material: {event['matnr']}")
            print(f"Planta: {event['werks']} | Quantidade: {event['menge']}")

            receiver.complete_message(msg)
```

---

## 5. OData Delta Queries — Extração Incremental

### 5.1 O que são Delta Tokens?

Para extração incremental via OData, o SAP suporta **Delta Tokens** — um mecanismo para buscar apenas registros criados/alterados desde a última extração.

```
1ª extração (Full):
  GET /A_PurchaseOrder?$deltatoken=initial
  Resposta: { "results": [...], "@odata.deltaLink": "?$deltatoken=abc123" }
        ↓
  Salvar o deltatoken: "abc123"

2ª extração (Delta):
  GET /A_PurchaseOrder?$deltatoken=abc123
  Resposta: { "results": [apenas novos/alterados], "@odata.deltaLink": "?$deltatoken=xyz789" }
        ↓
  Atualizar deltatoken: "xyz789"
```

```python
import requests
import json
from pathlib import Path

TOKEN_FILE = Path('delta_tokens.json')

def load_tokens() -> dict:
    if TOKEN_FILE.exists():
        return json.loads(TOKEN_FILE.read_text())
    return {}

def save_token(entity: str, token: str):
    tokens = load_tokens()
    tokens[entity] = token
    TOKEN_FILE.write_text(json.dumps(tokens))

def extract_delta(entity_set: str, service: str, auth) -> list:
    tokens = load_tokens()
    entity_key = f"{service}/{entity_set}"

    # Determinar se é full load ou delta
    delta_token = tokens.get(entity_key, 'initial')

    url = f"https://sap-host/sap/opu/odata/sap/{service}/{entity_set}"
    params = {
        '$format': 'json',
        '$deltatoken': delta_token
    }

    response = requests.get(url, params=params, auth=auth)
    data = response.json()

    results = data['d']['results']

    # Salvar novo deltatoken para próxima execução
    next_token = data['d'].get('@odata.deltaLink', '').split('$deltatoken=')[-1]
    if next_token:
        save_token(entity_key, next_token)

    return results

# Uso
from requests.auth import HTTPBasicAuth
auth = HTTPBasicAuth('user', 'password')

new_pos = extract_delta('A_PurchaseOrder', 'API_PURCHASEORDER_PROCESS_SRV', auth)
print(f"{len(new_pos)} registros novos/alterados")
```

---

## 6. Segurança e Governança de APIs SAP

### 6.1 Autenticação

| Método | Quando usar |
|--------|-------------|
| **Basic Auth** | Desenvolvimento/teste apenas ⚠️ |
| **OAuth 2.0 (Client Credentials)** | Integração sistema-a-sistema ✅ |
| **OAuth 2.0 (SAML Bearer)** | Usuário delegado ✅ |
| **mTLS (certificado)** | Ambientes críticos ✅ |

### 6.2 Configuração OAuth no BTP

```python
import requests

def get_oauth_token(
    token_url: str,
    client_id: str,
    client_secret: str
) -> str:
    """Obtém token OAuth 2.0 para chamadas SAP BTP"""
    response = requests.post(
        token_url,
        data={
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret
        }
    )
    response.raise_for_status()
    return response.json()['access_token']

# Uso
token = get_oauth_token(
    token_url='https://my-btp-tenant.authentication.eu10.hana.ondemand.com/oauth/token',
    client_id='sb-my-app!t12345',
    client_secret='secret_here'
)

# Usar token nas chamadas OData
headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
response = requests.get(odata_url, headers=headers)
```

### 6.3 Usuário de extração SAP — Perfil mínimo recomendado

```
Usuário tipo: "System" (não Dialog)
Roles obrigatórias:
  - SAP_BC_BASIS_MAPPING_RFC     → Acesso RFC básico
  - Z_EXTRACTOR_READ_ONLY        → Role customizada com:
      • S_RFC: Acesso às BAPIs/FMs usadas
      • S_TABU_DIS: Leitura das tabelas mapeadas
      • Apenas autorização de leitura (Display)

Restrições:
  - Sem acesso a telas (tipo "System")
  - IP restrito ao servidor de extração
  - Senha com rotação automática (via SAP Credential Store no BTP)
```

---

## ✅ Checklist de Aprendizado — Módulo 7

- [ ] Entender a arquitetura do SAP BTP e seus serviços principais
- [ ] Construir URLs OData com $filter, $select, $expand, $top e $skip
- [ ] Implementar paginação automática em cliente Python OData
- [ ] Localizar APIs no SAP API Business Hub e ler a documentação
- [ ] Entender o Integration Suite como camada de orquestração
- [ ] Implementar extração incremental com Delta Tokens
- [ ] Configurar autenticação OAuth 2.0 para chamadas ao BTP
- [ ] Entender o modelo de segurança para usuários de extração SAP

---

## 📚 Próximo Módulo

➡️ **Módulo 8 — Integração com Azure:** ADF + SAP Connector, Databricks + pyrfc, e construção de pipelines Delta Lake com dados SAP.

---

*Documento gerado para uso interno — Programa de Capacitação SAP*
*Versão 1.0 | Módulo 7 de 8*
