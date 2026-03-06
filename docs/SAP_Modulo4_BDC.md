# 📗 SAP — Módulo 4: BDC (Batch Data Communication)
### Programa de Estudo: Zero to Hero em Dados SAP

---

> **Objetivo do módulo:** Dominar o mecanismo de entrada de dados em massa no SAP via simulação de telas (BDC), entender quando usá-lo, como gravá-lo, programá-lo em ABAP e tratar erros de forma robusta em cargas massivas.

---

## 1. O que é BDC?

**BDC (Batch Data Communication)** é a técnica de entrada de dados em massa no SAP através da **simulação de interações de tela**. O programa BDC "digita" os dados automaticamente, navegando pelas mesmas telas que um usuário usaria manualmente.

### Como funciona

```
Arquivo externo (CSV / planilha / tabela)
        ↓
Programa ABAP BDC
        ↓  (simula pressionamento de teclas e preenchimento de campos)
Transação SAP (ex: ME21N, MM01, VA01...)
        ↓
Dados criados/alterados no SAP com validação completa das telas
```

### Por que o BDC valida os dados?

Diferente de um INSERT direto no banco de dados, o BDC **passa por todas as validações de negócio** que a transação SAP aplica — obrigatoriedade de campos, regras de consistência, autorizações. Isso torna o BDC mais seguro, porém mais lento que uma BAPI.

---

## 2. BDC vs. Outras Técnicas de Carga — Quando Usar Cada Uma

| Técnica | Velocidade | Validação | Complexidade | Melhor uso |
|---------|-----------|-----------|--------------|------------|
| **BDC** | Lenta | Total (tela) | Média | Quando não existe BAPI; replicar ação humana exata |
| **BAPI** | Rápida | Total (negócio) | Baixa | Sempre que existir BAPI disponível |
| **IDoc** | Rápida | Total | Alta | Integração entre sistemas, EDI |
| **LSMW** | Média | Total (tela) | Baixa | Migrações one-time sem programação |
| **Direct Input** | Muito rápida | Parcial | Alta | Cargas massivas iniciais (go-live) |
| **INSERT via ABAP** | Muito rápida | **Nenhuma** | Baixa | ⚠️ Nunca em produção — bypass completo |

### Regra de decisão

```
Existe BAPI para o processo?
  ├── SIM → Use a BAPI (mais rápido, mais simples)
  └── NÃO → Existe IDoc?
              ├── SIM → Use IDoc (se for integração entre sistemas)
              └── NÃO → Use BDC
```

---

## 3. Gravação de Sessão com SHDB

O ponto de partida de qualquer BDC é a **gravação da transação** usando o **SHDB** (Screen Recorder). O SAP registra automaticamente cada tela visitada e cada campo preenchido.

### Passo a passo do SHDB

```
1. Acesse a transação: SHDB
2. Clique em "New Recording"
3. Informe:
   - Recording Name: Z_MM01_CRIACAO_MATERIAL (sugestão de nomenclatura)
   - Transaction Code: MM01 (ou qualquer transação alvo)
4. Clique em "Start Recording"
   → O SAP abre a transação normalmente
5. Execute o processo COMPLETO com dados reais de exemplo
   → Preencha todos os campos necessários, navegue pelas telas
6. Ao finalizar, clique em "Stop Recording"
7. O SAP exibe a lista de telas e campos gravados
```

### O que o SHDB captura

```
Tela 1: SAPMM60M / 0100
  Campo: RMMG1-MATNR  →  "MAT-000001"  (número do material)
  Campo: RMMG1-MBRSH  →  "C"           (setor)
  Campo: RMMG1-MTART  →  "ROH"         (tipo de material)
  [ENTER]

Tela 2: SAPLMGMM / 0070
  Campo: MSICHTAUSW-DYTXT(01) →  "Basic Data 1"   (view selecionada)
  [ENTER]

Tela 3: SAPLMGMM / 4004
  Campo: MAKT-MAKTX  →  "Matéria Prima Teste"   (descrição)
  Campo: MARA-MEINS  →  "KG"                    (unidade base)
  [SAVE]
```

> 💡 **Dica:** Use dados **representativos** na gravação — escolha um exemplo que passe por todas as telas que os dados reais também passarão (ex: se alguns materiais têm visão de Compras, grave com essa visão ativa).

---

## 4. Estrutura do Programa BDC

### 4.1 A tabela BDCDATA

`BDCDATA` é a estrutura interna central do BDC. Cada linha representa uma **ação**: abrir uma tela ou preencher um campo.

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `PROGRAM` | CHAR 8 | Nome do programa da tela (ex: SAPMM60M) |
| `DYNPRO` | NUMC 4 | Número da tela/dynpro (ex: 0100) |
| `DYNBEGIN` | CHAR 1 | `X` = início de nova tela; ` ` = campo |
| `FNAM` | CHAR 132 | Nome do campo na tela |
| `FVAL` | CHAR 132 | Valor a preencher no campo |

### 4.2 Padrão de preenchimento da BDCDATA

```abap
" ============================================================
" Padrão obrigatório para cada tela:
" 1. Linha de cabeçalho da tela (DYNBEGIN = 'X')
" 2. Linhas de campos (DYNBEGIN = ' ')
" 3. Linha de ação (BDC_OKCODE = tecla pressionada)
" ============================================================

" Macro auxiliar para simplificar o preenchimento
DEFINE bdc_field.
  CLEAR gs_bdcdata.
  gs_bdcdata-fnam = &1.
  gs_bdcdata-fval = &2.
  APPEND gs_bdcdata TO gt_bdcdata.
END-OF-DEFINITION.

DEFINE bdc_dynpro.
  CLEAR gs_bdcdata.
  gs_bdcdata-program  = &1.
  gs_bdcdata-dynpro   = &2.
  gs_bdcdata-dynbegin = 'X'.
  APPEND gs_bdcdata TO gt_bdcdata.
END-OF-DEFINITION.

" ============================================================
" Exemplo: Criação de Material (MM01)
" ============================================================
bdc_dynpro 'SAPMM60M'    '0100'.          " Tela inicial
  bdc_field 'RMMG1-MATNR' lv_matnr.      " Número do material
  bdc_field 'RMMG1-MBRSH' lv_setor.      " Setor
  bdc_field 'RMMG1-MTART' lv_tipo.       " Tipo de material
  bdc_field 'BDC_OKCODE'  '/00'.         " ENTER

bdc_dynpro 'SAPLMGMM'    '0070'.          " Seleção de views
  bdc_field 'MSICHTAUSW-DYTXT(01)' 'X'.  " View: Basic Data 1
  bdc_field 'BDC_OKCODE'           '/00'. " ENTER

bdc_dynpro 'SAPLMGMM'    '4004'.          " Basic Data 1
  bdc_field 'MAKT-MAKTX'  lv_descricao.  " Descrição
  bdc_field 'MARA-MEINS'  lv_unidade.    " Unidade de medida
  bdc_field 'BDC_OKCODE'  '=MSIC'.       " SAVE (=BU ou =MSIC dependendo da transação)
```

### 4.3 OKCODEs mais comuns

| OKCODE | Ação equivalente |
|--------|-----------------|
| `/00` | ENTER |
| `=BACK` | F3 (voltar) |
| `=EXIT` | F15 (sair) |
| `=CANC` | F12 (cancelar) |
| `=BU` | Salvar (Save) — transações FI/MM |
| `=MSIC` | Salvar — Material Master |
| `=YES` | Confirmar popup |
| `=NO` | Negar popup |

---

## 5. CALL TRANSACTION vs. SESSION METHOD

São as duas formas de executar um BDC. Cada uma tem características distintas:

### 5.1 CALL TRANSACTION

Executa a transação **imediatamente, em tempo real**, registro por registro.

```abap
DATA: lv_mode   TYPE c VALUE 'N',   " N=background, A=foreground, E=erros
      lv_update TYPE c VALUE 'S',   " S=síncrono, A=assíncrono
      lt_msg    TYPE TABLE OF bdcmsgcoll.

CALL TRANSACTION lv_tcode
  USING    gt_bdcdata
  MODE     lv_mode
  UPDATE   lv_update
  MESSAGES INTO lt_msg.

" Verificar resultado
IF sy-subrc <> 0.
  " Houve erro — processar lt_msg para entender o motivo
  LOOP AT lt_msg INTO DATA(ls_msg).
    IF ls_msg-msgtyp = 'E' OR ls_msg-msgtyp = 'A'.
      " Log do erro
    ENDIF.
  ENDLOOP.
ENDIF.
```

#### Parâmetros de MODE

| Valor | Comportamento |
|-------|--------------|
| `'A'` | Foreground — abre telas para o usuário ver (debug/teste) |
| `'N'` | Background — executa sem exibir telas (produção) |
| `'E'` | Error — abre telas apenas quando há erro |

### 5.2 SESSION METHOD

Cria uma **sessão de BDC** que é processada posteriormente em background pelo SAP (via SM35).

```abap
" 1. Abrir a sessão
CALL FUNCTION 'BDC_OPEN_GROUP'
  EXPORTING
    client  = sy-mandt
    group   = 'Z_CARGA_MAT'    " Nome da sessão
    user    = sy-uname
    keep    = 'X'               " Manter sessão mesmo com sucesso
    holddate = sy-datum.

" 2. Para cada registro: inserir na sessão
CALL FUNCTION 'BDC_INSERT'
  EXPORTING
    tcode     = 'MM01'
  TABLES
    dynprotab = gt_bdcdata.

CLEAR gt_bdcdata.  " Limpar para o próximo registro

" 3. Fechar a sessão
CALL FUNCTION 'BDC_CLOSE_GROUP'.

" → Processar a sessão via SM35
```

### 5.3 Comparativo: Quando usar cada método

| Critério | CALL TRANSACTION | SESSION METHOD |
|----------|-----------------|----------------|
| Execução | Imediata | Agendada (SM35) |
| Volume | Pequeno / médio | Grande volume |
| Tratamento de erro | Linha a linha no código | Interface SM35 |
| Reprocessar erros | Manual (re-executar) | Sim, via SM35 |
| Rollback por registro | Sim | Sim |
| Performance | Mais lento | Mais eficiente |
| Melhor para | Até ~5.000 registros | Acima de ~5.000 registros |

---

## 6. Programa BDC Completo — Exemplo Real

Exemplo completo de carga de Purchase Orders via arquivo CSV:

```abap
*&---------------------------------------------------------------------*
*& Program: Z_BDC_ME21N_CARGA_PO
*& Descrição: Criação de Purchase Orders em massa via BDC
*&---------------------------------------------------------------------*
REPORT z_bdc_me21n_carga_po.

" -------------------------------------------------------
" Estruturas de dados
" -------------------------------------------------------
TYPES: BEGIN OF ty_input,
  lifnr TYPE ekko-lifnr,    " Fornecedor
  ekorg TYPE ekko-ekorg,    " Org. de compras
  ekgrp TYPE ekko-ekgrp,    " Grupo de compras
  bukrs TYPE ekko-bukrs,    " Company Code
  matnr TYPE ekpo-matnr,    " Material
  werks TYPE ekpo-werks,    " Planta
  menge TYPE ekpo-menge,    " Quantidade
  meins TYPE ekpo-meins,    " Unidade
  netpr TYPE ekpo-netpr,    " Preço unitário
  eindt TYPE ekpo-eindt,    " Data de entrega
END OF ty_input.

DATA: gt_input   TYPE TABLE OF ty_input,
      gt_bdcdata TYPE TABLE OF bdcdata,
      gs_bdcdata TYPE bdcdata,
      gt_msgs    TYPE TABLE OF bdcmsgcoll,
      gt_log     TYPE TABLE OF string.

" -------------------------------------------------------
" Tela de seleção
" -------------------------------------------------------
SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.
  PARAMETERS: p_file TYPE string OBLIGATORY,    " Caminho do arquivo
              p_mode TYPE c DEFAULT 'N',         " Modo BDC
              p_test TYPE c AS CHECKBOX.         " Modo teste
SELECTION-SCREEN END OF BLOCK b1.

" -------------------------------------------------------
" Macros BDC
" -------------------------------------------------------
DEFINE bdc_dynpro.
  CLEAR gs_bdcdata.
  gs_bdcdata-program  = &1.
  gs_bdcdata-dynpro   = &2.
  gs_bdcdata-dynbegin = 'X'.
  APPEND gs_bdcdata TO gt_bdcdata.
END-OF-DEFINITION.

DEFINE bdc_field.
  CLEAR gs_bdcdata.
  gs_bdcdata-fnam = &1.
  gs_bdcdata-fval = &2.
  APPEND gs_bdcdata TO gt_bdcdata.
END-OF-DEFINITION.

" -------------------------------------------------------
" START-OF-SELECTION
" -------------------------------------------------------
START-OF-SELECTION.

  " 1. Ler arquivo de entrada
  PERFORM ler_arquivo.

  " 2. Processar cada linha
  LOOP AT gt_input INTO DATA(ls_input).
    PERFORM montar_bdcdata USING ls_input.
    PERFORM executar_bdc   USING ls_input.
    CLEAR gt_bdcdata.
  ENDLOOP.

  " 3. Exibir log
  PERFORM exibir_log.

" -------------------------------------------------------
" FORM: Ler arquivo CSV
" -------------------------------------------------------
FORM ler_arquivo.
  DATA: lt_raw  TYPE TABLE OF string,
        lv_line TYPE string.

  " Upload do arquivo servidor/local
  CALL FUNCTION 'GUI_UPLOAD'
    EXPORTING
      filename = p_file
      filetype = 'ASC'
    TABLES
      data_tab = lt_raw
    EXCEPTIONS
      OTHERS   = 1.

  IF sy-subrc <> 0.
    MESSAGE 'Erro ao ler arquivo' TYPE 'E'.
  ENDIF.

  " Parser simples CSV (skip header)
  LOOP AT lt_raw INTO lv_line FROM 2.
    DATA(ls_input) = VALUE ty_input(
      lifnr = |{ lv_line }|+0(10)   " Ajustar offsets conforme layout
    ).
    " ... parse completo do CSV
    APPEND ls_input TO gt_input.
  ENDLOOP.
ENDFORM.

" -------------------------------------------------------
" FORM: Montar BDCDATA para ME21N
" -------------------------------------------------------
FORM montar_bdcdata USING ps_input TYPE ty_input.

  " Tela inicial da ME21N
  bdc_dynpro 'SAPMM06E' '0100'.
    bdc_field 'EKKO-LIFNR'  ps_input-lifnr.
    bdc_field 'EKKO-EKORG'  ps_input-ekorg.
    bdc_field 'EKKO-EKGRP'  ps_input-ekgrp.
    bdc_field 'EKKO-BUKRS'  ps_input-bukrs.
    bdc_field 'BDC_OKCODE'  '/00'.

  " Tela de item
  bdc_dynpro 'SAPMM06E' '0120'.
    bdc_field 'EKPO-MATNR(01)'  ps_input-matnr.
    bdc_field 'EKPO-WERKS(01)'  ps_input-werks.
    bdc_field 'EKPO-MENGE(01)'  ps_input-menge.
    bdc_field 'EKPO-MEINS(01)'  ps_input-meins.
    bdc_field 'EKPO-NETPR(01)'  ps_input-netpr.
    bdc_field 'EKPO-EINDT(01)'  ps_input-eindt.
    bdc_field 'BDC_OKCODE'      '=BU'.   " Salvar

ENDFORM.

" -------------------------------------------------------
" FORM: Executar BDC via CALL TRANSACTION
" -------------------------------------------------------
FORM executar_bdc USING ps_input TYPE ty_input.

  DATA: lv_ok TYPE string.

  IF p_test = 'X'.  " Modo teste: não executa
    APPEND |TESTE - Fornecedor: { ps_input-lifnr } / Material: { ps_input-matnr }| TO gt_log.
    RETURN.
  ENDIF.

  CLEAR gt_msgs.

  CALL TRANSACTION 'ME21N'
    USING    gt_bdcdata
    MODE     p_mode
    UPDATE   'S'
    MESSAGES INTO gt_msgs.

  " Avaliar resultado
  IF sy-subrc = 0.
    " Capturar número da PO gerada
    READ TABLE gt_msgs INTO DATA(ls_ok)
      WITH KEY msgtyp = 'S'
               msgid  = 'ME'
               msgnr  = '301'.   " Msg: "Standard PO created under number XXXXXXXX"
    IF sy-subrc = 0.
      lv_ok = ls_ok-msgv1.
      APPEND |✅ PO { lv_ok } criada - Fornecedor: { ps_input-lifnr }| TO gt_log.
    ENDIF.
  ELSE.
    " Capturar mensagem de erro
    LOOP AT gt_msgs INTO DATA(ls_err)
      WHERE msgtyp = 'E' OR msgtyp = 'A'.
      APPEND |❌ ERRO - Fornecedor: { ps_input-lifnr } / { ls_err-msgid }{ ls_err-msgnr }: { ls_err-msgv1 } { ls_err-msgv2 }| TO gt_log.
    ENDLOOP.
  ENDIF.

ENDFORM.

" -------------------------------------------------------
" FORM: Exibir log de execução
" -------------------------------------------------------
FORM exibir_log.
  LOOP AT gt_log INTO DATA(lv_linha).
    WRITE: / lv_linha.
  ENDLOOP.
ENDFORM.
```

---

## 7. Tratamento de Erros e Log de Sessão

### 7.1 Estrutura BDCMSGCOLL — Mensagens do BDC

Após cada `CALL TRANSACTION`, as mensagens ficam disponíveis em `BDCMSGCOLL`:

| Campo | Descrição |
|-------|-----------|
| `MSGTYP` | Tipo: `S`=Sucesso, `E`=Erro, `W`=Warning, `A`=Abend, `I`=Info |
| `MSGID` | Classe de mensagem (ex: `ME`, `M7`, `VB`) |
| `MSGNR` | Número da mensagem |
| `MSGV1~4` | Variáveis da mensagem (texto dinâmico) |

### 7.2 Padrão robusto de captura de erros

```abap
" Classificar mensagens por tipo
DATA: lv_sucesso TYPE abap_bool VALUE abap_false,
      lv_erro    TYPE string.

LOOP AT gt_msgs INTO DATA(ls_msg).
  CASE ls_msg-msgtyp.
    WHEN 'S'.
      " Sucesso — capturar número do documento criado
      lv_sucesso = abap_true.
    WHEN 'E' OR 'A'.
      " Erro — concatenar para log
      lv_erro = |{ ls_msg-msgid }{ ls_msg-msgnr } - { ls_msg-msgv1 } { ls_msg-msgv2 }|.
      EXIT.
    WHEN 'W'.
      " Warning — registrar mas continuar
  ENDCASE.
ENDLOOP.

" Gravar em tabela Z de log
DATA(ls_log) = VALUE zlog_bdc(
  datum    = sy-datum,
  uzeit    = sy-uzeit,
  uname    = sy-uname,
  registro = ls_input-lifnr,
  status   = COND #( WHEN lv_sucesso = abap_true THEN 'S' ELSE 'E' ),
  mensagem = lv_erro
).
INSERT zlog_bdc FROM ls_log.
```

### 7.3 Monitoramento via SM35

A transação **SM35** gerencia sessões BDC criadas pelo SESSION METHOD:

```
SM35
├── Listar sessões disponíveis
├── Filtrar por: nome, usuário, data, status
├── Status da sessão:
│     New         → Aguardando processamento
│     In Process  → Sendo executada
│     Finished    → Concluída com sucesso
│     Error       → Concluída com erros
│     Held        → Pausada manualmente
├── Processar sessão (foreground/background)
├── Ver log detalhado por registro
└── Reprocessar apenas registros com erro
```

> 💡 **Vantagem do SESSION METHOD:** O SM35 permite reprocessar **apenas os registros com erro** sem re-executar os que já foram bem-sucedidos.

---

## 8. BDC com Arquivo Externo — Upload de Planilha

### 8.1 Fluxo típico de carga com planilha

```
Planilha Excel / CSV (preparada pelo usuário)
        ↓
Upload via GUI_UPLOAD (arquivo local) ou
Upload via OPEN DATASET (arquivo no servidor)
        ↓
Parsing das linhas → Estrutura interna ABAP
        ↓
Loop: para cada linha → montar BDCDATA → executar BDC
        ↓
Log de resultado (sucesso / erro por linha)
        ↓
Download do log via GUI_DOWNLOAD
```

### 8.2 Template de layout de arquivo de entrada

```
" Exemplo de layout CSV para carga MM01 (criação de material):

MATNR;MBRSH;MTART;MAKTX;MEINS;MATKL;WERKS
MAT-000001;C;ROH;Etileno Grau Polimero;KG;00101;1000
MAT-000002;C;ROH;Propileno Quimico;KG;00101;1000
MAT-000003;C;FERT;Polietileno HDPE;KG;00201;1000
```

### 8.3 Dicas para o layout do arquivo

- Use **delimitador ponto-e-vírgula** (`;`) — evita conflito com vírgula decimal
- Inclua **linha de cabeçalho** com nomes dos campos SAP (facilita mapeamento)
- Para datas, padronize em **AAAAMMDD** (formato SAP interno)
- Para valores numéricos, use **ponto como decimal** (`1234.56`) — o SAP converte conforme parametrização do usuário
- Crie uma coluna `STATUS` e `MENSAGEM` para preencher com o resultado do BDC

---

## 9. Boas Práticas e Performance em Cargas Massivas

### 9.1 Boas práticas de desenvolvimento

```
✅ SEMPRE criar tabela Z de log (status por registro)
✅ SEMPRE implementar modo de teste (dry run sem executar)
✅ Processar em blocos (COMMIT WORK a cada N registros)
✅ Usar MODE 'N' (background) em produção
✅ Documentar o mapeamento campo-a-campo no código
✅ Gravar a transação SHDB com dados representativos
✅ Tratar popups inesperados (DYNPRO de confirmação)
✅ Validar dados ANTES do BDC (evitar erros previsíveis)

❌ NUNCA executar BDC em produção sem modo teste primeiro
❌ NUNCA usar MODE 'A' (foreground) para mais de 10 registros
❌ NUNCA ignorar mensagens de Warning (podem indicar problema)
❌ NUNCA fazer BDC sem log persistente
```

### 9.2 Tratamento de popups inesperados

O maior problema em BDC é um **popup inesperado** que não foi gravado no SHDB. O programa trava esperando uma resposta que nunca vem.

```abap
" Solução: adicionar tratamento genérico de popup no final
" de cada tela onde popups podem aparecer

" Se aparecer popup de confirmação → confirmar com YES
bdc_dynpro 'SAPLSPO1' '0300'.        " Tela de popup genérico
  bdc_field 'BDC_OKCODE' '=YES'.    " Confirmar

" Se aparecer popup de informação → ENTER para fechar
bdc_dynpro 'SAPLSPO1' '0100'.
  bdc_field 'BDC_OKCODE' '/00'.
```

> 💡 **Dica:** Execute o BDC em **MODE 'E'** (Error mode) inicialmente — o sistema abre as telas apenas quando há erro, permitindo identificar popups inesperados.

### 9.3 Performance por volume

| Volume | Estratégia recomendada |
|--------|----------------------|
| < 100 registros | CALL TRANSACTION MODE 'A' para validação visual |
| 100 – 5.000 | CALL TRANSACTION MODE 'N' com log Z |
| 5.000 – 50.000 | SESSION METHOD processada em background (SM35) |
| > 50.000 | Considerar Direct Input, BAPI ou IDoc em vez de BDC |

### 9.4 Otimizações de código

```abap
" ✅ Pré-validar dados antes de chamar o BDC
" Evita chamar o BDC para registros que já se sabe que falharão

" Exemplo: verificar se material já existe antes de criar
SELECT SINGLE matnr FROM mara
  INTO @DATA(lv_existe)
  WHERE mandt = @sy-mandt
    AND matnr = @ls_input-matnr.

IF sy-subrc = 0.
  " Material já existe → pular ou ir para alteração (MM02)
  APPEND |⚠️ Material { ls_input-matnr } já existe - ignorado| TO gt_log.
  CONTINUE.
ENDIF.

" ✅ COMMIT WORK a cada bloco para liberar locks
DATA lv_counter TYPE i VALUE 0.
ADD 1 TO lv_counter.
IF lv_counter MOD 100 = 0.
  COMMIT WORK AND WAIT.
ENDIF.
```

---

## 10. Checklist de Desenvolvimento BDC

### Antes de começar
- [ ] Mapear o processo completo na transação alvo
- [ ] Verificar se existe BAPI equivalente (se sim, usá-la)
- [ ] Gravar o SHDB com dados representativos de todos os cenários
- [ ] Definir layout do arquivo de entrada
- [ ] Criar tabela Z de log (MANDT, DATA, HORA, USUARIO, REGISTRO, STATUS, MENSAGEM, DOC_GERADO)

### Durante o desenvolvimento
- [ ] Implementar modo de teste (dry run)
- [ ] Mapear todos os OKCODEs necessários
- [ ] Tratar popups de confirmação
- [ ] Implementar log detalhado por registro
- [ ] Testar com 5 registros em modo foreground (`MODE 'A'`) para ver as telas
- [ ] Testar com 50 registros em modo background (`MODE 'N'`)
- [ ] Validar dados de entrada antes do BDC

### Antes de ir para produção
- [ ] Executar teste completo em ambiente de qualidade (QAS)
- [ ] Validar performance com volume real estimado
- [ ] Documentar mapeamento campo-a-campo
- [ ] Treinar o usuário responsável pela execução e log

---

## ✅ Checklist de Aprendizado — Módulo 4

Ao final deste módulo, você deve ser capaz de:

- [ ] Explicar o que é BDC e como ele diferencia de BAPI, IDoc e Direct Input
- [ ] Gravar uma transação SAP usando a transação SHDB
- [ ] Montar a tabela BDCDATA corretamente (cabeçalho de tela + campos + OKCODE)
- [ ] Usar as macros `bdc_dynpro` e `bdc_field` para montar a estrutura
- [ ] Escolher entre CALL TRANSACTION e SESSION METHOD conforme o volume
- [ ] Interpretar as mensagens de retorno em BDCMSGCOLL
- [ ] Monitorar e reprocessar sessões BDC via SM35
- [ ] Implementar log persistente em tabela Z
- [ ] Tratar popups inesperados em programas BDC
- [ ] Aplicar boas práticas de performance para cargas massivas

---

## 🎓 Conclusão do Programa — Zero to Hero em Dados SAP

Parabéns por concluir os 4 módulos! Veja a jornada percorrida:

| Módulo | Tema | Competência adquirida |
|--------|------|-----------------------|
| 1 — Básico | Fundamentos | Estrutura, mandante, dados mestres e transacionais |
| 2 — Intermediário | Fluxo por módulo | P2P, O2C, FI/CO e PP de ponta a ponta |
| 3 — Avançado | Dados em profundidade | Change Docs, IDocs, BAPIs, BW, ODP |
| 4 — BDC | Entrada de dados em massa | SHDB, BDCDATA, CALL TRANSACTION, SESSION METHOD |

### Próximos passos sugeridos

- **ABAP para Data Engineers:** SELECT avançado, internal tables, performance tuning
- **SAP S/4HANA:** CDS Views, AMDP, Virtual Data Models (VDM)
- **SAP BTP:** Extração de dados para cloud via APIs OData
- **Integração com Azure:** ADF + SAP Connector, Databricks + pyrfc, Delta Lake com dados SAP

---

*Documento gerado para uso interno — Programa de Capacitação SAP*
*Versão 1.0 | Módulo 4 de 4 — Módulo Final*
