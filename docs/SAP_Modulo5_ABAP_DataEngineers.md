# 📘 SAP — Módulo 5: ABAP para Data Engineers
### Programa de Estudo: Zero to Hero em Dados SAP

---

> **Objetivo do módulo:** Dominar os recursos ABAP essenciais para engenheiros de dados — SELECT avançado, internal tables, performance tuning e padrões de código para extração e transformação de dados dentro do SAP.

---

## 1. Por que ABAP para Data Engineers?

ABAP (Advanced Business Application Programming) é a linguagem nativa do SAP. Para um engenheiro de dados que trabalha com SAP, conhecer ABAP significa:

- Escrever **extractors customizados** diretamente no SAP
- Criar **programas de validação e transformação** antes de exportar dados
- Entender e **depurar** programas existentes que alimentam pipelines
- Construir **BDCs, BAPIs wrappers e relatórios** de qualidade de dados
- Otimizar **performance de leitura** em tabelas com bilhões de registros

---

## 2. SELECT Avançado em ABAP

### 2.1 Sintaxe moderna (ABAP 7.40+)

A partir do ABAP 7.40, o SAP introduziu sintaxe inline e expressões inline que tornam o código muito mais limpo:

```abap
" ✅ Sintaxe moderna — declaração inline com DATA(...)
SELECT ebeln, lifnr, bedat, netwr
  FROM ekko
  INTO TABLE @DATA(lt_po)           " Declara e preenche na mesma linha
  WHERE mandt = @sy-mandt
    AND bedat >= @lv_data_inicio
    AND bstyp = 'F'.

" ✅ Single result inline
SELECT SINGLE matnr, maktx
  FROM makt
  INTO @DATA(ls_material)
  WHERE mandt = @sy-mandt
    AND matnr = @lv_matnr
    AND spras = @sy-langu.
```

### 2.2 SELECT com JOIN

```abap
" JOIN entre cabeçalho e item de PO
SELECT k~ebeln,
       k~lifnr,
       k~bedat,
       p~ebelp,
       p~matnr,
       p~menge,
       p~netpr,
       p~netwr
  FROM ekko AS k
  INNER JOIN ekpo AS p
    ON k~mandt = p~mandt
   AND k~ebeln = p~ebeln
  INTO TABLE @DATA(lt_po_itens)
  WHERE k~mandt = @sy-mandt
    AND k~bedat BETWEEN @lv_de AND @lv_ate
    AND k~bstyp = 'F'
    AND p~loekz = space.              " Exclui itens deletados
```

### 2.3 SELECT com Subquery

```abap
" Materiais que tiveram movimentação no período
SELECT matnr, maktx
  FROM makt
  INTO TABLE @DATA(lt_mat_com_mov)
  WHERE mandt = @sy-mandt
    AND spras = 'PT'
    AND matnr IN (
      SELECT matnr FROM mseg
        WHERE mandt = @sy-mandt
          AND budat BETWEEN @lv_de AND @lv_ate
          AND bwart = '101'
    ).
```

### 2.4 SELECT com GROUP BY e agregações

```abap
" Estoque total por material e planta
SELECT matnr,
       werks,
       lgort,
       SUM( menge ) AS estoque_total
  FROM mard
  INTO TABLE @DATA(lt_estoque)
  WHERE mandt = @sy-mandt
    AND werks IN @lr_werks           " Range de plantas
  GROUP BY matnr, werks, lgort
  HAVING SUM( menge ) > 0            " Apenas com estoque positivo
  ORDER BY matnr, werks.
```

### 2.5 SELECT com FOR ALL ENTRIES — o padrão SAP de "IN com lista"

O `FOR ALL ENTRIES IN` é o equivalente SAP de um `JOIN` com uma lista em memória. É amplamente usado mas exige cuidados:

```abap
" Passo 1: Selecionar cabeçalhos
SELECT ebeln, lifnr, bedat
  FROM ekko
  INTO TABLE @DATA(lt_ekko)
  WHERE mandt = @sy-mandt
    AND bedat >= @lv_data_inicio.

" Passo 2: Usar FOR ALL ENTRIES para buscar itens
IF lt_ekko IS NOT INITIAL.           " ⚠️ OBRIGATÓRIO verificar se não está vazia
  SELECT ebeln, ebelp, matnr, menge, netpr
    FROM ekpo
    INTO TABLE @DATA(lt_ekpo)
    FOR ALL ENTRIES IN @lt_ekko      " Para cada linha de lt_ekko
    WHERE mandt = @sy-mandt
      AND ebeln = @lt_ekko-ebeln.   " Filtra pelo campo correspondente
ENDIF.
```

> ⚠️ **Regra de ouro do FOR ALL ENTRIES:**
> - **SEMPRE** verificar se a tabela driver não está vazia antes de usar
> - Se a tabela driver estiver **vazia**, o SAP ignora o filtro e traz **TODOS** os registros da tabela alvo — um full scan catastrófico
> - Limita internamente a ~32.000 entradas; acima disso, use SELECT com range ou loop com blocos

### 2.6 SELECT com expressões de cálculo inline

```abap
" Cálculo direto no SELECT (ABAP 7.50+)
SELECT vbeln,
       posnr,
       matnr,
       kwmeng,
       netpr,
       kwmeng * netpr AS valor_total,    " Cálculo inline
       CASE abgru
         WHEN space THEN 'Ativo'
         ELSE 'Rejeitado'
       END AS status_item
  FROM vbap
  INTO TABLE @DATA(lt_so_itens)
  WHERE mandt = @sy-mandt
    AND vbeln IN @lr_so.
```

---

## 3. Internal Tables — O Coração do ABAP

### 3.1 Tipos de Internal Table

| Tipo | Acesso | Melhor uso |
|------|--------|------------|
| `STANDARD TABLE` | Sequencial / índice | Leitura em loop, insert rápido |
| `SORTED TABLE` | Binário (por chave) | Leitura frequente por chave, já ordenada |
| `HASHED TABLE` | Hash (O(1)) | Lookup único por chave primária completa |

```abap
" Standard Table — uso geral
DATA lt_standard TYPE TABLE OF ekko.

" Sorted Table — acesso rápido por chave parcial
DATA lt_sorted TYPE SORTED TABLE OF ekko
  WITH NON-UNIQUE KEY ebeln.

" Hashed Table — lookup O(1) por chave completa
DATA lt_hashed TYPE HASHED TABLE OF ekko
  WITH UNIQUE KEY ebeln.
```

### 3.2 Operações essenciais em Internal Tables

```abap
" -------------------------------------------------------
" APPEND — adicionar linha ao final
" -------------------------------------------------------
DATA(ls_nova) = VALUE ekko( ebeln = '4500000001' lifnr = 'FORN001' ).
APPEND ls_nova TO lt_standard.

" -------------------------------------------------------
" READ TABLE — buscar um registro
" -------------------------------------------------------
" Por índice
READ TABLE lt_standard INDEX 1 INTO DATA(ls_first).

" Por chave (STANDARD TABLE — lento, O(n))
READ TABLE lt_standard INTO DATA(ls_po)
  WITH KEY ebeln = '4500000001'.

" Por chave (SORTED/HASHED — rápido, O(log n) ou O(1))
READ TABLE lt_sorted INTO DATA(ls_sorted)
  WITH TABLE KEY ebeln = '4500000001'.

" Verificar se encontrou
IF sy-subrc = 0.
  WRITE: / ls_po-lifnr.
ENDIF.

" -------------------------------------------------------
" LOOP AT — iterar sobre a tabela
" -------------------------------------------------------
LOOP AT lt_standard INTO DATA(ls_loop).
  WRITE: / ls_loop-ebeln, ls_loop-lifnr.
ENDLOOP.

" Loop com WHERE inline (ABAP 7.40+)
LOOP AT lt_standard INTO DATA(ls_f)
  WHERE lifnr = 'FORN001'.
  " Processa apenas linhas do fornecedor FORN001
ENDLOOP.

" -------------------------------------------------------
" MODIFY — alterar linha existente
" -------------------------------------------------------
READ TABLE lt_standard INTO DATA(ls_mod)
  WITH KEY ebeln = '4500000001'.
IF sy-subrc = 0.
  ls_mod-netwr = 50000.
  MODIFY lt_standard FROM ls_mod
    TRANSPORTING netwr              " Atualiza apenas o campo netwr
    WHERE ebeln = '4500000001'.
ENDIF.

" -------------------------------------------------------
" DELETE — remover linhas
" -------------------------------------------------------
DELETE lt_standard WHERE lifnr = space.  " Remove linhas sem fornecedor

" -------------------------------------------------------
" SORT — ordenar
" -------------------------------------------------------
SORT lt_standard BY bedat DESCENDING lifnr ASCENDING.

" -------------------------------------------------------
" DELETE ADJACENT DUPLICATES — remover duplicatas
" -------------------------------------------------------
SORT lt_standard BY ebeln.
DELETE ADJACENT DUPLICATES FROM lt_standard COMPARING ebeln.
```

### 3.3 VALUE e CORRESPONDING — Construtores modernos

```abap
" VALUE: construir estrutura ou tabela diretamente
DATA(ls_ekko) = VALUE ekko(
  ebeln = '4500000001'
  lifnr = 'FORN001'
  bedat = sy-datum
  waers = 'BRL'
).

" VALUE para tabela com múltiplas linhas
DATA(lt_filtros) = VALUE range_matnr_tab(
  ( sign = 'I' option = 'EQ' low = 'MAT-001' )
  ( sign = 'I' option = 'EQ' low = 'MAT-002' )
  ( sign = 'I' option = 'BT' low = 'MAT-010' high = 'MAT-020' )
).

" CORRESPONDING: copiar campos de mesmo nome entre estruturas
DATA ls_target TYPE zmy_struct.
ls_target = CORRESPONDING #( ls_ekko ).    " Copia campos com mesmo nome

" CORRESPONDING com mapeamento customizado
ls_target = CORRESPONDING #( ls_ekko
  MAPPING z_fornecedor = lifnr             " z_fornecedor ← lifnr
          z_data_po    = bedat ).
```

---

## 4. Performance Tuning em ABAP

### 4.1 As regras de ouro de performance

```
1. Sempre filtrar pela chave primária (ou índice secundário) no WHERE
2. Nunca fazer SELECT * — especificar apenas os campos necessários
3. Nunca SELECT dentro de LOOP — use JOIN ou FOR ALL ENTRIES
4. Verificar ALWAYS se FOR ALL ENTRIES driver não está vazia
5. Preferir HASHED TABLE para lookups repetidos
6. Usar FIELD-SYMBOLS para leitura de tabelas grandes (evita cópia)
7. Evitar SORT em tabelas com milhões de linhas — use SORTED TABLE
```

### 4.2 O anti-pattern mais perigoso: SELECT dentro de LOOP

```abap
" ❌ PÉSSIMO — N queries no banco (1 por linha da tabela)
LOOP AT lt_ekko INTO DATA(ls_po).
  SELECT SINGLE maktx
    FROM makt
    INTO @DATA(lv_descricao)
    WHERE matnr = @ls_po-matnr
      AND spras = 'PT'.
  ls_po-descricao = lv_descricao.
ENDLOOP.

" ✅ CORRETO — 1 query + lookup em memória
" Passo 1: coletar todos os materiais únicos
DATA lt_matnr TYPE SORTED TABLE OF mara-matnr
  WITH UNIQUE KEY table_line.
LOOP AT lt_ekko INTO DATA(ls_po2).
  INSERT ls_po2-matnr INTO TABLE lt_matnr.
ENDLOOP.

" Passo 2: buscar descrições de uma vez
IF lt_matnr IS NOT INITIAL.
  SELECT matnr, maktx
    FROM makt
    INTO TABLE @DATA(lt_desc)
    FOR ALL ENTRIES IN @lt_matnr
    WHERE matnr = @lt_matnr-table_line
      AND spras = 'PT'.
ENDIF.

" Passo 3: construir hashed table para lookup O(1)
DATA lt_desc_hash TYPE HASHED TABLE OF LINE OF lt_desc
  WITH UNIQUE KEY matnr.
lt_desc_hash = lt_desc.    " Conversão direta

" Passo 4: loop com READ TABLE rápido
LOOP AT lt_ekko INTO DATA(ls_po3).
  READ TABLE lt_desc_hash INTO DATA(ls_d)
    WITH TABLE KEY matnr = ls_po3-matnr.
  IF sy-subrc = 0.
    ls_po3-descricao = ls_d-maktx.
  ENDIF.
ENDLOOP.
```

### 4.3 FIELD-SYMBOLS — Leitura sem cópia de memória

Para tabelas muito grandes, usar `FIELD-SYMBOLS` em vez de `INTO DATA(...)` evita a cópia de cada linha:

```abap
" ❌ Com DATA() — copia cada linha para ls_loop (uso de memória)
LOOP AT lt_grande INTO DATA(ls_loop).
  WRITE: / ls_loop-ebeln.
ENDLOOP.

" ✅ Com FIELD-SYMBOL — ponteiro direto na tabela (sem cópia)
FIELD-SYMBOLS: <fs_po> TYPE ekko.

LOOP AT lt_grande ASSIGNING <fs_po>.
  WRITE: / <fs_po>-ebeln.
  " Modificação direta (sem MODIFY)
  <fs_po>-netwr = <fs_po>-netwr * 1.1.
ENDLOOP.
```

### 4.4 Secondary Indexes — Acelerando SELECTs em tabelas customizadas

```abap
" Para tabelas Z com acesso frequente por campos não-chave,
" criar índice secundário via SE11:
"   SE11 → Tabela Z → aba 'Indexes' → Create

" Exemplo: tabela ZLOG_BDC com acesso frequente por DATUM
" Criar índice: ZLOG_BDC~Z01 com campos (MANDT, DATUM, STATUS)

" O SELECT passa a usar o índice automaticamente:
SELECT * FROM zlog_bdc
  INTO TABLE @DATA(lt_log)
  WHERE mandt  = @sy-mandt
    AND datum  = @sy-datum    " ← campo do índice Z01
    AND status = 'E'.         " ← campo do índice Z01
```

### 4.5 Parallel Cursor — Loop duplo eficiente

Quando é necessário iterar duas tabelas ordenadas em paralelo sem usar READ TABLE:

```abap
" Ambas as tabelas ordenadas pelo campo de join
SORT lt_ekko BY ebeln.
SORT lt_ekpo BY ebeln.

DATA lv_index TYPE sy-tabix VALUE 1.

LOOP AT lt_ekko INTO DATA(ls_k).
  LOOP AT lt_ekpo INTO DATA(ls_p)
    FROM lv_index.              " Começa de onde parou
    IF ls_p-ebeln <> ls_k-ebeln.
      lv_index = sy-tabix.      " Salva posição para próxima iteração
      EXIT.
    ENDIF.
    " Processar par (ls_k, ls_p)
    WRITE: / ls_k-ebeln, ls_p-matnr, ls_p-menge.
  ENDLOOP.
ENDLOOP.
```

---

## 5. Padrões de Código para Extração de Dados

### 5.1 Estrutura de um programa extractor ABAP

```abap
*&---------------------------------------------------------------------*
*& Program: Z_EXTRACTOR_PO
*& Descrição: Extração de Purchase Orders para arquivo/tabela staging
*&---------------------------------------------------------------------*
REPORT z_extractor_po.

" -------------------------------------------------------
" Tipos e estruturas
" -------------------------------------------------------
TYPES: BEGIN OF ty_po_flat,
  ebeln  TYPE ekko-ebeln,
  lifnr  TYPE ekko-lifnr,
  bedat  TYPE ekko-bedat,
  ebelp  TYPE ekpo-ebelp,
  matnr  TYPE ekpo-matnr,
  menge  TYPE ekpo-menge,
  netpr  TYPE ekpo-netpr,
  netwr  TYPE ekpo-netwr,
  maktx  TYPE makt-maktx,
  name1  TYPE lfa1-name1,
END OF ty_po_flat.

DATA: lt_result  TYPE TABLE OF ty_po_flat,
      lt_output  TYPE TABLE OF string.

" -------------------------------------------------------
" Tela de seleção
" -------------------------------------------------------
SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.
  SELECT-OPTIONS so_bedat FOR ekko-bedat OBLIGATORY.
  SELECT-OPTIONS so_werks FOR ekpo-werks.
  PARAMETERS: p_bukrs TYPE ekko-bukrs DEFAULT 'BRAS'.
  PARAMETERS: p_file  TYPE string.
SELECTION-SCREEN END OF BLOCK b1.

" -------------------------------------------------------
" START-OF-SELECTION
" -------------------------------------------------------
START-OF-SELECTION.

  " 1. Extração principal com JOIN
  SELECT k~ebeln, k~lifnr, k~bedat,
         p~ebelp, p~matnr, p~menge, p~netpr, p~netwr
    FROM ekko AS k
    INNER JOIN ekpo AS p
      ON k~mandt = p~mandt AND k~ebeln = p~ebeln
    INTO TABLE @DATA(lt_raw)
    WHERE k~mandt = @sy-mandt
      AND k~bukrs = @p_bukrs
      AND k~bedat IN @so_bedat
      AND k~bstyp = 'F'
      AND p~werks IN @so_werks
      AND p~loekz = @space.

  CHECK lt_raw IS NOT INITIAL.

  " 2. Enriquecer com descrição de material
  SELECT matnr, maktx
    FROM makt INTO TABLE @DATA(lt_makt)
    FOR ALL ENTRIES IN @lt_raw
    WHERE matnr = @lt_raw-matnr AND spras = 'PT'.

  " 3. Enriquecer com nome do fornecedor
  SELECT lifnr, name1
    FROM lfa1 INTO TABLE @DATA(lt_lfa1)
    FOR ALL ENTRIES IN @lt_raw
    WHERE lifnr = @lt_raw-lifnr.

  " 4. Construir hashed tables para lookup
  DATA lt_makt_h TYPE HASHED TABLE OF LINE OF lt_makt WITH UNIQUE KEY matnr.
  DATA lt_lfa1_h TYPE HASHED TABLE OF LINE OF lt_lfa1 WITH UNIQUE KEY lifnr.
  lt_makt_h = lt_makt.
  lt_lfa1_h = lt_lfa1.

  " 5. Montar resultado final
  LOOP AT lt_raw INTO DATA(ls_raw).
    DATA(ls_out) = VALUE ty_po_flat(
      ebeln = ls_raw-ebeln
      lifnr = ls_raw-lifnr
      bedat = ls_raw-bedat
      ebelp = ls_raw-ebelp
      matnr = ls_raw-matnr
      menge = ls_raw-menge
      netpr = ls_raw-netpr
      netwr = ls_raw-netwr
    ).

    READ TABLE lt_makt_h INTO DATA(ls_mat) WITH TABLE KEY matnr = ls_raw-matnr.
    IF sy-subrc = 0. ls_out-maktx = ls_mat-maktx. ENDIF.

    READ TABLE lt_lfa1_h INTO DATA(ls_forn) WITH TABLE KEY lifnr = ls_raw-lifnr.
    IF sy-subrc = 0. ls_out-name1 = ls_forn-name1. ENDIF.

    APPEND ls_out TO lt_result.
  ENDLOOP.

  " 6. Exportar para CSV
  PERFORM exportar_csv USING lt_result p_file.

" -------------------------------------------------------
" FORM: Exportar para CSV
" -------------------------------------------------------
FORM exportar_csv
  USING pt_data TYPE TABLE
        pv_file TYPE string.

  DATA lt_csv TYPE TABLE OF string.

  " Cabeçalho
  APPEND 'EBELN;LIFNR;BEDAT;EBELP;MATNR;MENGE;NETPR;NETWR;MAKTX;NAME1'
    TO lt_csv.

  " Linhas
  LOOP AT pt_data INTO DATA(ls_r) CASTING TYPE ty_po_flat.
    APPEND |{ ls_r-ebeln };{ ls_r-lifnr };{ ls_r-bedat };| &
           |{ ls_r-ebelp };{ ls_r-matnr };{ ls_r-menge };| &
           |{ ls_r-netpr };{ ls_r-netwr };{ ls_r-maktx };{ ls_r-name1 }|
      TO lt_csv.
  ENDLOOP.

  " Download
  CALL FUNCTION 'GUI_DOWNLOAD'
    EXPORTING
      filename = pv_file
      filetype = 'ASC'
    TABLES
      data_tab = lt_csv.

ENDFORM.
```

---

## 6. Debugging e Análise de Performance

### 6.1 ABAP Debugger — Comandos essenciais

```
F5  →  Step Into (entrar em chamada de função)
F6  →  Step Over (pular chamada)
F7  →  Step Out (sair da função atual)
F8  →  Continue (executar até próximo breakpoint)

Breakpoint externo:  /h  no campo de comando → ativa debugger na próxima tela
Breakpoint em código: BREAK-POINT.  ou  BREAK <usuario>.
```

### 6.2 Runtime Analysis — SAT

```
Transação: SAT (antigo SE30)
→ Define qual programa monitorar
→ Executa o programa
→ Analisa: tempo de CPU, chamadas de banco, hot spots
→ Identifica quais SELECTs consomem mais tempo
```

### 6.3 SQL Trace — ST05

```
Transação: ST05
→ Activate Trace → executa programa → Deactivate Trace
→ Display Trace: lista todos os SQLs executados
→ Mostra: tabela, índice usado, tempo, registros retornados
→ Identify expensive SELECTs: full table scans aparecem com "TABLE SCAN"
```

---

## ✅ Checklist de Aprendizado — Módulo 5

- [ ] Escrever SELECT com JOIN, subquery, GROUP BY e FOR ALL ENTRIES
- [ ] Usar sintaxe moderna com DATA(...) inline e VALUE(...)
- [ ] Escolher o tipo correto de internal table (STANDARD, SORTED, HASHED)
- [ ] Eliminar o anti-pattern SELECT dentro de LOOP
- [ ] Usar FIELD-SYMBOLS para leitura de tabelas grandes sem cópia
- [ ] Aplicar o padrão: SELECT → enriquecer com FOR ALL ENTRIES → Hashed Table lookup
- [ ] Usar ST05 e SAT para identificar gargalos de performance
- [ ] Construir um extractor ABAP completo com export para CSV

---

## 📚 Próximo Módulo

➡️ **Módulo 6 — SAP S/4HANA:** CDS Views, AMDP, Virtual Data Models e como o S/4HANA transforma a arquitetura de dados SAP.

---

*Documento gerado para uso interno — Programa de Capacitação SAP*
*Versão 1.0 | Módulo 5 de 8*
