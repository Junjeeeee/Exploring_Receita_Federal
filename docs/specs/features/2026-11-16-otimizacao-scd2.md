# Feature: SCD2 da silver_intermediate em duas fases (otimização, fase 2)

**Status:** T0–T4 implementadas e validadas na amostra (2026-11-16; resultados na §11, desvios na §11.1) —
**preparado, não executado no cluster**; falta a T5 (medição na base real) e a T6 (documentação)
**Camadas afetadas:** silver_intermediate (`empresas`, `estabelecimentos`, `dados_simples`, `socios`)
**Branch:** `otimizacao_pipeline`
**Spec guarda-chuva:** [`2026-11-16-otimizacao-pipeline.md`](2026-11-16-otimizacao-pipeline.md) (princípios no §2)

## Roteiro de leitura (1 página)

**O problema.** As quatro tabelas de histórico (SCD2) guardam uma linha por versão de cada CNPJ, sócio ou
opção pelo Simples. A cada mês, 98% a 99% das linhas da Receita chegam iguais às do mês anterior. Mesmo assim,
o código de hoje lê a bronze com todas as colunas duas a três vezes e espalha essas linhas largas pela rede do
cluster (*shuffle*) três ou quatro vezes. No fim, entrega ao `MERGE` uma lista que inclui **todas** as versões
de todas as empresas, e o `MERGE` reescreve a tabela inteira. Em 4 meses, `si_estabelecimentos` levou 38 min e
embaralhou 97 GB. Nos 40 meses da janela, esse padrão estoura o tempo.

**A ideia.** Separar "descobrir o que mudou" de "gravar o que mudou".
1. **Detectar (fase 1).** Para saber se uma linha mudou, basta comparar a impressão digital dela (o
   `change_hash`, um SHA-256 dos atributos que a bronze já calcula) com a da versão anterior. A fase 1 lê só
   quatro colunas: chave, mês, carimbo e hash. Agrupa por chave (uma única troca de dados, com linhas estreitas),
   aplica a mesma regra de hoje (abre versão quando o hash muda) e grava uma tabela pequena de **ações**:
   `INSERE` (versão nova), `AJUSTA_FIM` (fechar uma versão) e `APAGA` (só em reprocessamento).
2. **Gravar (fase 2).** Um único `MERGE`, atômico, aplica as ações. Os atributos completos (endereço, telefone…)
   e a limpeza (regex, datas) só são buscados na bronze para as linhas que abrem versão: cerca de 1% a 2% por
   mês. A busca usa uma lista de chaves enviada a todas as máquinas (*broadcast*). Assim a bronze larga não
   atravessa a rede; ela ainda é lida do disco uma vez.

**O que não muda.** O resultado: mesmas linhas e mesmos valores (§4); só `updated_at` registra o momento da
escrita. Também ficam os mesmos parâmetros, nomes de arquivo, tasks do Job e esquemas das tabelas.

**O que se ganha** (estimativa, a medir na T5). Na carga inicial de 4 meses, cerca de 2× menos shuffle e uma
leitura larga em vez de duas ou três. Em cada mês incremental, que é o que se repete ~36 vezes, a parte larga
que passa pela rede cai de ~57 M linhas para ~1 M. Sem *deletion vectors*, o `MERGE` ainda reescreve os arquivos
da tabela (D4).

**O que o usuário decide (§9):** D1, duas fases (recomendado) ou uma passada larga, mais simples e com menos
ganho; D3, `MERGE` único (recomendado); D4, *deletion vectors*; D5, falhar se faltar mês na bronze; D6,
checar snapshot parcial de sócios; D7, aceitar duas diferenças que só aparecem com bronze alterada; D8, blocos
de 12 meses. A D2 (prefixo do hash) foi retirada: usa-se o hash completo.

## 1. Requisito

- **Problema:** com 4 meses, `si_estabelecimentos` levou 38 min (76 GB lidos, 97 GB de shuffle),
  `si_socios` 25 min (69 GB lidos) e `si_dados_simples` 28 min, quase sem *spill* (B20). A janela completa
  (~40 meses, em blocos de 6–12) precisa caber no cluster clássico DBR 15.4 / Spark 3.5 (r5d.xlarge, 1–4 workers
  spot), sem serverless, dentro do *timeout* de 4 h por execução (C-21).
- **Quem consome:** `log_eventos_negocio`, `dim_empresa`, `dim_grupo_cnae`, `fato_mudanca_estado`,
  `fato_empresa_mes`, `dq_gold`. O conteúdo das tabelas não muda, então nada muda para eles.
- **Fora de escopo:** bronze (B17); log, fato e dashboard (fase 3); regra de PK repetida (o B16 mantém o
  desempate de hoje); identidade de sócio (C-18); ordem das colunas do hash (C-19).

## 2. Diagnóstico

### 2.1 O que já se sabe (medido)

| Tabela | Linhas bronze (4 meses) | Por mês | Versões SCD2 | Correntes | Versões novas/mês após o 1º (estimativa) |
|---|---|---|---|---|---|
| empresas | 216,6 M | ~54 M | 55,0 M | 54,7 M | ~0,4 M (~0,7%) |
| estabelecimentos | 228,1 M | ~57 M | 59,0 M | 57,6 M | ~0,8 M (~1,5%) |
| dados_simples | 143,2 M | ~36 M | 36,8 M | 36,3 M | ~0,5 M (~1,4%) |
| socios | 91,7 M | ~23 M | 24,1 M | 23,3 M | ~0,5 M (~2%) |

Fonte: contagens de 2026-11-15 nas tabelas reais. A última coluna é **estimativa** (versões menos pks do 1º
mês, dividido por 3) e é confirmada na T0 (Q2). A conclusão vale mesmo com erro de 2×: **por mês, 98% a 99%
das linhas não mudam nada**, mas o código atual carrega todas elas, com todas as colunas, pelo plano inteiro.

Outros fatos do código:
- A bronze real é particionada por `mes_referencia` (`bronze_lib.py:268`, `partitionBy` + `replaceWhere`). Ler
  só os meses da janela já acontece; o desperdício é ler os **mesmos** meses várias vezes, e com todas as
  colunas. A bronze **da amostra** não é particionada (`criar_amostra_bronze.sql:12-25`, CTAS simples), então os
  planos da amostra não mostram poda de partição.
- A bronze não tem `pk` nula (`bronze_lib.py:189`) e remove cópias idênticas (`dropDuplicates()`,
  `bronze_lib.py:197`). Por isso `(pk, mes, change_hash)` é única na bronze (confirmar na T0).
- O Job roda com `spark.databricks.delta.optimizeWrite.enabled = true` (`tcc_receita_pipeline.job.yml:36`), o que
  acrescenta uma troca de dados antes de cada escrita Delta.
- As SCD2 de hoje **já processam a janela inteira de uma vez** com `LAG`/`LEAD`. O problema é a largura das
  linhas e o número de leituras, não um laço mês a mês.

### 2.2 `empresas.sql`, `estabelecimentos.sql`, `dados_simples.sql` (mesmo padrão)

Linhas do `estabelecimentos.sql`; entre parênteses, as de `empresas.sql` / `dados_simples.sql`.

| # | Onde | O que acontece | Custo |
|---|---|---|---|
| 1 | `pks_afetadas` 60–66 (34–40 / 35–41) | 1ª leitura da bronze (só `pk`) + `DISTINCT` | troca estreita por `pk` |
| 2 | `bronze_raw` 67–77 (41–51 / 42–52) | 2ª leitura da bronze, **todas as colunas**, `INNER JOIN pks_afetadas` | **troca larga nº 1**: a bronze inteira da janela é embaralhada para um join que não filtra nada (`pks_afetadas` sai das mesmas linhas, com o mesmo filtro) |
| 3 | `bronze_limpo` 78–138 (52–67 / 53–78) | limpeza (4 `regexp_replace`, datas, telefones) em todas as linhas | CPU gasta em ~98% de linhas que não abrem versão |
| 4 | `historico_intacto` 139–157 (68–80 / 79–90) | 1ª leitura da silver, todas as colunas, join com `pks_afetadas` | troca larga da silver (≈ 0 na carga inicial; ≈ a tabela no incremental) |
| 5 | união → `LAG` → `LEAD` 158–187 (81–102 / 91–113) | `UNION ALL` + janelas por `pk` | provável **troca larga nº 2**: o `UNION` não preserva o particionamento (confirmar no `EXPLAIN` do cluster) |
| 6 | `FULL OUTER JOIN` 188–198 (103–113 / 114–124) | 2ª leitura da silver + join por `(pk, valid_since)` | **troca larga nº 3** de todas as versões recalculadas |
| 7 | `MERGE` 200–274 (115–146 / 126–162) | *materialize source* (B20: recalcula a fonte, relendo a bronze) + 3ª leitura da silver + join | a fonte traz todas as versões de todas as pks afetadas (≈ todas), então quase toda linha do destino casa, passa pelo join e é reescrita. O `optimizeWrite` soma mais uma troca larga |

### 2.3 `socios.sql`

| # | Linhas | O que acontece | Custo |
|---|---|---|---|
| 1 | 42–50 | guarda `EXISTS` (instrução própria) | barata |
| 2 | 59 | `MIN(mes_referencia)` da bronze | só a coluna de partição |
| 3 | 61–73 | `bronze_raw`: janela **+ mês anterior**, todas as colunas, `ROW_NUMBER` por `(pk, mes)` | **troca larga nº 1** |
| 4 | 74–98 | `COUNT(DISTINCT mes)` sobre `bronze_raw` | CTE referenciada duas vezes; recalculada ou reaproveitada, conforme o plano |
| 5 | 99–125 | limpeza de todas as linhas (também do mês anterior) | CPU |
| 6 | 150–172 | `estados_inativos`: `anterior` × `meses_janela` + `LEFT ANTI JOIN atual` | **troca larga nº 2** |
| 7 | 178–185 | `pks_afetadas` + 1ª leitura da silver | `eventos_janela` também referenciada duas vezes |
| 8 | 186–200 | `historico_intacto` (silver larga) | troca larga da silver |
| 9 | 201–223 | união + janelas | provável **troca larga nº 3** |
| 10 | 224–234 | `FULL OUTER JOIN` com a 3ª leitura da silver | **troca larga nº 4** |
| 11 | 236–276 | `MERGE` (`PARTITIONED BY is_current`) | igual ao item 7 da §2.2 |

### 2.4 Avaliação das ideias de partida

| Ideia | Avaliação |
|---|---|
| (1) Detectar com `(pk, mes, change_hash)` e buscar atributos só de quem abre versão | **Adotada, com uma condição**: a busca é um join com a bronze larga. Como join comum (*sort-merge*), embaralharia a bronze larga inteira e o ganho sumiria. Ela só compensa porque a lista de chaves das versões novas é difundida (*broadcast*) como inteiros de 64 bits, `xxhash64(pk, mes)`, e o join exato vem depois, já com poucas linhas. A bronze larga **continua sendo lida** do disco (a `pk` não está ordenada nos arquivos); o que se evita é a troca de dados. No mês em que quase toda pk abre versão ("mês denso": 1º mês da carga inicial, ou todos os hashes mudando como no C-19 ou na reextração da D-16), a lista seria do tamanho da bronze. Esse mês dispensa a lista e entra inteiro |
| (2) Ler a bronze uma vez | **Adotada**: a fase 1 lê 4 colunas; a fase 2 lê as colunas largas uma vez, num único *scan*. `CACHE TABLE`/*temp view* não servem: o runner da amostra manda cada instrução sozinha à Statement Execution API, sem garantia de sessão compartilhada. Tabelas de trabalho Delta (`tmp_scd2_*`) servem nos dois runners |
| (3) Carga inicial por `INSERT OVERWRITE` | **Não é caminho separado**: o mesmo arquivo serve; na carga inicial só há `INSERE`. Reescrever a tabela inteira a cada bloco custaria proporcional ao histórico sem ganho |
| (4) Janela inteira com `LAG` | **Já é assim hoje**; mantido |
| (5) Poda por `mes_referencia` | **Já acontece** na bronze real. Clustering na silver não ajuda o `MERGE` (as mudanças se espalham por todas as pks). Decisão desta fase: **sem clustering na silver**; se a fase 3 precisar de *data skipping* no log, reabre aqui (R21) |

## 3. Desenho

### 3.1 Princípios

- **Uma troca de dados por pk na fase 1**, com linhas estreitas: `pk`, mês, origem, carimbo e `change_hash`
  **completo** (64 caracteres). Decisão do gerente: o hash completo dispensa defender colisão de prefixo e
  mantém exato o desempate do B16 (`change_hash DESC`). O custo é uma linha estreita um pouco maior
  (~80 bytes comprimidos contra ~175 da linha larga em estabelecimentos).
- **Comparação novo × existente por janela em `(pk, mês)`**, não por join (um join reembaralharia tudo).
- **Uma escrita só e atômica**: um `MERGE` com as três ações. Não existe estado intermediário a retomar (R3).
- **Limpeza só nas linhas que viram versão**, com as expressões de hoje copiadas sem alteração.

### 3.2 Estrutura de cada arquivo (mesmo nome, mesmas tasks do Job)

| Nº | Instrução | Escreve? |
|---|---|---|
| 0 | `CREATE TABLE IF NOT EXISTS` do destino (DDL de hoje, sem mudança) | — |
| 1 | `SELECT ASSERT_TRUE(...)`: janela válida; todo mês da janela existe na bronze (socios: também o mês anterior, salvo na carga inicial) | não |
| 2 | `CREATE OR REPLACE TABLE ...tmp_scd2_acoes_<t> AS` — fase 1 | tabela de trabalho |
| 3 | `CREATE OR REPLACE TABLE ...tmp_scd2_meses_<t> AS` — versões novas e linhas da bronze por mês, marca meses densos | tabela de trabalho |
| 4 | `SELECT ASSERT_TRUE(...)` — chaves a difundir ≤ limite | não |
| 5 | `MERGE` único: `APAGA`, `AJUSTA_FIM` e `INSERE` (com a busca dos atributos) | destino (1 *commit*) |
| 6 | `DROP TABLE IF EXISTS` das tabelas de trabalho | — |

- Uma falha antes do passo 5 não altera o destino. A reexecução recria as tabelas de trabalho
  (`CREATE OR REPLACE`), então uma tabela de trabalho que sobrou não atrapalha nada.
- Uma falha **durante** o passo 5 também não altera nada, porque o `MERGE` é uma transação Delta. A regra de
  retomada é rodar de novo, com qualquer janela.
- Tabelas de trabalho no schema `silver_intermediate` (o runner da amostra as troca para
  `amostra_silver_intermediate`). O `gerar_job.py` (`RE_ESCRITA`) vai listá-las como produzidas pela task
  `si_<t>`. Isso só mexe na ordem do YAML; nenhum DQ ou SQL de outra camada deve ler `tmp_*` (R23).
- O bloco `params` se repete em cada instrução, porque cada uma é independente.

### 3.3 Fase 1 — detecção (exemplo: `estabelecimentos`; vale para `empresas` e `dados_simples`)

```sql
CREATE OR REPLACE TABLE tcc_receita.silver_intermediate.tmp_scd2_acoes_estabelecimentos AS
WITH params AS (
    SELECT
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
),
-- (a) Bronze da janela: só chave, mês, carimbo e hash.
bronze_estreita AS (
    SELECT b.pk, b.mes_referencia AS mes, 'B' AS origem,
           b.bronze_processed_at AS ts, b.change_hash AS h,
           CAST(NULL AS STRING) AS valid_until_atual
    FROM tcc_receita.bronze.estabelecimentos b CROSS JOIN params p
    WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim AND b.pk IS NOT NULL
),
-- (b) Versões que a janela pode alterar: abertas ou terminando em mes_ini ou depois.
--     Versão com valid_until < mes_ini tem sucessora antes da janela: nada nela muda.
--     No incremental normal (janela depois de tudo), são só as versões correntes.
silver_estreita AS (
    SELECT s.pk, s.valid_since AS mes,
           CASE WHEN s.valid_since BETWEEN p.mes_ini AND p.mes_fim THEN 'S_DENTRO' ELSE 'S_FORA' END AS origem,
           CAST(NULL AS TIMESTAMP) AS ts, s.change_hash AS h,
           s.valid_until AS valid_until_atual
    FROM tcc_receita.silver_intermediate.estabelecimentos s CROSS JOIN params p
    WHERE s.valid_until IS NULL OR s.valid_until >= p.mes_ini
),
-- (c) A ÚNICA troca de dados: tudo da mesma pk vai para a mesma tarefa.
uniao AS (
    SELECT * FROM (SELECT * FROM bronze_estreita UNION ALL SELECT * FROM silver_estreita) u
    DISTRIBUTE BY pk
),
-- (d) B16: uma linha da bronze por (pk, mês), mesmo desempate de hoje (hash completo).
dedup AS (
    SELECT * FROM (
        SELECT u.*, ROW_NUMBER() OVER (PARTITION BY pk, mes, origem
                                       ORDER BY ts DESC NULLS LAST, h DESC) AS rn
        FROM uniao u
    ) x WHERE origem NOT IN ('B', 'B_CTX') OR rn = 1        -- B_CTX só existe em socios (§3.6)
),
-- (e) Linha do tempo nova. Candidatas = versões fora da janela + bronze da janela.
--     Versões existentes DENTRO da janela não são candidatas: são o "antes" a comparar.
com_anterior AS (
    SELECT d.*, origem IN ('S_FORA', 'B') AS eh_candidata,
           LAG(h) OVER (PARTITION BY pk, origem IN ('S_FORA', 'B') ORDER BY mes) AS h_anterior
    FROM dedup d
),
com_abertura AS (
    SELECT c.*, (eh_candidata AND h_anterior IS DISTINCT FROM h) AS abre_versao FROM com_anterior c
),
com_vigencia AS (   -- valid_until = início da próxima versão (exclusivo); NULL = atual
    SELECT a.*, LEAD(mes) OVER (PARTITION BY pk, abre_versao ORDER BY mes) AS valid_until_novo
    FROM com_abertura a
),
-- (f) Pareamento no mesmo (pk, mês): versão existente dentro da janela × versão nova.
--     '9999-12' só distingue "versão nova aberta" (fim NULL) de "não há versão nova".
pareada AS (
    SELECT v.*,
        MAX(CASE WHEN origem = 'S_DENTRO' THEN h END) OVER (PARTITION BY pk, mes) AS h_existente,
        MAX(CASE WHEN origem = 'B' AND abre_versao THEN h END) OVER (PARTITION BY pk, mes) AS h_nova,
        MAX(CASE WHEN origem = 'B' AND abre_versao THEN COALESCE(valid_until_novo, '9999-12') END)
            OVER (PARTITION BY pk, mes) AS fim_nova
    FROM com_vigencia v
),
acoes AS (
    SELECT pk, mes, origem, h, ts,
        CASE
          WHEN origem = 'S_FORA'   AND NOT abre_versao                                          THEN 'APAGA'
          WHEN origem = 'S_FORA'   AND valid_until_novo IS DISTINCT FROM valid_until_atual       THEN 'AJUSTA_FIM'
          WHEN origem = 'S_DENTRO' AND (h_nova IS NULL OR h_nova <> h)                           THEN 'APAGA'
          WHEN origem = 'S_DENTRO' AND NULLIF(fim_nova, '9999-12') IS DISTINCT FROM valid_until_atual THEN 'AJUSTA_FIM'
          WHEN origem = 'B' AND abre_versao AND (h_existente IS NULL OR h_existente <> h)        THEN 'INSERE'
        END AS acao,
        CASE WHEN origem = 'S_DENTRO' THEN NULLIF(fim_nova, '9999-12') ELSE valid_until_novo END AS valid_until
    FROM pareada
)
SELECT pk,
       mes                                        AS valid_since,
       acao,
       valid_until,
       valid_until IS NULL                        AS is_current,
       CASE WHEN origem <> 'B' THEN h END         AS hash_atual,      -- identifica a versão existente no MERGE
       mes                                        AS mes_origem,      -- onde buscar os atributos (INSERE)
       CASE WHEN origem = 'B' THEN h END          AS hash_origem,
       CASE WHEN origem = 'B' THEN ts END         AS ts_origem
FROM acoes WHERE acao IS NOT NULL;
```

Por que funciona:
- As versões existentes fora da janela entram como pontos da linha do tempo, no mês `valid_since`, com o hash
  delas, como o `historico_intacto` de hoje. O `LAG` enxerga a versão anterior à janela e as posteriores.
- `PARTITION BY pk, <expressão>` faz o `LAG`/`LEAD` ignorar as linhas não candidatas, sem nova troca: a troca
  por `pk` já agrupou tudo, e o resto é ordenação local.

### 3.4 Tabela de ações

| Linha | Condição | Ação | Quando acontece |
|---|---|---|---|
| versão fora da janela | o hash ficou igual ao da nova anterior | `APAGA` | reprocessar mês do meio e a 1ª versão posterior virar redundante |
| versão fora da janela | o fim mudou | `AJUSTA_FIM` | caso típico do incremental: a versão corrente é fechada no mês em que a pk mudou |
| versão dentro da janela | não há versão nova no mês, ou o hash é outro | `APAGA` | reprocessamento; a substituição é `APAGA` + `INSERE` |
| versão dentro da janela | mesmo hash, fim diferente | `AJUSTA_FIM` | reprocessar parte de uma janela já processada |
| bronze da janela | abre versão e não há existente igual no mês | `INSERE` | toda versão nova |
| qualquer | nada mudou | (nenhuma) | reexecução: **zero ações** |

### 3.5 Fase 2 — meses densos, guarda e `MERGE` único

**Instrução 3 — meses densos.** Um mês é denso quando mais de 20% das linhas da bronze daquele mês abrem
versão. No mês denso não se monta a lista de chaves; o mês entra inteiro.
```sql
CREATE OR REPLACE TABLE tcc_receita.silver_intermediate.tmp_scd2_meses_estabelecimentos AS
WITH params AS (...),
bronze_mes AS (
    SELECT b.mes_referencia AS mes, COUNT(*) AS n_bronze
    FROM tcc_receita.bronze.estabelecimentos b CROSS JOIN params p
    WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim      -- socios: desde add_months(mes_ini, -1)
    GROUP BY b.mes_referencia
),
novas_mes AS (
    SELECT mes_origem AS mes, COUNT(*) AS n_insere
    FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_estabelecimentos
    WHERE acao = 'INSERE' GROUP BY mes_origem
)
SELECT b.mes, b.n_bronze, COALESCE(n.n_insere, 0) AS n_insere,
       COALESCE(n.n_insere, 0) > 0.2 * b.n_bronze AS denso
FROM bronze_mes b LEFT JOIN novas_mes n ON b.mes = n.mes;
```

**Instrução 4 — guarda de memória.** Números nas mensagens vão com `CAST(... AS STRING)` (convenção do
`dq_gold.sql:5`, por causa do ANSI do warehouse). O texto das mensagens **não** pode conter as palavras
`INSERT` ou `MERGE`: `run_sql.py:97` deixaria de executar a asserção (R15).
```sql
SELECT ASSERT_TRUE(
    SUM(CASE WHEN NOT denso THEN n_insere ELSE 0 END) <= 25000000,
    CONCAT('Versoes novas em meses esparsos: ',
           CAST(SUM(CASE WHEN NOT denso THEN n_insere ELSE 0 END) AS STRING),
           ' (limite 25 M para a lista difundida). Rode a janela em blocos menores.'))
FROM tcc_receita.silver_intermediate.tmp_scd2_meses_estabelecimentos;
```
São 25 M chaves de 8 bytes, ≈ 600 MB difundidos, aceitável para r5d.xlarge (32 GB) e driver m5d.xlarge (16 GB).
Com ~1 M versões novas por mês, um bloco de 12 meses fica em ~12 M.

**Instrução 5 — `MERGE` único.** A fonte une as ações estreitas sobre versões existentes com as versões novas,
já com os atributos buscados na bronze. A condição `ON` inclui o hash da versão existente (`hash_atual`). As
linhas `INSERE` têm `hash_atual` NULL e por isso **nunca casam**, nem numa substituição, em que `APAGA` e
`INSERE` têm a mesma `(pk, valid_since)`. Assim, cada linha do destino casa com no máximo uma linha da fonte.

```sql
MERGE INTO tcc_receita.silver_intermediate.estabelecimentos AS t
USING (
    -- (1) ações sobre versões existentes: só chave, fim e hash de identificação
    SELECT a.pk, a.valid_since, a.acao, a.hash_atual, a.valid_until, a.is_current,
           CAST(NULL AS STRING) AS cnpj_basico, ...,            -- demais atributos NULL (mesmos tipos do destino)
           CAST(NULL AS TIMESTAMP) AS inserted_at, CAST(NULL AS STRING) AS change_hash
    FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_estabelecimentos a
    WHERE a.acao IN ('APAGA', 'AJUSTA_FIM')
    UNION ALL
    -- (2) versões novas: atributos da bronze, limpeza de hoje
    SELECT n.pk, n.valid_since, 'INSERE' AS acao, CAST(NULL AS STRING) AS hash_atual, n.valid_until, n.is_current,
           NULLIF(TRIM(b.cnpj_basico), '') AS cnpj_basico,
           ...,                                                  -- EXATAMENTE as expressões de bronze_limpo (81–132)
           b.bronze_processed_at AS inserted_at, b.change_hash
    FROM (
        -- um único scan da bronze da janela: mês denso passa inteiro; mês esparso só se a chave
        -- (pk, mês) estiver na lista difundida. Nenhuma linha larga é embaralhada aqui.
        SELECT /*+ BROADCAST(m), BROADCAST(k) */ b.*
        FROM tcc_receita.bronze.estabelecimentos b
        CROSS JOIN params p
        JOIN tcc_receita.silver_intermediate.tmp_scd2_meses_estabelecimentos m
          ON b.mes_referencia = m.mes
        LEFT JOIN (
            SELECT DISTINCT xxhash64(a.pk, a.mes_origem) AS chave
            FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_estabelecimentos a
            JOIN tcc_receita.silver_intermediate.tmp_scd2_meses_estabelecimentos mm
              ON a.mes_origem = mm.mes AND NOT mm.denso
            WHERE a.acao = 'INSERE'
        ) k ON xxhash64(b.pk, b.mes_referencia) = k.chave
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim       -- poda estática de partição
          AND (m.denso OR k.chave IS NOT NULL)
    ) b
    -- join exato, já com poucas linhas (fora o mês denso): descarta falso positivo do xxhash64 e traz a vigência
    JOIN (SELECT * FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_estabelecimentos WHERE acao = 'INSERE') n
      ON  b.pk = n.pk
      AND b.mes_referencia = n.mes_origem
      AND b.change_hash = n.hash_origem
      AND b.bronze_processed_at <=> n.ts_origem
) AS s
ON t.pk = s.pk AND t.valid_since = s.valid_since AND t.change_hash = s.hash_atual
WHEN MATCHED AND s.acao = 'APAGA' THEN DELETE
WHEN MATCHED AND s.acao = 'AJUSTA_FIM' THEN UPDATE SET
    t.valid_until = s.valid_until,
    t.is_current  = s.is_current,
    t.updated_at  = current_timestamp()
WHEN NOT MATCHED AND s.acao = 'INSERE' THEN INSERT (
    pk, cnpj_basico, ..., data_situacao_especial,
    mes_referencia, inserted_at, updated_at, change_hash, valid_since, valid_until, is_current
) VALUES (
    s.pk, s.cnpj_basico, ..., s.data_situacao_especial,
    s.valid_since, s.inserted_at, current_timestamp(), s.change_hash, s.valid_since, s.valid_until, s.is_current
);
```
- O join exato usa `(pk, mês, hash, carimbo)`, a mesma linha que venceu o desempate do B16 na fase 1. Como
  `(pk, mes, change_hash)` é única na bronze (§2.1), sai **uma** linha por versão nova. Não há `ROW_NUMBER` de
  segurança, porque ele custaria mais uma troca larga do mês denso (R24). O DQ de unicidade de
  `(pk, valid_since)` (T0) acusa se isso deixar de valer.
- Sem a lista difundida e sem o mês denso, uma linha da bronze nunca entra na fonte. Se a dica `BROADCAST`
  for ignorada (tamanho), o resultado não muda; só o custo, e isso aparece no `EXPLAIN` (§3.8).

### 3.6 `socios.sql` — o que muda em relação ao padrão

Regra a mais: a **ausência** no snapshot do mês M, de uma relação presente em M−1, gera uma versão
`vinculo_ativo = FALSE` em M, com os atributos e o `inserted_at` de M−1 e com
`change_hash = SHA2(TO_JSON(NAMED_STRUCT('source_change_hash', <hash de M−1>, 'vinculo_ativo', FALSE)), 256)`.

1. **Instrução 1:** a validação de hoje (linhas 42–50 e 74–98) inteira, antes de qualquer escrita. Janela
   válida e `COUNT(DISTINCT mes_referencia)` em `[mes_ini−1, mes_fim]` igual ao esperado (sem o mês anterior
   quando `mes_ini = MIN(mes_referencia)` da bronze, a carga inicial de 2023-05). Opcionalmente, a checagem de
   snapshot parcial (D6).
2. **`bronze_estreita`** lê `[mes_ini−1, mes_fim]`, com `origem = 'B_CTX'` para `mes_ini−1` (só contexto) e
   `'B'` para a janela. **O `dedup` vale também para `B_CTX`** (filtro `origem NOT IN ('B','B_CTX') OR rn = 1`):
   hoje o desempate cobre o mês anterior (`socios.sql:61-73, 120-122`). Sem isso, duas linhas `B_CTX` da mesma
   `(pk, mês)` empatariam no `LEAD` seguinte, e a inativa sairia de uma delas ao acaso (R1).
3. **Próxima presença:** `LEAD(mes) OVER (PARTITION BY pk, origem IN ('B','B_CTX') ORDER BY mes) AS prox_presenca`.
4. **Pontos da linha do tempo** (cada linha vira 0, 1 ou 2 pontos; `pk`, `ts` e `h` continuam como colunas da
   linha de origem, sem passar pelo `INLINE`). O hash da inativa é calculado **só** para as linhas com
   ausência em seguida, depois da troca:
   ```sql
   pontos AS (
     SELECT pr.pk,
            CASE WHEN pr.origem IN ('B','B_CTX') THEN 'B' ELSE pr.origem END AS origem,
            pr.valid_until_atual,
            pr.h  AS hash_origem,        -- hash da linha da bronze de onde vêm os atributos
            pr.ts AS ts_origem,
            pt.mes, pt.h, pt.vinculo_ativo, pt.mes_origem
     FROM presenca pr CROSS JOIN params p
     LATERAL VIEW INLINE(FILTER(ARRAY(
         -- a própria linha: versões existentes e bronze da janela (B_CTX não é ponto)
         IF(pr.origem <> 'B_CTX',
            NAMED_STRUCT('mes', pr.mes, 'h', pr.h,
                         'vinculo_ativo', IF(pr.origem = 'B', TRUE, CAST(NULL AS BOOLEAN)),
                         'mes_origem', pr.mes),
            NULL),
         -- saída do QSA: presente em pr.mes, ausente no mês seguinte (que precisa ser <= mes_fim)
         IF(pr.origem IN ('B','B_CTX') AND pr.mes < p.mes_fim
            AND (pr.prox_presenca IS NULL
                 OR pr.prox_presenca > date_format(add_months(to_date(concat(pr.mes, '-01')), 1), 'yyyy-MM')),
            NAMED_STRUCT('mes', date_format(add_months(to_date(concat(pr.mes, '-01')), 1), 'yyyy-MM'),
                         'h', sha2(to_json(named_struct('source_change_hash', pr.h, 'vinculo_ativo', FALSE)), 256),
                         'vinculo_ativo', FALSE,
                         'mes_origem', pr.mes),
            NULL)
     ), x -> x IS NOT NULL)) pt
   )
   ```
   A regra é a de hoje: a inativa sai só no 1º mês ausente, e só para M dentro da janela.
5. Sobre `pontos` seguem os passos (e) e (f) da §3.3 (`com_anterior` … `pareada`, com `h` = hash do ponto).
   **A projeção final de socios é diferente da genérica** e usa as colunas `*_origem` de `pontos`, nunca
   `h`/`mes` (R4):
   ```sql
   SELECT pk, mes AS valid_since, acao, valid_until, valid_until IS NULL AS is_current,
          CASE WHEN origem <> 'B' THEN h END              AS hash_atual,
          CASE WHEN origem = 'B' THEN mes_origem END      AS mes_origem,     -- M−1 para a inativa
          CASE WHEN origem = 'B' THEN hash_origem END     AS hash_origem,    -- hash da linha de M−1, não o da inativa
          CASE WHEN origem = 'B' THEN ts_origem END       AS ts_origem,
          CASE WHEN origem = 'B' THEN vinculo_ativo END   AS vinculo_ativo
   FROM acoes WHERE acao IS NOT NULL;      -- 'acoes' de socios carrega também mes_origem, hash_origem, ts_origem, vinculo_ativo
   ```
6. **`MERGE`:** a busca lê a bronze de `[mes_ini−1, mes_fim]` e faz o join exato por
   `(pk, mes_origem, hash_origem, ts_origem)`. Uma mesma linha da bronze pode gerar duas versões (ativa em M−1
   e inativa em M); as duas entram, porque `valid_since` difere. Na fonte, os valores são:
   `vinculo_ativo = n.vinculo_ativo`;
   `change_hash = IF(n.vinculo_ativo, b.change_hash, sha2(to_json(named_struct('source_change_hash', b.change_hash, 'vinculo_ativo', FALSE)), 256))`;
   `mes_referencia = n.valid_since`; `inserted_at = b.bronze_processed_at`.
   No `WHEN MATCHED ... UPDATE`, socios mantém o comportamento de hoje.
   A tabela segue `PARTITIONED BY (is_current)`; o Delta move a linha de partição sozinho.

### 3.7 Por tabela

| Tabela | Particularidades | Risco |
|---|---|---|
| `dados_simples` | padrão puro; 10 atributos | baixo — **primeira** |
| `estabelecimentos` | padrão puro; 34 atributos; maior ganho | baixo (mesmo molde) |
| `empresas` | padrão puro; `capital_social` DOUBLE (expressão copiada) | baixo; ganho menor (linhas já estreitas) |
| `socios` | §3.6 | médio — **última** |

### 3.8 Regras de plano — conferir no **cluster do Job, sobre as tabelas reais**

A conferência fica num único notebook no cluster, na mesma subida da medição (T5). Não serve fazer no warehouse:
ele roda Photon e outro otimizador, e a bronze da amostra não é particionada (R13).
- Instrução 2: um único `Exchange hashpartitioning(pk, ...)` acima dos dois *scans*; as janelas aparecem só com
  `Sort`. Em socios, nenhum `Exchange` depois do `Generate`. Se surgir uma troca extra, ela é estreita: registrar
  nas notas, sem mudar o desenho.
- Instrução 5: um único *scan* da bronze, com `PartitionFilters` da janela. `BroadcastHashJoin` para `m` e `k`,
  **sem** `Exchange` entre o *scan* e esses joins. O join exato pode ter `Exchange` (os dois lados já são
  pequenos, fora o mês denso).
- Anotar se o `MERGE` materializou a fonte (`materializeSourceTimeMs` no `DESCRIBE HISTORY`) e a troca do
  `optimizeWrite`.

### 3.9 Compatibilidade com Spark 3.5 / DBR 15.4

A **única** lista de construções proibidas é a de `scripts/checar_sql_cluster.py`, para não haver duas listas
que divergem; os quatro arquivos devem passar nela. Itens já conhecidos: sem `TRY_TO_DATE` (usar
`CAST(try_to_timestamp(...) AS DATE)`, como hoje) e sem `OPTIMIZE ... ZORDER`. `QUALIFY` **é permitido**: o log
usa (`log_eventos_negocio.sql:203`) e rodou no cluster (R10). Não há `SET` de configuração (o warehouse recusa).
Funções usadas: `xxhash64`, `sha2`, `to_json`, `named_struct`, `inline`, `filter`, `LATERAL VIEW`, `IF`, `<=>`,
`IS DISTINCT FROM`, `LAG`/`LEAD`/`ROW_NUMBER`, `DISTRIBUTE BY`, dica `BROADCAST`, `CREATE OR REPLACE TABLE AS`
e `MERGE` com as três cláusulas. **ANSI:** a amostra roda com ANSI ligado (warehouse) e o cluster com ANSI
desligado. Nenhuma expressão nova depende disso, mas a igualdade provada na amostra não cobre dado sujo que só
exista na base real (R16).

## 4. Semântica preservada

| Invariante de hoje | Como se mantém |
|---|---|
| `[valid_since, valid_until)` em `'YYYY-MM'`; `valid_until` NULL = atual; `is_current = (valid_until IS NULL)` | `valid_until` = `LEAD(mes)` entre as versões que abrem; `is_current` derivado dele |
| Versão nova só quando o hash difere do anterior (`IS DISTINCT FROM`) | mesmo `LAG`, mesmo hash completo, mesmo conjunto de pontos (versões fora da janela + bronze da janela) |
| Versões fora da janela preservadas | entram como pontos; só recebem `AJUSTA_FIM`/`APAGA` quando o recálculo exige, como no `MERGE` de hoje |
| Correção de hash atualiza todos os atributos (D-04) | substituição = `APAGA` + `INSERE` com os atributos da bronze |
| B16: desempate `bronze_processed_at DESC NULLS LAST, change_hash DESC`, inclusive no mês de contexto de socios | mesmo `ROW_NUMBER`, aplicado a `B` e `B_CTX`; a busca usa exatamente a linha vencedora |
| Idempotência: reexecutar a mesma janela não muda nada | a fase 1 compara com o estado atual → zero ações → o `MERGE` não escreve linha nenhuma |
| Reprocessar mês do meio | versões dentro da janela são o "antes"; versões posteriores entram como pontos |
| 2023-05 = carga inicial (socios sem mês anterior e sem saídas no 1º mês) | mesma regra de validação; sem `B_CTX`, não há inativa no 1º mês |
| Socios: saída por ausência num snapshot completo; inativa com atributos, `inserted_at` e hash derivado de M−1 | §3.6, mesma regra e mesmo hash |
| Estabelecimentos/empresas/simples: pk que some da bronze não fecha a versão (C-10) | sem linha da bronze, a versão corrente não recebe ação |
| `inserted_at` = carimbo da bronze da versão; `updated_at` = momento da última alteração | `INSERE` igual a hoje; `AJUSTA_FIM` só muda `updated_at` |
| `mes_referencia` = `valid_since`; esquema e comentários das tabelas | idem; DDL inalterado |
| Escrita atômica (um `MERGE`) | mantida: continua um `MERGE` por arquivo |

**C-05 e C-06 (socios).** As correções já estão no código atual (commit `9ede9b4`), validadas só na amostra, e
são mantidas: não há bloco "legado" (C-05), e toda validação roda antes de qualquer escrita (C-06; com a bronze
vazia, a instrução 1 falha). A tabela real foi gerada em 2026-11-08 pelo código anterior. Na carga inicial
(tabela vazia), porém, o bloco `estados_inativos_legado` não gera linhas, e a Q5 mediu 0 versões fantasmas.
**Diferença esperada contra a tabela real: nenhuma.** Se aparecer, ela fica em pks cuja 1ª versão é inativa (Q5 da
`critica.md`).

**Diferenças intencionais.** Só aparecem com uma bronze diferente da que gerou a tabela; o cenário R30-6 as
mede.
1. Hoje, uma versão de um mês reprocessado que sumiu da bronze já é apagada, **se** a pk aparece em outro mês da
   janela (`estabelecimentos.sql:193-202`). A diferença do desenho novo vale só para pks **sem nenhuma** linha na
   janela: hoje ficam; no novo, são apagadas. Socios já se comporta como o novo.
2. Substituição por hash corrigido: o `inserted_at` passa a ser o da bronze nova. Hoje fica o antigo nos três
   não-socios.
3. Mês faltando na bronze dentro da janela: o novo falha na instrução 1 (D5). Hoje, os três não-socios seguem;
   as versões existentes desse mês sobrevivem só se a pk não aparecer em nenhum outro mês da janela.

## 5. Modos de execução

Os parâmetros não mudam: `{{mes_referencia_inicial}}` vazio = mês anterior; `{{mes_referencia_final}}` vazio
= o inicial. Um arquivo só serve a todos os modos.

| Modo | Como rodar | O que a fase 1 lê da silver | Ações típicas |
|---|---|---|---|
| Carga inicial / reconstrução (dev, D-08) | `DROP TABLE` (opcional) + `2023-05..fim do bloco` | nada | só `INSERE`; 1º mês denso |
| Bloco seguinte (6–12 meses) | `mes_ini` = mês seguinte ao último processado | versões correntes | `INSERE` + `AJUSTA_FIM` |
| Mensal | `mes_ini = mes_fim` = mês novo (ou vazio) | versões correntes | idem, ~1–2% das linhas |
| Reexecução (retry/repair) | mesma janela | correntes + as da janela | nenhuma |
| Mês do meio (produção) | janela no meio do histórico | versões que terminam em `mes_ini` ou depois | `APAGA`/`AJUSTA_FIM`/`INSERE` |

**Custo por execução (todas as parcelas):**
- leitura estreita da bronze dos meses do bloco + troca estreita dessas linhas (proporcional a meses × linhas/mês);
- leitura estreita das versões correntes da silver + troca estreita (fixa por execução, ~57 M em estabelecimentos);
- **leitura larga da bronze de todos os meses do bloco** (sem troca: o filtro por lista difundida descarta as
  linhas depois de lidas);
- troca larga só das linhas que viram versão (no mês denso, o mês inteiro);
- o `MERGE`: leitura das chaves do destino + **reescrita dos arquivos tocados**. Como as versões a fechar se
  espalham por todos os arquivos, isso é ≈ a tabela inteira **sem *deletion vectors***, e ≈ só o que mudou
  com eles (D4).

Logo, "proporcional ao que mudou" vale para as **trocas largas**, e para a escrita só com *deletion vectors*. A
parcela fixa (versões correntes) favorece blocos maiores; o limite da instrução 4 segura o máximo.

**Ordem com a reextração de `empresas` (D-16, fase 4):** a reextração muda o hash de todas as razões sociais
mascaradas. Feita depois da carga da janela, ela abriria uma onda de versões falsas num mês só. O desenho aguenta
(mês denso), mas o histórico ficaria errado. A reextração deve vir **antes** da reconstrução das SCD2 da janela
completa (R22).

## 6. Plano de validação

### 6.1 Amostra (1%, SQL warehouse, `scripts/amostra/rodar_no_warehouse.py`)

**Referência**, com o código atual (`main`), antes de trocar qualquer arquivo:
```bash
python scripts/amostra/rodar_no_warehouse.py --arquivos src/silver_intermediate/ingest/<t>.sql   # 2023-05..2023-08, tabela vazia
```
```sql
CREATE SCHEMA IF NOT EXISTS tcc_receita.amostra_ref;
CREATE OR REPLACE TABLE tcc_receita.amostra_ref.<t> DEEP CLONE tcc_receita.amostra_silver_intermediate.<t>;
```

**Bronze de teste** para os cenários que alteram a bronze. É sempre uma **cópia** da amostra, nunca a bronze
real:
`CREATE OR REPLACE TABLE tcc_receita.amostra_bronze_teste.<t> DEEP CLONE tcc_receita.amostra_bronze.<t>`. Para
rodar contra ela, troca-se `amostra_bronze` por `amostra_bronze_teste` no texto antes de executar (uma opção nova
no runner, `--bronze-teste`, ou uma cópia do arquivo em `/tmp`).

**Cenários (código novo).** "Do zero" = `DROP TABLE tcc_receita.amostra_silver_intermediate.<t>` antes.

| Id | Cenário | Esperado |
|---|---|---|
| C1 | do zero, bloco `2023-05..08` | igual à referência |
| C2 | do zero, mês a mês: 05, 06, 07, 08 | igual |
| C3 | do zero, blocos `05..06` + `07..08` | igual; em socios, conferir à parte que existem saídas e retornos em `07` (contexto `06` no bloco anterior). Se a amostra não tiver nenhum, o cenário não prova a fronteira, e isso é anotado |
| C4 | após C1, reexecutar `05..08` | igual; `DESCRIBE HISTORY`: `MERGE` com 0 linhas inseridas, atualizadas ou apagadas |
| C5 | após C1, reexecutar `06..07` e depois só `06` | igual |
| C6 | após C1, `DELETE ... WHERE valid_since = '2023-07'` **e** `UPDATE ... SET valid_until = NULL, is_current = TRUE WHERE valid_until = '2023-07'`, depois reexecutar `07` | igual (exercita `INSERE` **e** `AJUSTA_FIM`) |
| C7 | após C1, `UPDATE ... SET change_hash = repeat('0', 64) WHERE valid_since = '2023-07' AND pk IN (<100 pks>)` e reexecutar `07` | igual (substituição: `APAGA` + `INSERE`) |
| C8 | após C1, reexecutar só `05` | igual (versões posteriores como pontos) |
| C9 | janela `2030-01` (sem bronze) | instrução 1 falha; histórico sem versão nova |
| C10 | socios: após C4, Q5 (1ª versão inativa) | 0 |
| C11 | **B16 sintético** (bronze de teste): duplicar algumas linhas de socios em `2023-06` (contexto de `07`) e de estabelecimentos em `2023-07`, com `change_hash` diferente e o mesmo `bronze_processed_at`. Rodar o código atual e o novo sobre a mesma cópia e comparar | iguais entre si (pega R1) |
| C12 | **mês faltando no meio**: apagar `2023-07` da bronze de teste e rodar `05..08` | instrução 1 falha; nada gravado |
| C13 | **falha no meio**: rodar só as instruções 0–4 (sem o `MERGE`) e depois rodar a **janela seguinte** inteira | igual à referência daquela janela (o `MERGE` atômico não deixa estado intermediário; sobras de `tmp_*` não atrapalham) |
| C14 | **mês denso que não é o primeiro**: na bronze de teste, trocar o `change_hash` de todas as linhas de `2023-07` (`sha2(concat(change_hash,'x'),256)`) e rodar `07` sobre a tabela pronta, comparando com o código atual sobre a mesma cópia | iguais; `tmp_scd2_meses` marca `07` como denso |
| C15 | **guarda da instrução 4**: limite baixado para 10 numa cópia do arquivo | falha sem escrever |
| C16 | **reprocessamento com bronze diferente** (bronze de teste: remover de `2023-07` as linhas de 50 pks que só aparecem em `07` na janela `07..07`; trocar o hash de outras 50) e rodar `07` com o código atual e com o novo | diferenças **só** as da §4 (itens 1 e 2), contadas e anotadas |

**Comparação** (dois sentidos; `EXCEPT ALL` pega duplicatas; `updated_at` fica de fora porque é o momento da
escrita):
```sql
SELECT 'novo_menos_ref' AS lado, COUNT(*) AS n FROM (
    SELECT * EXCEPT (updated_at) FROM tcc_receita.amostra_silver_intermediate.<t>
    EXCEPT ALL
    SELECT * EXCEPT (updated_at) FROM tcc_receita.amostra_ref.<t>)
UNION ALL
SELECT 'ref_menos_novo', COUNT(*) FROM (
    SELECT * EXCEPT (updated_at) FROM tcc_receita.amostra_ref.<t>
    EXCEPT ALL
    SELECT * EXCEPT (updated_at) FROM tcc_receita.amostra_silver_intermediate.<t>);
-- esperado: 0 e 0
```
```sql
SELECT COALESCE(n.valid_since, r.valid_since) AS mes, n.versoes AS novo, r.versoes AS ref,
       n.correntes AS novo_corr, r.correntes AS ref_corr
FROM (SELECT valid_since, COUNT(*) AS versoes, COUNT_IF(is_current) AS correntes
      FROM tcc_receita.amostra_silver_intermediate.<t> GROUP BY valid_since) n
FULL OUTER JOIN
     (SELECT valid_since, COUNT(*) AS versoes, COUNT_IF(is_current) AS correntes
      FROM tcc_receita.amostra_ref.<t> GROUP BY valid_since) r
  ON n.valid_since = r.valid_since
ORDER BY mes;
```
```sql
-- T0: (pk, mes, change_hash) única na bronze (premissa do join exato da §3.5)
SELECT COUNT(*) AS linhas, COUNT(DISTINCT pk, mes_referencia, change_hash) AS distintas
FROM tcc_receita.amostra_bronze.<t>;                          -- esperado: iguais
```
O `dq_<t>.sql` passa em todos os cenários (com os testes novos da T0).

**Limites da amostra:** não prova compatibilidade com o cluster (fica para a checagem automática e para a T5),
nem desempenho. Os planos (§3.8) só valem no cluster. O B16 real quase não aparece numa amostra de 1%, e é por
isso que existe o C11.

### 6.2 Base real (medição; uma vez, ao fim das quatro tabelas; mesma execução descrita na fase 3)

1. **Preservar a tabela atual:** `CREATE TABLE tcc_receita.silver_intermediate_ref.<t> DEEP CLONE
   tcc_receita.silver_intermediate.<t>` para as quatro. Custa uma cópia (dezenas de GB no S3, alguns minutos de
   cluster), mas a referência fica independente da origem: um `SHALLOW CLONE` perderia os arquivos após o `DROP`
   ou um `VACUUM` da otimização preditiva (R18).
2. **Execução comparável com o B20:** `DROP` das quatro SCD2 e rodar as tasks `si_*` com `2023-05..2023-08`
   **de uma vez** (carga inicial, mesma janela da linha de base) (R17).
3. **Mês incremental, na mesma subida de cluster:** se a bronze tiver `2023-09`, rodar `09`. Se não tiver,
   voltar a tabela ao estado de "depois de 07" e rodar `08`:
   `DELETE ... WHERE valid_since = '2023-08'` e
   `UPDATE ... SET valid_until = NULL, is_current = TRUE WHERE valid_until = '2023-08'`. Isso reconstrói
   exatamente o estado de `05..07`, porque as versões fechadas em `08` eram as correntes naquele estado.
   Ao fim, a tabela deve continuar igual à referência.
4. **Comparação:** as consultas da §6.1 trocando `amostra_*` por `silver_intermediate` / `silver_intermediate_ref`,
   num `.sql` rodado **no cluster** (com 60 M linhas, o warehouse 2X-Small seria lento). Esperado: 0 e 0.
5. **Medir, por task `si_*` e por DQ das SCD2** (o DQ também cresce com o histórico): duração
   (`system.lakeflow.job_task_run_timeline`), bytes lidos, shuffle, *spill* (Spark UI / logs em
   `/Volumes/tcc_receita/ops/cluster_logs`), `DESCRIBE HISTORY` do `MERGE` (`numTargetRowsInserted/Updated/Deleted/Copied`,
   `numTargetFilesAdded/Removed`, `materializeSourceTimeMs`, `numDeletionVectorsAdded`). Fazer também o
   `EXPLAIN FORMATTED` da §3.8. Registrar em `docs/medicoes/`.
6. Depois, log → silver_final → gold uma vez, com os DQ, como sanidade.

## 7. Ganho esperado e riscos

### 7.1 Ganho por mudança (qualitativo; números na T5)

| Mudança | O que elimina | Efeito |
|---|---|---|
| Tirar o `JOIN pks_afetadas` e a 2ª leitura larga da bronze | 1 leitura larga + 1 troca larga (item 2 da §2.2) | ~⅓ do shuffle de hoje |
| Detecção estreita (hash completo) | as trocas largas da união e da janela viram uma troca de ~45% da largura | ~2× menos bytes nessa troca; ordenações locais mais leves |
| Comparação por janela em `(pk, mês)` | o `FULL OUTER JOIN` por `(pk, valid_since)` | −1 troca larga |
| Fonte do `MERGE` só com o que muda | o join de ~100% do destino com a fonte | o `MERGE` casa ~1–2% das linhas no incremental |
| Busca com lista difundida | troca larga da bronze na busca | a bronze larga é lida, mas não embaralhada (fora o mês denso) |
| Limpeza depois do filtro | regex em 4 colunas × 57 M linhas/mês | CPU ~50× menor nos meses esparsos |
| Socios: ausência por janela | o auto-join largo `anterior × atual` | −1 troca larga |
| (D4) *Deletion vectors* | reescrita dos arquivos tocados pelo `MERGE` (≈ a tabela) | decisivo no mensal em produção |

**Ordem de grandeza** (`si_estabelecimentos`, 4 meses, carga inicial; hoje 97 GB de shuffle). Troca estreita
de 228 M linhas a ~80 bytes ≈ 18 GB. A fonte do `MERGE` com o mês denso (~59 M versões largas) entra no join
exato, no próprio join do `MERGE` e no `optimizeWrite`, somando ≈ 25–30 GB. Total ≈ **45 GB, ~2× menos**. A
leitura larga da bronze passa a ser uma só. É na **carga inicial** que o desenho ganha menos, porque quase tudo é
versão nova.
**Mês incremental** (o que se repete ~36 vezes): troca estreita de ~57 M linhas da bronze + ~57 M versões
correntes ≈ 9 GB; parte larga ~1 M linhas; mais a reescrita do destino pelo `MERGE` sem DV. Hoje, cada mês
custa ~24 GB de troca larga mais o join do `MERGE` com a tabela inteira.

### 7.2 Riscos

| Risco | Mitigação |
|---|---|
| Otimizador cria troca extra (`UNION`, janela com expressão, `Generate`) | `EXPLAIN` no cluster (§3.8); a troca extra seria estreita |
| Lista difundida grande demais (bloco longo, muitos hashes mudando) | mês denso entra inteiro; guarda da instrução 4; blocos ≤ 12 meses |
| `MERGE` único materializa a fonte e faz join com as inserções (custo na carga inicial) | é o preço da atomicidade (D3); medido na T5 (`materializeSourceTimeMs`) |
| `optimizeWrite` (conf do cluster) soma uma troca antes da escrita | medir; se pesar, propriedade de tabela `delta.autoOptimize.optimizeWrite = false`, mas só depois de confirmar que a propriedade vence a conf de sessão do cluster (`gerar_job.py:87`; R12). Se não vencer, tirar a conf do cluster e ligar por tabela |
| `(pk, mes, change_hash)` deixar de ser única na bronze (por exemplo, se o B17 tirar o `dropDuplicates`) | DQ de unicidade de `(pk, valid_since)` (T0) falha alto; quem mudar a bronze reavalia esta premissa |
| A amostra não pega incompatibilidade com Spark 3.5 nem o B16 real | checagem automática + C11 sintético + T5 |
| Pseudo-SQL desta spec com erro de detalhe | os cenários C1–C16 cobrem cada caminho da tabela de ações |

## 8. Tarefas (uma tabela por vez; commit próprio cada)

- [x] **T0 — Preparação** (sem escrita na base real)
  - Reautenticar o CLI e rodar, num lote no warehouse: Q1 `DESCRIBE DETAIL` + `SHOW TBLPROPERTIES` das bronzes e
    SCD2 (inclusive se `delta.enableDeletionVectors` **já** está ligado: nesse caso a D4 já está decidida); Q2
    `SELECT valid_since, COUNT(*)` por SCD2 (calibra os 20% do mês denso); Q6 `DESCRIBE HISTORY` das SCD2; D1/D2
    da revisão (tamanho do B16 por mês; pks sem corrente e versões fechadas sem sucessora nas tabelas reais).
  - Na amostra: referência + `DEEP CLONE` (§6.1); bronze de teste; unicidade de `(pk, mes, change_hash)`.
  - **DQ** de `empresas`, `estabelecimentos` e `dados_simples`: acrescentar `sem_corrente` (toda pk com
    exatamente uma versão corrente), continuidade com `IS DISTINCT FROM` (pega versão fechada sem sucessora) e
    unicidade de `(pk, valid_since)`, copiando o molde de `dq_socios.sql:19-42`. Esta é a única tarefa de
    contiguidade; a fase 3 depende dela (R19).
  - **Aceite:** Q1/Q2 anotadas na §11; DQ novos passam na referência; unicidade confirmada.
- [x] **T1 — `dados_simples.sql`.** **Aceite:** C1–C9, C12–C15 com 0/0 e contagens iguais; DQ passa;
  `checar_sql_cluster.py` passa.
- [x] **T2 — `estabelecimentos.sql`.** **Aceite:** igual à T1 + C11 e C16.
- [x] **T3 — `empresas.sql`.** **Aceite:** igual à T1.
- [x] **T4 — `socios.sql`.** **Aceite:** igual à T1 + C10, C11 (socios) e a conferência de fronteira do C3;
  com o mês anterior faltando, a instrução 1 falha; `dq_socios` passa.
- [ ] **T5 — Medição e planos na base real** (§6.2 e §3.8), com o usuário ligando NAT/cluster. **Aceite:**
  EXCEPT 0/0 contra o `DEEP CLONE`, depois de `05..08` e depois do mês incremental; números de `05..08` lado a
  lado com o B20 (meta da guarda-chuva: cadeia silver → gold com pelo menos metade do tempo; para as `si_*`,
  shuffle pelo menos 2× menor); custo de um mês incremental registrado.
- [ ] **T6 — Documentação:** `docs/specs/silver_intermediate.md`, `src/silver_intermediate/readme.md`,
  `docs/decisions.md` (D-18 "SCD2 em duas fases", com D1, D3–D8), `docs/backlog.md` (B20 parte silver) e
  sugestão em `docs/tcc.md`, se o texto descreve o `MERGE`.
- [ ] ~~**T7 — (se D4 = sim)** ligar *deletion vectors*~~ — **sem objeto**: já estão ligados nas quatro SCD2
  (T0, Q1). Resta anotar `numDeletionVectorsAdded` do `MERGE` na T5.

## 9. Decisões para o usuário

| Id | Escolha | Opções | Recomendação |
|---|---|---|---|
| D1 | Desenho | (a) **duas fases** (esta spec); (b) uma passada larga: o mesmo fluxo de ações, mas a bronze larga atravessa a troca uma vez e sai já com os atributos | **(a)**. No incremental, a troca larga cai de ~57 M para ~1 M linhas por mês. (b) é mais simples (sem instruções 3–4, sem lista difundida) e já corta ~⅔ do shuffle de hoje; fica como plano B se a lista difundida der problema. **O que se perde com (b):** cada mês incremental embaralha a bronze larga inteira |
| D2 | ~~Prefixo de 16 caracteres do hash~~ | — | **retirada**: decisão do gerente, hash completo (mais fácil de defender; a troca estreita fica ~1,5× maior) |
| D3 | Escrita | (a) **`MERGE` único** (atômico); (b) `MERGE` estreito + `INSERT` separados (dois *commits*) | **(a)**. (b) poupa na carga inicial uma troca larga das versões novas, mas uma falha entre os dois deixa pks sem versão corrente. Uma execução seguinte de outra janela transformaria isso num buraco no histórico e em "primeiras aparições" falsas no log (R3). **O que se perde com (a):** na carga inicial, as versões novas passam pelo join do `MERGE` (≈ 1 troca larga a mais, uma vez por reconstrução) |
| D4 | *Deletion vectors* nas SCD2 — **perdeu o objeto: já ligados (T0, Q1)** | ligar ou não | **ligar depois da T5**, se o DBR 15.4 os usar no `MERGE` (`numDeletionVectorsAdded`) e se nenhum leitor externo quebrar. É reversível (`ALTER TABLE ... DROP FEATURE deletionVectors`, DBR 14.1+). Conferir na T0 se já estão ligados |
| D5 | Mês faltando na bronze dentro da janela | (a) falhar (todas as tabelas); (b) seguir (hoje, nos três não-socios) | **(a)**: igual a socios; um mês faltando é quase sempre erro de ingestão, e falhar antes de escrever é mais seguro |
| D6 | Socios: snapshot parcial | acrescentar à instrução 1 "linhas(M) ≥ 95% de linhas(M−1)" ou deixar só o DQ de saídas (5%) | **acrescentar**: uma contagem por partição impede gravar saídas falsas, em vez de só acusá-las depois |
| D7 | Diferenças intencionais 1 e 2 da §4 | aceitar ou replicar o antigo | **aceitar**: só ocorrem com bronze alterada, e o novo é o consistente (igual a socios) |
| D8 | Tamanho do bloco | 6 ou 12 meses | **12** no piloto (a guarda da instrução 4 protege); 6 se a guarda falhar ou o bloco passar de ~1 h |

## 10. Alternativas consideradas

- **Uma passada larga (D1-b):** ver D1.
- **Prefixo de 64 bits do hash:** linha estreita ~40% menor, mas exige defender colisão e o desempate por prefixo.
  Retirado (D2).
- **`MERGE` estreito + `INSERT` (D3-b):** mais barato só na carga inicial; perde a atomicidade.
- **`ROW_NUMBER` de segurança na busca:** custava mais uma troca larga do mês denso para cobrir um caso que a
  bronze já elimina (`dropDuplicates`); trocado por DQ de unicidade (R24).
- **Dois ramos na busca (denso `UNION ALL` esparso com `IN (subquery)`):** dependia de poda dinâmica para não
  ler a janela larga duas vezes; trocado por um único *scan* com joins difundidos (R14).
- **`INSERT OVERWRITE` da tabela inteira a cada execução:** custo cresce com o histórico.
- **Laço mês a mês em Python:** tira o SQL do formato do Job e multiplica o custo fixo.
- **Clustering/partição da silver para podar o `MERGE`:** as mudanças se espalham por todas as pks; o ganho real
  vem das *deletion vectors* (D4).
- **`CACHE TABLE` / *temp view*:** não funcionam no runner da amostra.

## 11. Notas de execução

- 2026-11-16: consultas não rodadas — CLI Databricks com *refresh token* inválido (bloqueio da linha de base).
  Volumes da §2.1 vêm das contagens de 2026-11-15.
- 2026-11-16: spec revista após a revisão adversarial (§12). Mudanças de desenho: hash completo, `MERGE` único
  atômico, busca num só *scan*, sem `rn_busca`, dedup incluindo `B_CTX`.
- 2026-11-16 — **T0 (tabelas reais, só leitura, warehouse):**
  - Q1: as 4 bronzes são particionadas por `mes_referencia`; **as 4 SCD2 e as 4 bronzes já têm
    `delta.enableDeletionVectors = true`** (padrão do workspace). A **D4 perdeu o objeto**: não há nada a ligar
    (a T7 vira só "medir `numDeletionVectorsAdded` na T5"). `empresas` e `estabelecimentos` da silver têm
    *clustering* automático (`clusterByAuto = true`, colunas escolhidas pela otimização preditiva:
    `valid_until, valid_since` e `valid_until, identificador_matriz_filial, valid_since`), e o histórico mostra
    `OPTIMIZE`/`VACUUM` automáticos: reforça o `DEEP CLONE` da §6.2. `dados_simples` e `socios` sem clustering.
  - Q2 (versões por `valid_since` ÷ linhas da bronze do mês): 06–08 abrem 0,7%–1,7% (empresas 0,7–0,9%,
    estabelecimentos 1,4–1,7%, dados_simples 1,3–1,5%, socios 1,7–2,3%). O limiar de 20% do mês denso separa
    com folga o 1º mês (100%) dos demais.
  - Q6: o `MERGE` da carga inicial gastou quase todo o tempo materializando a fonte
    (`materializeSourceTimeMs` / `executionTimeMs`: dados_simples 1555/1682 s, socios 1336/1494 s, empresas
    365/436 s).
  - D1 da revisão (B16): linhas a mais que pks distintas por mês: empresas 2–3, socios 6, estabelecimentos e
    dados_simples 0. Na amostra de 1%: 0 nas quatro (por isso o C11 sintético).
  - D2 da revisão: nas 4 SCD2 reais, 0 pks sem exatamente uma corrente e 0 versões descontínuas.
  - Unicidade de `(pk, mes, change_hash)` na bronze da amostra: confirmada nas 4 (linhas = distintas; também
    `(pk, mes)` única e nenhuma `pk` nula).
- 2026-11-16 — **T0 (amostra):** referência gerada com o código anterior (`05..08`, tabela vazia) em
  `tcc_receita.amostra_f2` e copiada (`DEEP CLONE`) para `tcc_receita.amostra_f2_ref`. Os DQ novos passam nas 4
  referências. Versões/correntes: dados_simples 367.669/362.213; empresas 549.779/546.853; estabelecimentos
  593.991/579.547; socios 242.868/234.865.

- 2026-11-16 — **T1–T4 (amostra, warehouse, código novo × referência; "OK" = `EXCEPT ALL` 0 nos dois
  sentidos, contagens por `valid_since` iguais e DQ novo passando):**

  | Cenário | dados_simples | estabelecimentos | empresas | socios |
  |---|---|---|---|---|
  | C1–C8 | OK | OK | OK | OK |
  | C4 (`MERGE` da reexecução: inseridas/atualizadas/apagadas) | 0/0/0 | 0/0/0 | 0/0/0 | 0/0/0 |
  | C6 (`MERGE`: inseridas/atualizadas) | 4.681/1.700 | 7.706/4.403 | 4.077/882 | 4.194/2.420 |
  | C7 (`MERGE`: inseridas/apagadas) | 100/100 | 100/100 | 100/100 | 100/100 |
  | C9, C12 (falha na instrução 1, nada gravado) | OK | OK | OK | OK |
  | C11 (B16 sintético, 200 pks, mesmo carimbo) | OK | OK | OK | OK (mês de contexto 06) |
  | C13 (instruções 0–4 sem `MERGE`, depois a janela) | OK | OK | OK | OK |
  | C14 (07 todo com hash novo; `tmp_scd2_meses` marca 07 denso) | OK | OK | OK | OK |
  | C15 (limite 10 na instrução 4: falha na instrução 4, nada gravado) | OK | OK | OK | OK |
  | C16 (bronze diferente) | — | só diferença 1 da §4 | — | 0 diferenças |

  Linhas da referência: dados_simples 367.669; estabelecimentos 593.991; empresas 549.779; socios 242.868.
  **C16 em estabelecimentos:** todas as diferenças estão nas 50 pks removidas de 07 (diferença 1): o antigo
  mantém 79 linhas que o novo não tem (as 50 versões de 07 + fins ajustados), o novo tem 29 linhas
  (vigências reajustadas). Nas 50 pks com hash trocado, 0 diferenças (a diferença 2 não aparece porque a cópia
  mantém o mesmo `bronze_processed_at`). Observação: no novo, uma pk cuja única versão começa no mês
  reprocessado e que some dele fica sem versão mesmo existindo no mês seguinte da bronze; reprocessar até o
  fim do histórico resolve (é a semântica aceita na D7).
  **Socios:** C3 na fronteira tem 639 saídas e 1 retorno em 07 (contexto 06 no bloco anterior), então o
  cenário prova a fronteira; C10 / Q5 = 0 primeiras versões inativas; com 06 apagado da bronze, a janela
  `07..08` falha na instrução 1; com 10% das linhas de 07 apagadas, `05..08` falha na instrução 1 (D6).
  **DQ novos:** disparam em cópias quebradas de propósito (pk sem corrente, versão descontínua, versão
  repetida). `checar_sql_cluster.py` passa nos quatro arquivos. O gerador do Job não muda (`gerar_job --checar`:
  as `tmp_scd2_*` são escritas e lidas no mesmo arquivo e não entram como dependência).

## 11.1 Desvios da implementação (T0–T4)

Nenhum muda o resultado; todos foram validados pelos cenários da §6.1.
1. **Instrução 1 sem `to_date`:** a contagem esperada de meses usa `CAST(try_to_timestamp(mes, 'yyyy-MM') AS DATE)`,
   que devolve NULL (e faz a asserção falhar) com ANSI ligado ou desligado; `to_date` de mês inválido daria erro
   só com ANSI. O mesmo vale para o mês anterior (`mes_ant`) e o mês seguinte da inativa em socios.
2. **Socios, instrução 1:** a guarda `EXISTS` antiga virou redundante (a contagem de meses já falha com a
   bronze vazia, e a validação agora é instrução própria, fora do `MERGE`). A D6 foi implementada como
   "cada mês de `[mes_ini−1, mes_fim]` tem ≥ 95% das linhas do mês anterior" (`LAG` sobre ≤ 13 contagens por
   partição), na mesma instrução.
3. **Socios, `presenca`:** o mês seguinte é calculado uma vez como coluna (`mes_seguinte`) em vez de repetido
   dentro do `INLINE`; o resto da §3.6 está como escrito.
4. **DQ de unicidade de `(pk, valid_since)`** também em `dq_socios` (a premissa do `MERGE` vale para as quatro).
5. **Arquivos com 9 instruções** (o `CREATE SCHEMA` e o `CREATE TABLE` são duas), não 7.
6. **Ambiente de validação:** em vez de `amostra_ref`/`amostra_bronze_teste`, os schemas `amostra_f2` (candidatas
   e tabelas de trabalho), `amostra_f2_ref` (referência) e `amostra_f2_bt` (bronze de teste), para não tocar
   nas tabelas `amostra_*` que a fase 3 lê. O runner ganhou `--schemas camada=amostra_x,...`. A referência foi
   regenerada com o código anterior (não reaproveitada de `amostra_silver_intermediate`).
7. **C13** foi rodado como: do zero `05..06`; instruções 0–4 de `07..08` (sem o `MERGE`); `07..08` inteiro.
8. **C11** rodou nas quatro tabelas (o B16 real existe em empresas e socios, D1 da T0), com 200 pks duplicadas,
   mesmo `bronze_processed_at`, `change_hash` e um atributo diferentes. **C16** rodou em estabelecimentos e,
   como evidência extra, em socios.

## 12. Achados da revisão → o que foi feito

Só os achados que tocam esta spec; os demais (R2, R6, R9, R11, R20, R26, R27, R31 e a parte de F3 do R28) são da
fase 3.

| Achado | Procede? | O que foi feito |
|---|---|---|
| R1 — `B_CTX` sem desempate B16 | sim (`socios.sql:61-73, 120-122` deduplicam o mês anterior) | `dedup` com `origem NOT IN ('B','B_CTX') OR rn = 1` (§3.3, §3.6.2); cenário C11 |
| R3 — falha entre `MERGE` e `INSERT` deixa pk sem corrente | sim | voltou ao **`MERGE` único** (D3), que não tem estado intermediário; DQ `sem_corrente` + continuidade `IS DISTINCT FROM` nos três não-socios (T0); C13. A guarda "tmp ainda existe" ficou desnecessária: com escrita atômica, a sobra de `tmp_*` é inofensiva (é recriada) |
| R4 — `acoes` de socios com `*_origem` errados | sim | projeção de socios escrita por extenso (§3.6.5); `hash_origem` = hash da linha de M−1 |
| R5 — texto da D5 | sim (`estabelecimentos.sql:193-202` já apaga, se a pk aparece em outro mês) | §4, diferenças 1 e 3, e D5 reescritas |
| R6 — `d_before` | não toca esta spec (é da F3) | — |
| R7 — "filtra na leitura" / "custo proporcional ao que mudou" | sim | §0, §2.4, §5 (fórmula com leitura larga e reescrita do `MERGE`) e §7.1 corrigidos |
| R8 — linhas desatualizadas do `bronze_lib.py` | sim | agora `:268`, `:189`, `:197` |
| R10 — `QUALIFY` | sim (`log_eventos_negocio.sql:203`, rodou no cluster) | retirado dos proibidos; lista única = `checar_sql_cluster.py` (§3.9) |
| R12 — precedência do `optimizeWrite` | sim | risco na §7.2 com a ordem de confirmação |
| R13 — `EXPLAIN` no warehouse; bronze da amostra sem partição | sim (`criar_amostra_bronze.sql:12-25`) | §3.8 passa a valer só no cluster sobre as tabelas reais; §2.1 e §6.1 registram |
| R14 — poda dinâmica dos dois ramos | sim | busca reescrita num **único *scan*** com poda estática e joins difundidos (§3.5) |
| R15 — `CAST` nas mensagens; `INSERT`/`MERGE` no texto | sim (`run_sql.py:97`) | regra na instrução 4 e mensagem ajustada |
| R16 — ANSI | sim (observação) | registrado na §3.9 |
| R17 — medição comparável com o B20 | sim | `05..08` de uma vez + um mês incremental reconstruído na mesma subida (§6.2) |
| R18 — `SHALLOW CLONE` frágil | sim | `DEEP CLONE` (§6.2) |
| R19 — contiguidade em duas tarefas | sim | uma tarefa só (T0), com `IS DISTINCT FROM` + `sem_corrente` |
| R21 — clustering por `valid_since` | sim (contradição) | decisão registrada aqui: sem clustering na silver nesta fase (§2.4) |
| R22 — reextração da D-16 antes da reconstrução | sim | §5, "Ordem com a reextração" |
| R23 — tabelas `tmp_*` no gerador | sim | nota na §3.2 |
| R24 — `rn_busca` | sim | removido; premissa de unicidade + DQ (§3.5) |
| R25 — prefixo de 16 caracteres | sim | decisão do gerente: hash completo; D2 retirada; estimativas refeitas |
| R28 (parte da F2) | não se aplica | o `descartadas` com `BROADCAST` é da F3; esta spec já tem guarda de tamanho na instrução 4 |
| R29 — DV "sem volta" | sim | D4 corrigida (`DROP FEATURE`); conferir na T0 se já estão ligados |
| R30 — cenários faltando | sim | C3 (fronteira), C6 (com `AJUSTA_FIM`), C11–C16 |

**Simplificações pedidas e o que se perde:**
- Hash completo: a troca estreita fica ~1,5× maior, e a carga inicial ganha menos (~2× em vez de 3–5×).
- `MERGE` único: na carga inicial, as versões novas passam por mais uma troca larga.
- Busca num só *scan*: nada se perde.
- Sem `rn_busca`: nada se perde enquanto a bronze tiver o `dropDuplicates`; o DQ vigia.
