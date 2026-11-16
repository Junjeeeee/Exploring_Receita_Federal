# Feature: SCD2 da silver_intermediate em duas fases (otimização, fase 2)

**Status:** rascunho — aguardando aprovação do usuário (decisões na §9)
**Camadas afetadas:** silver_intermediate (`empresas`, `estabelecimentos`, `dados_simples`, `socios`)
**Branch:** `otimizacao_pipeline`
**Spec guarda-chuva:** [`2026-11-16-otimizacao-pipeline.md`](2026-11-16-otimizacao-pipeline.md) (princípios no §2)

## 0. Resumo

Hoje cada SCD2 lê a bronze inteira (todas as colunas) duas a três vezes, embaralha (*shuffle*) as linhas largas
três ou quatro vezes e entrega ao `MERGE` uma fonte que casa com **quase todas** as linhas da tabela destino.
O desenho novo separa o trabalho em duas fases:

1. **Detectar** o que mudou olhando só a "impressão digital" de cada linha: `(pk, mês, 16 primeiros caracteres
   do change_hash, bronze_processed_at)`. Uma única troca de dados por `pk`, com linhas estreitas. O resultado é
   uma tabela pequena de **ações**: `INSERE`, `AJUSTA_FIM`, `APAGA`.
2. **Aplicar** as ações: um `MERGE` estreito, que só fecha ou apaga versões, e um `INSERT` que busca na bronze
   os atributos completos **apenas** das linhas que abrem versão (de 1% a 2% por mês). A busca filtra a bronze já
   na leitura, por uma lista difundida (*broadcast*) de chaves.

A semântica atual fica intacta: o intervalo meio-aberto, 2023-05 como carga inicial, a idempotência, o
reprocessamento de mês do meio, o B16 e as saídas do QSA. A validação é linha a linha na amostra. Ganho esperado:
de 3 a 5 vezes menos shuffle no `si_estabelecimentos`, e um custo por mês incremental proporcional ao que
mudou, não ao tamanho da tabela.

## 1. Requisito

- **Problema:** com 4 meses, `si_estabelecimentos` levou 38 min (76 GB lidos, 97 GB de shuffle), `si_socios`
  25 min (69 GB) e `si_dados_simples` 28 min, quase sem *spill*. O custo é volume de shuffle (B20). A janela
  completa (~40 meses, em blocos de 6–12) precisa caber no cluster clássico DBR 15.4 / Spark 3.5
  (r5d.xlarge, 1–4 workers spot), sem serverless, dentro do *timeout* de 4 h por execução (C-21).
- **Quem consome:** `log_eventos_negocio`, `dim_empresa`, `dim_grupo_cnae`, `fato_mudanca_estado`,
  `fato_empresa_mes` e `dq_gold`. O conteúdo das tabelas não muda, então nada muda para eles.
- **Fora de escopo:** bronze (B17), log de eventos e gold (fase 3), regra de PK repetida (B16 continua com o
  desempate de hoje), identidade de sócio (C-18) e a ordem das colunas do hash (C-19).

## 2. Diagnóstico

### 2.1 O que já se sabe (medido)

| Tabela | Linhas bronze (4 meses) | Por mês | Versões SCD2 | Correntes | Versões novas/mês após o 1º (estimativa) |
|---|---|---|---|---|---|
| empresas | 216,6 M | ~54 M | 55,0 M | 54,7 M | ~0,4 M (~0,7%) |
| estabelecimentos | 228,1 M | ~57 M | 59,0 M | 57,6 M | ~0,8 M (~1,5%) |
| dados_simples | 143,2 M | ~36 M | 36,8 M | 36,3 M | ~0,5 M (~1,4%) |
| socios | 91,7 M | ~23 M | 24,1 M | 23,3 M | ~0,5 M (~2%) |

Fonte: contagens de 2026-11-15 nas tabelas reais (linhas da bronze e versões da silver). A última coluna é
uma **estimativa** (versões menos pks do 1º mês, dividido por 3) e precisa ser confirmada na T0 (Q2).
A conclusão vale mesmo com erro de 2×: **por mês, 98% a 99% das linhas não mudam nada**, mas o código
atual carrega todas elas com todas as colunas por todo o plano.

Outros fatos do código:
- A bronze é particionada por `mes_referencia` (`bronze_lib.py:241`, `partitionBy` + `replaceWhere`). Ler só
  os meses da janela já acontece: o desperdício está em ler **os mesmos meses várias vezes e todas as
  colunas**, não em ler meses demais. A ideia (5) de partida, *pruning* por mês, portanto, já está atendida.
- A bronze não tem `pk` nula (`bronze_lib.py:161-162`) e remove cópias idênticas (`_remover_copias_exatas`).
- O Job roda com `spark.databricks.delta.optimizeWrite.enabled = true` (`tcc_receita_pipeline.job.yml:36`).
  Isso acrescenta uma troca de dados antes de cada escrita Delta, inclusive na reescrita do `MERGE`.
- As SCD2 de hoje **já processam a janela inteira de uma vez** com `LAG`/`LEAD` (não é mês a mês). A ideia (4)
  de partida, portanto, já está no código: o problema é a largura das linhas e o número de leituras, não o laço.

### 2.2 `empresas.sql`, `estabelecimentos.sql`, `dados_simples.sql` (mesmo padrão)

Referências de linha do `estabelecimentos.sql`; entre parênteses, `empresas.sql` / `dados_simples.sql`.

| # | Onde | O que acontece | Custo |
|---|---|---|---|
| 1 | `pks_afetadas` 60–66 (34–40 / 35–41) | 1ª leitura da bronze (só `pk`) + `DISTINCT` | troca estreita por `pk` |
| 2 | `bronze_raw` 67–77 (41–51 / 42–52) | 2ª leitura da bronze, **todas as colunas** (`b.*`), `INNER JOIN pks_afetadas` | **troca larga nº 1**: a bronze inteira da janela é embaralhada por `pk` para um join que não filtra nada (`pks_afetadas` vem das mesmas linhas, com o mesmo filtro) |
| 3 | `bronze_limpo` 78–138 (52–67 / 53–78) | limpeza (4 `regexp_replace`, datas, telefones) em **todas** as linhas da janela | CPU desperdiçada em ~98% das linhas, que não abrem versão |
| 4 | `historico_intacto` 139–157 (68–80 / 79–90) | 1ª leitura da silver, todas as colunas, join com `pks_afetadas` | troca larga da silver (≈ 0 na carga inicial, ≈ tabela inteira no incremental) |
| 5 | `uniao_historico` → `LAG` → `LEAD` 158–187 (81–102 / 91–113) | `UNION ALL` + janelas por `pk` | provável **troca larga nº 2**: no Spark 3.5 o `UNION` não preserva o particionamento dos filhos, então tudo é reembaralhado (a confirmar no `EXPLAIN`) |
| 6 | `FULL OUTER JOIN` 188–198 (103–113 / 114–124) | 2ª leitura da silver (`pk, valid_since`) + join por `(pk, valid_since)` | **troca larga nº 3**: todas as versões recalculadas (na carga inicial, ~59 M linhas largas) mudam de chave de particionamento |
| 7 | `MERGE` 200–274 (115–146 / 126–162) | *materialize source* (B20: a fonte é recalculada, relendo a bronze, e gravada em disco local) + 3ª leitura da silver + join com o destino | a fonte traz **todas** as versões de todas as pks afetadas (≈ todas), então quase toda linha do destino "casa", passa pelo join e a tabela inteira é reescrita; o `optimizeWrite` acrescenta mais uma troca larga |

Na carga inicial (a execução medida, tabela vazia), os itens 4 e 6 custam pouco do lado da silver. Os 97 GB
vêm de: bronze larga embaralhada duas vezes (itens 2 e 5, 228 M linhas) + versões largas embaralhadas no item 6,
no `MERGE` e no `optimizeWrite`. No incremental, o custo dos itens 4, 6 e 7 cresce com o histórico e se
paga a cada mês.

### 2.3 `socios.sql`

| # | Linhas | O que acontece | Custo |
|---|---|---|---|
| 1 | 42–50 | guarda `EXISTS` na bronze (instrução própria) | barata (para na 1ª linha) |
| 2 | 59 | `MIN(mes_referencia)` da bronze inteira | só a coluna de partição; barata |
| 3 | 61–73 | `bronze_raw`: janela **+ mês anterior**, todas as colunas, `ROW_NUMBER` por `(pk, mes)` | **troca larga nº 1** |
| 4 | 74–98 | `validacao_snapshots`: `COUNT(DISTINCT mes)` sobre `bronze_raw` | `bronze_raw` é referenciada duas vezes; conforme o plano, recalculada ou reaproveitada (a confirmar) |
| 5 | 99–125 | limpeza de todas as linhas (incluindo o mês anterior) | CPU |
| 6 | 150–172 | `estados_inativos`: `bronze_limpo anterior` × `meses_janela` + `LEFT ANTI JOIN bronze_limpo atual` por `(pk, mes)` | **troca larga nº 2** (lado `anterior`, largo) + troca estreita (lado `atual`) |
| 7 | 178–185 | `pks_afetadas`: `DISTINCT` sobre `eventos_janela` + 1ª leitura da silver | `eventos_janela` também é referenciada duas vezes |
| 8 | 186–200 | `historico_intacto`: 2ª leitura da silver, larga | troca larga da silver |
| 9 | 201–223 | união + janelas | provável **troca larga nº 3** |
| 10 | 224–234 | `FULL OUTER JOIN` com a 3ª leitura da silver | **troca larga nº 4** |
| 11 | 236–276 | `MERGE` (tabela `PARTITIONED BY is_current`; as duas partições são tocadas) | igual ao item 7 da §2.2 |

### 2.4 Avaliação das ideias de partida

| Ideia | Avaliação |
|---|---|
| (1) Duas fases: detectar com `(pk, mes, change_hash)` e buscar atributos só de quem abre versão | **Adotada, com uma ressalva**: a busca dos atributos é um join com a bronze larga. Se for um join comum (*sort-merge*), o Spark embaralha a bronze larga inteira e o ganho some (fica pior que uma passada só). A busca só compensa se a bronze for filtrada **na leitura**, sem troca, o que exige difundir as chaves das versões novas para todos os executores (*broadcast*). Com `pk` em texto, ~12 M chaves (12 meses) dariam mais de 1 GB; por isso a lista difundida usa um inteiro de 64 bits, `xxhash64(pk, mes)` (~300 MB), e um join exato posterior, já pequeno, elimina falsos positivos. No 1º mês da carga inicial todas as pks abrem versão, e a lista seria do tamanho da bronze. Esse mês ("mês denso") é lido inteiro. |
| (2) Ler a bronze uma vez | **Adotada em espírito**: a fase 1 lê só 4 colunas estreitas e a fase 2 lê as colunas largas uma vez. Não há leitura repetida de colunas largas nem *materialize source*. Tabela temporária em sessão (`CACHE TABLE`, *temp view*) **não** serve: o runner da amostra (`rodar_no_warehouse.py`) manda cada instrução separada à Statement Execution API, sem garantia de sessão compartilhada. Tabelas de trabalho Delta (`tmp_scd2_*`) funcionam nos dois runners. |
| (3) Carga inicial por `INSERT OVERWRITE` | **Desnecessária como caminho separado**: no desenho novo, as versões novas sempre entram por `INSERT` (*append*), sem join com o destino. Na carga inicial, `MERGE` e `APAGA` ficam vazios. O mesmo arquivo serve aos dois modos. Reescrever a tabela inteira a cada bloco (`INSERT OVERWRITE`) custaria proporcional ao histórico e não traria nada a mais. |
| (4) Janela inteira com `LAG` por pk | **Já é assim hoje**; o desenho mantém e estreita. |
| (5) Pruning por `mes_referencia` | **Já acontece** (bronze particionada). Clustering ou `ZORDER` na silver não ajudam o `MERGE`: as mudanças se espalham por todas as pks, logo por todos os arquivos. O `ZORDER` ainda falha em tabela com clustering automático. |

## 3. Desenho

### 3.1 A ideia em linguagem simples

Para saber se um estabelecimento mudou num mês, basta comparar a impressão digital (`change_hash`) da linha
desse mês com a da versão anterior. Não é preciso carregar endereço, telefone e nome fantasia para isso.
Então:

- **Fase 1 (detecção):** junta, por `pk`, as impressões digitais da bronze da janela com as das versões
  existentes que a janela pode afetar. Calcula a linha do tempo nova (mesma regra de hoje: abre versão quando o
  hash difere do anterior) e compara com a existente. Sai uma lista de ações por `(pk, valid_since)`.
- **Fase 2 (aplicação):** fecha ou apaga versões com um `MERGE` que só mexe em `valid_until`/`is_current`
  e insere as versões novas buscando os atributos na bronze. A limpeza (regex, datas, telefones) roda só
  nessas linhas.

**Impressão digital curta.** A fase 1 usa `substr(change_hash, 1, 16)`: 16 caracteres hexadecimais = 64 bits
do SHA-256. Motivo: o `change_hash` completo tem 64 caracteres pouco compressíveis e seria a maior parte da
linha estreita. Com 16, a linha cai para ~40% do tamanho. O prefixo tem a mesma ordem do texto completo, então
o desempate do B16 (`change_hash DESC`) se mantém. Risco: duas versões **consecutivas** da mesma pk com hashes
diferentes, mas o mesmo prefixo, seriam tratadas como iguais. A probabilidade é de 2⁻⁶⁴ por comparação: ~6·10⁹
comparações em 40 meses e 4 tabelas dão ~3·10⁻¹⁰. Desprezível, mas é escolha do usuário (§9, D2).

### 3.2 Estrutura de cada arquivo (mesmo nome, mesmas tasks do Job)

| Nº | Instrução | Escreve? |
|---|---|---|
| 0 | `CREATE TABLE IF NOT EXISTS` do destino (DDL de hoje, sem mudança) | — |
| 1 | `SELECT ASSERT_TRUE(...)`: janela válida; **todo mês da janela existe na bronze**; socios também exige o mês anterior, salvo na carga inicial | não |
| 2 | `CREATE OR REPLACE TABLE ...tmp_scd2_acoes_<t> AS` (fase 1) | tabela de trabalho |
| 3 | `CREATE OR REPLACE TABLE ...tmp_scd2_meses_<t> AS` (versões novas e linhas da bronze por mês; marca meses densos) | tabela de trabalho |
| 4 | `SELECT ASSERT_TRUE(...)`: chaves a difundir ≤ limite | não |
| 5 | `MERGE` estreito: `APAGA` e `AJUSTA_FIM` | destino |
| 6 | `INSERT INTO` destino: `INSERE`, com busca de atributos | destino |
| 7 | `DROP TABLE IF EXISTS` das duas tabelas de trabalho | — |

- As tabelas de trabalho ficam no schema `silver_intermediate` (o runner da amostra as troca para
  `amostra_silver_intermediate` automaticamente). Se a execução falhar no meio, elas ficam para diagnóstico.
- O bloco `params` é repetido em cada instrução (cada uma é independente; o runner substitui os parâmetros
  no texto).
- **Ordem 5 antes de 6** é obrigatória: numa substituição (mesma `(pk, valid_since)` com hash novo), a
  versão velha é apagada no passo 5 e a nova é inserida no passo 6.
- **Atomicidade:** 5 e 6 são dois *commits*. Se o passo 6 falhar, a tabela fica com versões fechadas sem a
  sucessora. Reexecutar a mesma janela conserta: a fase 1 recalcula a partir do estado atual, vê o fim já
  ajustado (nenhuma ação) e a versão nova ausente (`INSERE`). Ver §9, D3.

### 3.3 Fase 1 — detecção (pseudo-SQL fiel ao Spark 3.5; exemplo: `estabelecimentos`)

```sql
CREATE OR REPLACE TABLE tcc_receita.silver_intermediate.tmp_scd2_acoes_estabelecimentos AS
WITH params AS (
    SELECT
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
),
-- (a) Bronze da janela: só chave, mês, carimbo e impressão digital curta.
bronze_estreita AS (
    SELECT b.pk, b.mes_referencia AS mes, 'B' AS origem,
           b.bronze_processed_at AS ts, substr(b.change_hash, 1, 16) AS h,
           CAST(NULL AS STRING) AS valid_until_atual
    FROM tcc_receita.bronze.estabelecimentos b CROSS JOIN params p
    WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim AND b.pk IS NOT NULL
),
-- (b) Versões existentes que a janela pode alterar: abertas ou terminando em mes_ini ou depois.
--     Versões com valid_until < mes_ini têm sucessora antes da janela: nada nelas pode mudar.
--     No modo incremental normal (janela depois de tudo), são só as versões correntes.
silver_estreita AS (
    SELECT s.pk, s.valid_since AS mes,
           CASE WHEN s.valid_since BETWEEN p.mes_ini AND p.mes_fim THEN 'S_DENTRO' ELSE 'S_FORA' END AS origem,
           CAST(NULL AS TIMESTAMP) AS ts, substr(s.change_hash, 1, 16) AS h,
           s.valid_until AS valid_until_atual
    FROM tcc_receita.silver_intermediate.estabelecimentos s CROSS JOIN params p
    WHERE s.valid_until IS NULL OR s.valid_until >= p.mes_ini
),
-- (c) A ÚNICA troca de dados: tudo da mesma pk vai para a mesma tarefa.
--     DISTRIBUTE BY pk força a troca por pk; as janelas abaixo particionam por (pk, ...) e a reaproveitam.
uniao AS (
    SELECT * FROM (SELECT * FROM bronze_estreita UNION ALL SELECT * FROM silver_estreita) u
    DISTRIBUTE BY pk
),
-- (d) B16: uma linha da bronze por (pk, mês), mesmo desempate de hoje.
dedup AS (
    SELECT * FROM (
        SELECT u.*, ROW_NUMBER() OVER (PARTITION BY pk, mes, origem
                                       ORDER BY ts DESC NULLS LAST, h DESC) AS rn
        FROM uniao u
    ) x WHERE origem <> 'B' OR rn = 1
),
-- (e) Linha do tempo nova. Candidatas = versões fora da janela + bronze da janela.
--     Versões existentes DENTRO da janela não são candidatas: são o "antes" a comparar.
com_anterior AS (
    SELECT d.*, origem IN ('S_FORA', 'B') AS eh_candidata,
           LAG(h) OVER (PARTITION BY pk, origem IN ('S_FORA', 'B') ORDER BY mes) AS h_anterior
    FROM dedup d
),
com_abertura AS (
    SELECT c.*, (eh_candidata AND h_anterior IS DISTINCT FROM h) AS abre_versao
    FROM com_anterior c
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
    SELECT pk, mes AS valid_since, ts AS ts_origem, h AS h_origem, mes AS mes_origem,
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
SELECT pk, valid_since, acao, valid_until, valid_until IS NULL AS is_current,
       mes_origem, h_origem, ts_origem
FROM acoes WHERE acao IS NOT NULL;
```

Por que funciona (para defender):
- As versões existentes **fora** da janela entram como "pontos" da linha do tempo, no mês `valid_since`
  com o seu hash, exatamente como o `historico_intacto` de hoje. Assim o `LAG` enxerga a versão anterior à
  janela e as posteriores.
- `PARTITION BY pk, <expressão>` faz o `LAG`/`LEAD` ignorar as linhas que não são candidatas sem nova troca de
  dados: a troca por `pk` já agrupou tudo, e o Spark só ordena localmente.
- A comparação "novo × existente" é feita por janela em `(pk, mês)`, não por join. Um join por
  `(pk, valid_since)` reembaralharia tudo.

### 3.4 Tabela de ações

| Linha | Condição | Ação | Quando acontece |
|---|---|---|---|
| versão fora da janela | o hash ficou igual ao da nova anterior | `APAGA` | reprocessar mês do meio e a 1ª versão depois da janela virar redundante |
| versão fora da janela | o fim (`valid_until`) mudou | `AJUSTA_FIM` | caso típico do incremental: a versão corrente é fechada no mês em que a pk mudou |
| versão dentro da janela | não há versão nova no mesmo mês, ou o hash é outro | `APAGA` | reprocessamento com bronze diferente; a substituição é `APAGA` + `INSERE` |
| versão dentro da janela | mesmo hash, fim diferente | `AJUSTA_FIM` | reprocessar parte de uma janela já processada |
| bronze da janela | abre versão e não há existente igual no mês | `INSERE` | toda versão nova |
| qualquer | nada mudou | (nenhuma) | reexecução: **zero ações** |

### 3.5 Fase 2 — aplicação

**Instrução 3: meses densos.** Um mês é "denso" quando mais de 20% das linhas da bronze desse mês abrem
versão. Na prática, é o 1º mês da carga inicial, ou um mês em que todos os hashes mudaram (C-19). Mês denso
é lido inteiro; mês esparso é filtrado na leitura.

```sql
CREATE OR REPLACE TABLE tcc_receita.silver_intermediate.tmp_scd2_meses_estabelecimentos AS
WITH params AS (...),
bronze_mes AS (
    SELECT b.mes_referencia AS mes, COUNT(*) AS n_bronze
    FROM tcc_receita.bronze.estabelecimentos b CROSS JOIN params p
    WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim   -- socios: desde add_months(mes_ini, -1)
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

**Instrução 4: guarda de memória.**
```sql
SELECT ASSERT_TRUE(SUM(CASE WHEN NOT denso THEN n_insere ELSE 0 END) <= 25000000,
       CONCAT('Versões novas em meses esparsos: ', SUM(CASE WHEN NOT denso THEN n_insere ELSE 0 END),
              ' (limite 25 M para o broadcast). Rode a janela em blocos menores.'))
FROM tcc_receita.silver_intermediate.tmp_scd2_meses_estabelecimentos;
```
25 M chaves de 8 bytes ≈ 600 MB na tabela de hash difundida, aceitável num r5d.xlarge (32 GB) e num driver
m5d.xlarge (16 GB). Com ~1 M versões novas por mês, um bloco de 12 meses fica em ~12 M.

**Instrução 5: `MERGE` estreito.** Não mexe em atributos, só no fim da vigência. A fonte tem ~1–2% das linhas.
```sql
MERGE INTO tcc_receita.silver_intermediate.estabelecimentos AS t
USING (
    SELECT pk, valid_since, valid_until, is_current, acao
    FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_estabelecimentos
    WHERE acao IN ('APAGA', 'AJUSTA_FIM')
) AS a
ON t.pk = a.pk AND t.valid_since = a.valid_since
WHEN MATCHED AND a.acao = 'APAGA' THEN DELETE
WHEN MATCHED AND a.acao = 'AJUSTA_FIM' THEN UPDATE SET
    t.valid_until = a.valid_until,
    t.is_current  = a.is_current,
    t.updated_at  = current_timestamp();
```
Cada `(pk, valid_since)` aparece no máximo uma vez com `APAGA`/`AJUSTA_FIM`, porque vem de uma única versão
existente. Logo, o `MERGE` nunca recebe duas linhas de origem para a mesma linha de destino.

**Instrução 6: `INSERT` com busca dos atributos.**
```sql
INSERT INTO tcc_receita.silver_intermediate.estabelecimentos (
    pk, cnpj_basico, ..., data_situacao_especial,           -- mesma lista e ordem do INSERT de hoje (246–259)
    mes_referencia, inserted_at, updated_at, change_hash, valid_since, valid_until, is_current)
SELECT
    x.pk,
    NULLIF(TRIM(x.cnpj_basico), '') AS cnpj_basico,
    ...                                                      -- EXATAMENTE as expressões de bronze_limpo (81–132)
    x.valid_since AS mes_referencia, x.bronze_processed_at AS inserted_at, current_timestamp() AS updated_at,
    x.change_hash, x.valid_since, x.valid_until, x.is_current
FROM (
    SELECT b.*, n.valid_since, n.valid_until, n.is_current,
           ROW_NUMBER() OVER (PARTITION BY n.pk, n.valid_since ORDER BY b.change_hash DESC) AS rn_busca
    FROM (
        -- meses densos: o mês inteiro (quase toda pk abre versão)
        SELECT b.* FROM tcc_receita.bronze.estabelecimentos b
        WHERE b.mes_referencia IN (SELECT mes FROM tcc_receita.silver_intermediate.tmp_scd2_meses_estabelecimentos
                                   WHERE denso)
        UNION ALL
        -- meses esparsos: filtro na leitura por uma lista difundida de impressões (pk, mês) de 64 bits
        SELECT /*+ BROADCAST(k) */ b.*
        FROM tcc_receita.bronze.estabelecimentos b
        LEFT SEMI JOIN (
            SELECT DISTINCT xxhash64(a.pk, a.mes_origem) AS chave
            FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_estabelecimentos a
            JOIN tcc_receita.silver_intermediate.tmp_scd2_meses_estabelecimentos m
              ON a.mes_origem = m.mes AND NOT m.denso
            WHERE a.acao = 'INSERE'
        ) k ON xxhash64(b.pk, b.mes_referencia) = k.chave
        WHERE b.mes_referencia IN (SELECT mes FROM tcc_receita.silver_intermediate.tmp_scd2_meses_estabelecimentos
                                   WHERE NOT denso AND n_insere > 0)
    ) b
    -- join exato, já com poucas linhas: elimina falso positivo do xxhash64 e traz a vigência
    JOIN (SELECT * FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_estabelecimentos WHERE acao = 'INSERE') n
      ON  b.pk = n.pk
      AND b.mes_referencia = n.mes_origem
      AND substr(b.change_hash, 1, 16) = n.h_origem
      AND b.bronze_processed_at <=> n.ts_origem
) x
WHERE x.rn_busca = 1;
```
- O join exato usa `(pk, mês, prefixo, ts)` — a mesma linha que venceu o desempate do B16 na fase 1. O
  `rn_busca` é rede de segurança para cópias idênticas, que a bronze hoje já remove; sem ela, uma cópia
  duplicaria a versão.
- A limpeza (regex etc.) é **copiada sem alteração** do `bronze_limpo` atual. Só muda o lugar: agora roda
  depois do filtro, em ~1–2% das linhas por mês (100% no mês denso).

### 3.6 `socios.sql` — o que muda em relação ao padrão

Socios tem uma regra a mais: **ausência** no snapshot do mês M de uma relação presente em M−1 gera uma versão
`vinculo_ativo = FALSE` em M, com os atributos de M−1 e `change_hash = SHA2(TO_JSON(NAMED_STRUCT(
'source_change_hash', <hash de M−1>, 'vinculo_ativo', FALSE)), 256)`. Hoje isso é um auto-join da bronze larga
(linhas 150–172). No desenho novo, vira uma janela sobre as linhas estreitas já agrupadas por `pk`:

1. **Instrução 1 (validação):** a regra atual das linhas 42–50 e 74–98, toda numa instrução própria, antes
   de qualquer escrita: janela válida e `COUNT(DISTINCT mes_referencia)` em `[mes_ini−1, mes_fim]` igual ao
   esperado (sem o mês anterior quando `mes_ini = MIN(mes_referencia)` da bronze: carga inicial, 2023-05).
2. **`bronze_estreita`** lê `[mes_ini−1, mes_fim]`, com `origem = 'B_CTX'` para `mes_ini−1` (só contexto) e
   `'B'` para a janela. Antes da troca, calcula também a impressão da versão inativa:
   `substr(SHA2(TO_JSON(NAMED_STRUCT('source_change_hash', change_hash, 'vinculo_ativo', FALSE)), 256), 1, 16)
   AS h_inativo`. É um SHA por linha: CPU barata, sem troca.
3. Depois do `dedup`, a **próxima presença** de cada linha da bronze:
   `LEAD(mes) OVER (PARTITION BY pk, origem IN ('B','B_CTX') ORDER BY mes) AS prox_presenca`.
4. **Pontos da linha do tempo:** cada linha vira 0, 1 ou 2 pontos, via `LATERAL VIEW INLINE` (o `pk` e as
   colunas de controle continuam os mesmos atributos, o que preserva o particionamento):
   ```sql
   pontos AS (
     SELECT pr.pk, pr.origem AS origem_linha, pr.valid_until_atual, pr.ts AS ts_origem, pr.h AS h_origem,
            pt.mes, pt.h, pt.vinculo_ativo, pt.mes_origem,
            CASE WHEN pr.origem IN ('B','B_CTX') THEN 'B' ELSE pr.origem END AS origem
     FROM presenca pr CROSS JOIN params p
     LATERAL VIEW INLINE(FILTER(ARRAY(
         -- a própria linha: versões existentes e bronze da janela (B_CTX não é ponto)
         IF(pr.origem <> 'B_CTX',
            NAMED_STRUCT('mes', pr.mes, 'h', pr.h, 'vinculo_ativo', IF(pr.origem = 'B', TRUE, CAST(NULL AS BOOLEAN)),
                         'mes_origem', pr.mes),
            NULL),
         -- saída do QSA: presente em M−1 = pr.mes, ausente em M (M <= mes_fim)
         IF(pr.origem IN ('B','B_CTX') AND pr.mes < p.mes_fim
            AND (pr.prox_presenca IS NULL
                 OR pr.prox_presenca > date_format(add_months(to_date(concat(pr.mes, '-01')), 1), 'yyyy-MM')),
            NAMED_STRUCT('mes', date_format(add_months(to_date(concat(pr.mes, '-01')), 1), 'yyyy-MM'),
                         'h', pr.h_inativo, 'vinculo_ativo', FALSE, 'mes_origem', pr.mes),
            NULL)
     ), x -> x IS NOT NULL)) pt
   )
   ```
   É a mesma regra de hoje: só gera inativa no 1º mês ausente, e só para M dentro da janela, porque o
   anterior tem de estar em `[mes_ini−1, mes_fim−1]`.
5. A partir daí, segue o padrão da §3.3, itens (e) e (f), sobre `pontos`. A ação `INSERE` carrega
   `vinculo_ativo`, `mes_origem` (= M−1 para inativas), `h_origem` (o prefixo do hash **de origem**) e
   `ts_origem`.
6. **Busca (instrução 6):** lê a bronze de `[mes_ini−1, mes_fim]`, junta por
   `(pk, mes_origem, h_origem, ts_origem)` e calcula
   `change_hash = IF(n.vinculo_ativo, b.change_hash, SHA2(TO_JSON(NAMED_STRUCT('source_change_hash', b.change_hash, 'vinculo_ativo', FALSE)), 256))`,
   `vinculo_ativo = n.vinculo_ativo`, `mes_referencia = n.valid_since` e
   `inserted_at = b.bronze_processed_at` (como hoje: a inativa herda o carimbo do mês anterior). Uma mesma
   linha da bronze pode gerar duas versões (ativa em M−1 e inativa em M); o `ROW_NUMBER` é por
   `(pk, valid_since)`, então as duas ficam.
7. **`MERGE`:** igual ao padrão. A tabela continua `PARTITIONED BY (is_current)`, e `AJUSTA_FIM` move a linha
   de partição (o Delta faz isso sozinho).

### 3.7 Por tabela

| Tabela | Particularidades no desenho | Risco |
|---|---|---|
| `dados_simples` | padrão puro; 10 atributos | baixo — **primeira** |
| `estabelecimentos` | padrão puro; 34 atributos; maior ganho absoluto | baixo (mesmo molde) |
| `empresas` | padrão puro; `capital_social` DOUBLE (expressão copiada) | baixo; ganho menor (linhas já estreitas) |
| `socios` | §3.6: contexto `mes_ini−1`, ausência, hash da inativa, `PARTITIONED BY is_current` | médio — **última** |

### 3.8 Regras de plano (verificar com `EXPLAIN FORMATTED` antes de rodar na base)

- Instrução 2: exatamente **um** `Exchange hashpartitioning(pk, ...)` acima dos dois *scans*. As janelas
  aparecem só com `Sort`, sem novo `Exchange`. Em socios, nenhum `Exchange` depois do `Generate`. Se o
  `UNION`, a janela ou o `Generate` forçarem outra troca, ela é estreita, então o desenho continua valendo, mas
  o fato fica registrado nas notas.
- Instrução 6: o ramo dos meses esparsos mostra `BroadcastHashJoin LeftSemi` **sem** `Exchange` entre o
  *scan* da bronze e o join. O join exato pode ter `Exchange`, porque os dois lados já são pequenos (fora o
  mês denso).
- Nenhum `materialize source` grande no `MERGE`: a fonte é uma leitura simples de uma tabela Delta pequena.

### 3.9 Compatibilidade com Spark 3.5 / DBR 15.4

Usa só: `substr`, `sha2`, `to_json`, `named_struct`, `xxhash64`, `inline`, `filter` (função de ordem
superior), `LATERAL VIEW`, `IF`, `<=>`, `IS DISTINCT FROM`, `LAG`/`LEAD`/`ROW_NUMBER`, `DISTRIBUTE BY`, a dica
`BROADCAST`, `CREATE OR REPLACE TABLE AS`, `MERGE` só com `WHEN MATCHED`, `INSERT INTO t (colunas) SELECT` e
`CAST(try_to_timestamp(...) AS DATE)` (já usado hoje). **Proibido:** `TRY_TO_DATE`, `QUALIFY`,
`OPTIMIZE ... ZORDER`, tabelas temporárias de sessão e `SET` de configurações que o warehouse recusa. A
checagem automática da fase 1 (guarda-chuva, §3, item 2) deve rodar sobre os quatro arquivos.

## 4. Semântica preservada

| Invariante de hoje | Como se mantém |
|---|---|
| Intervalo meio-aberto `[valid_since, valid_until)` em `'YYYY-MM'`; `valid_until` NULL = atual; `is_current = (valid_until IS NULL)` | `valid_until` = `LEAD(mes)` entre as versões que abrem (igual a hoje); `is_current` derivado dele |
| Versão nova só quando o hash difere do anterior (`IS DISTINCT FROM`) | mesmo `LAG` sobre o mesmo conjunto de pontos (versões fora da janela + bronze da janela), comparando o prefixo de 64 bits (D2) |
| Versões fora da janela preservadas | entram como pontos e só recebem `AJUSTA_FIM` ou `APAGA` quando o recálculo exige; hoje o `MERGE` faz o mesmo |
| Correção de hash atualiza todos os atributos (D-04) | substituição = `APAGA` + `INSERE` com os atributos da bronze |
| B16: uma linha por (pk, mês), desempate `bronze_processed_at DESC NULLS LAST, change_hash DESC` | mesmo `ROW_NUMBER` na fase 1 (o prefixo tem a ordem do texto); a fase 2 busca exatamente a linha vencedora |
| Idempotência: reexecutar a mesma janela não muda nada | a fase 1 compara com o estado atual → zero ações; `MERGE` e `INSERT` não escrevem linha (até `updated_at` fica) |
| Reprocessar mês do meio | versões existentes dentro da janela são o "antes"; versões depois da janela entram como pontos (podem ser apagadas, se viraram redundantes, ou ter o fim ajustado) |
| 2023-05 = carga inicial (socios sem mês anterior e sem saídas no 1º mês) | mesma regra da validação; sem linhas `B_CTX`, não há inativa no 1º mês |
| Socios: saída = ausência num snapshot completo; inativa carrega atributos e `inserted_at` de M−1 | §3.6, mesma regra, mesmo hash da inativa |
| Socios: exige o mês anterior completo (`ASSERT`) | instrução 1, antes de qualquer escrita |
| Estabelecimentos/empresas/simples: pk que some da bronze **não** fecha a versão (C-10) | mantido: sem linha da bronze, a versão corrente não recebe ação |
| `inserted_at` = carimbo da bronze que originou a versão; `updated_at` = momento da última alteração | `INSERE`: igual a hoje; `AJUSTA_FIM`: só `updated_at` muda (hoje também) |
| `mes_referencia` da versão = `valid_since` | idem |
| Esquema, nomes e comentários das tabelas | DDL inalterado |

**C-05 e C-06 (socios).** As duas correções **já estão no código atual** (commit `9ede9b4`, T5 da spec
de semântica, validadas só na amostra). O desenho novo as mantém: não existe bloco "legado" (C-05) e toda
validação roda numa instrução separada, antes de qualquer escrita (C-06; com a bronze vazia, a instrução 1
falha e nada é gravado). A tabela real foi gerada em 2026-11-08 pelo código **anterior** à correção. Porém, na
carga inicial (tabela vazia), o bloco `estados_inativos_legado` não gera linha nenhuma, e a consulta Q5 mediu
**0** versões fantasmas. **Diferença esperada entre o novo resultado e a tabela real: nenhuma.** Se aparecer,
ela só pode estar em pks cuja 1ª versão é inativa ou em versões com `valid_since = 2023-05` e
`vinculo_ativo = FALSE`; a query Q5 da `critica.md` as isola.

**Diferenças intencionais (só em situações que não ocorrem com a mesma bronze):**
1. Empresas/estabelecimentos/simples: se um mês já processado for reprocessado com uma bronze **diferente** em
   que a pk sumiu, a versão órfã desse mês é apagada. Hoje ela fica, porque `pks_afetadas` só olha a bronze.
   Socios já se comporta assim.
2. Substituição por hash corrigido: o `inserted_at` passa a ser o da bronze nova. Hoje fica o antigo, porque o
   `UPDATE` não o toca nos três arquivos não-socios.
3. Todas as tabelas falham (instrução 1) se faltar algum mês da janela na bronze. Hoje, os três não-socios
   pulam o mês em silêncio. Ver D5.

## 5. Modos de execução

Os parâmetros não mudam: `{{mes_referencia_inicial}}` vazio = mês anterior; `{{mes_referencia_final}}` vazio
= o inicial. **Um arquivo só** serve a todos os modos, porque a fase 1 sempre compara com o estado atual da
tabela.

| Modo | Como rodar | O que a fase 1 lê da silver | Ações típicas |
|---|---|---|---|
| Carga inicial / reconstrução (dev, D-08) | `DROP TABLE` (opcional) + janela `2023-05..fim do bloco` | nada (tabela vazia) | só `INSERE`; o 1º mês é denso |
| Bloco seguinte (6–12 meses) | `mes_ini` = mês seguinte ao último processado | só versões correntes | `INSERE` + `AJUSTA_FIM` |
| Mensal | `mes_ini = mes_fim` = mês novo (ou vazio) | só versões correntes | idem, ~1–2% das linhas |
| Reexecução (retry/repair) | a mesma janela | versões correntes + as da janela | nenhuma |
| Mês do meio (produção, D-08) | janela no meio do histórico | versões que terminam em `mes_ini` ou depois | `APAGA`/`AJUSTA_FIM`/`INSERE` conforme a bronze |

Custo por execução ≈ meses do bloco × (linhas estreitas da bronze) + versões correntes (estreitas) + meses
densos × (linhas largas) + o que mudou × (linhas largas). O custo fixo por execução, a leitura estreita das
versões correntes, favorece blocos maiores; o limite do *broadcast* (instrução 4) segura o tamanho máximo.

**Escrita do `MERGE`.** Mesmo com a fonte pequena, as versões a fechar se espalham por todos os arquivos.
Sem *deletion vectors*, o Delta reescreve todos os arquivos tocados (≈ a tabela). Com *deletion vectors*, ele
grava só marcas de exclusão e as linhas alteradas. Isso pesa pouco nos ~4–7 blocos da janela, mas pesa no
mensal em produção. Ver D4.

## 6. Plano de validação

### 6.1 Amostra (1%, SQL warehouse, `scripts/amostra/rodar_no_warehouse.py`)

**Referência.** Antes de trocar qualquer arquivo, com o código atual (`main`):
```bash
python scripts/amostra/rodar_no_warehouse.py --arquivos src/silver_intermediate/ingest/<t>.sql   # 2023-05..2023-08, tabela vazia
```
```sql
CREATE SCHEMA IF NOT EXISTS tcc_receita.amostra_ref;
CREATE OR REPLACE TABLE tcc_receita.amostra_ref.<t> DEEP CLONE tcc_receita.amostra_silver_intermediate.<t>;
```
A referência é uma cópia física, então rodar o código novo sobre `amostra_silver_intermediate` não a altera.
Nada no schema real é tocado.

**Cenários (código novo), por tabela.** "Do zero" = `DROP TABLE tcc_receita.amostra_silver_intermediate.<t>` antes.

| Id | Cenário | Esperado |
|---|---|---|
| C1 | do zero, bloco `2023-05..2023-08` | igual à referência |
| C2 | do zero, mês a mês: 05, 06, 07, 08 | igual à referência |
| C3 | do zero, blocos `05..06` + `07..08` | igual à referência |
| C4 | após C1, reexecutar `05..08` | igual; `DESCRIBE HISTORY`: `MERGE` com 0 linhas atualizadas/apagadas e `INSERT` com 0 linhas |
| C5 | após C1, reexecutar `06..07` e depois só `06` | igual (versões antes e depois da janela preservadas) |
| C6 | após C1, `DELETE ... WHERE valid_since = '2023-07'` e reexecutar `07` | igual (caminho `INSERE` + `AJUSTA_FIM`) |
| C7 | após C1, `UPDATE ... SET change_hash = repeat('0', 64) WHERE valid_since = '2023-07' AND pk IN (<100 pks>)` e reexecutar `07` | igual (caminho substituição = `APAGA` + `INSERE`) |
| C8 | após C1, reexecutar só `05` | igual (caminho "versões depois da janela" como pontos) |
| C9 | janela `2030-01` (sem bronze) | instrução 1 falha; `DESCRIBE HISTORY` sem versão nova |
| C10 | socios: após C4, Q5 (1ª versão inativa) | 0 |

**Consultas de comparação** (dois sentidos, `EXCEPT ALL` pega também duplicatas; `updated_at` fica de fora
porque é o momento da escrita):
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
-- contagens por mês (as duas tabelas, lado a lado)
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
-- confirmações da fase 1 na amostra (T0)
SELECT COUNT(DISTINCT change_hash) AS completos, COUNT(DISTINCT substr(change_hash, 1, 16)) AS prefixos
FROM tcc_receita.amostra_bronze.<t>;                          -- esperado: iguais
SELECT COUNT(*) AS linhas, COUNT(DISTINCT pk, mes_referencia, change_hash) AS distintas
FROM tcc_receita.amostra_bronze.<t>;                          -- esperado: iguais (sem cópia idêntica)
```
Além disso: o `dq_<t>.sql` passa em todos os cenários. Os DQ de `empresas`, `estabelecimentos` e
`dados_simples` ganham os dois testes que hoje só socios tem: continuidade (`valid_until` = `valid_since`
da próxima) e unicidade de `(pk, valid_since)`.

**Limite da amostra:** ela roda no warehouse e não prova compatibilidade com o cluster do Job, nem desempenho.
Os `EXPLAIN` da §3.8 também devem ser conferidos no warehouse, e a compatibilidade, pela checagem automática da
fase 1.

### 6.2 Base real (medição; uma vez, ao fim das quatro tabelas)

1. **Preservar o atual sem custo de cópia:** `CREATE TABLE tcc_receita.silver_intermediate_ref.<t> SHALLOW CLONE
   tcc_receita.silver_intermediate.<t>` para as quatro. *Shallow clone* só copia metadados e aponta para os
   arquivos atuais, por isso **não rodar `VACUUM`** nas SCD2 até a comparação terminar.
2. **Execução 1 (carga inicial):** `DROP` das quatro SCD2 e rodar só as tasks `si_*` com `2023-05..2023-06`.
3. **Execução 2 e 3 (incremental):** `2023-07` e depois `2023-08`. Medem o custo de um mês incremental, que
   é o que se repete 36 vezes.
4. **Comparação:** as consultas da §6.1 trocando `amostra_*` por `silver_intermediate` /
   `silver_intermediate_ref`, num arquivo `.sql` rodado **no cluster do Job**, numa task única (com 60 M
   linhas, o warehouse 2X-Small seria lento). Esperado: 0 e 0.
5. **O que medir por task `si_*`:** duração (`system.lakeflow.job_task_run_timeline`), bytes lidos, shuffle
   escrito, *spill* (Spark UI / logs em `/Volumes/tcc_receita/ops/cluster_logs`), `DESCRIBE HISTORY`
   (`numTargetRowsUpdated/Deleted/Copied`, `numTargetFilesAdded/Removed`, `numOutputRows` do `INSERT`) e o
   tamanho das tabelas de trabalho. Registrar em `docs/medicoes/` ao lado da linha de base.
6. **Depois:** rodar log → silver_final → gold uma vez e conferir o DQ, como sanidade; as SCD2 iguais
   implicam log igual.

## 7. Ganho esperado e riscos

### 7.1 Ganho por mudança (qualitativo, a confirmar na §6.2)

| Mudança | O que elimina | Efeito esperado |
|---|---|---|
| Tirar o `JOIN pks_afetadas` e a 2ª leitura larga da bronze | 1 leitura larga + 1 troca larga (item 2 da §2.2) | ~⅓ do shuffle de hoje |
| Detecção estreita (prefixo de 16 chars) | as trocas largas da união e da janela (itens 5 e 9) viram uma troca de ~40% da largura | ~2–3× menos bytes nessa troca; ordenações locais também estreitas |
| Comparação por janela em `(pk, mês)` | o `FULL OUTER JOIN` por `(pk, valid_since)` (itens 6 e 10) | −1 troca larga |
| Fonte mínima do `MERGE` | o *materialize source* (que relia a bronze) e o join de ~100% do destino | o `MERGE` casa ~1–2% das linhas |
| `INSERT` só das versões novas, com busca filtrada na leitura | a escrita de todas as versões pelo `MERGE` | no incremental, escreve ~1 M linhas/mês, não ~60 M |
| Limpeza depois do filtro | regex em 4 colunas × 57 M linhas/mês | CPU ~50× menor nos meses esparsos |
| Socios: ausência por janela | o auto-join largo `anterior × atual` (item 6 da §2.3) | −1 troca larga |
| (D4) *Deletion vectors* | a reescrita da tabela inteira pelo `MERGE` | essencial no mensal em produção |

**Ordem de grandeza** (`si_estabelecimentos`, 4 meses, carga inicial): hoje 97 GB de shuffle. Depois, troca
estreita de 228 M linhas (~10–15 GB) + mês denso largo (~10 GB) + pequenos ≈ **20–25 GB (3–5× menos)**.
Leitura: colunas estreitas da janela + uma leitura larga, em vez de 76 GB. Num **mês incremental**, a troca é
de ~57 M linhas estreitas da bronze + ~57 M estreitas da silver; a parte larga é só o ~1 M que mudou. Estimativa,
não medição: os números entram em `docs/medicoes/` na T5.

### 7.2 Riscos

| Risco | Mitigação |
|---|---|
| O otimizador não preservar o particionamento (`UNION`, janelas com expressão, `Generate`) e criar outra troca | critério de aceite via `EXPLAIN` (§3.8); a troca extra seria estreita |
| *Broadcast* grande demais (bloco longo, muitos hashes mudando — C-19) | mês denso lido inteiro; guarda da instrução 4 (≤ 25 M chaves); blocos ≤ 12 meses |
| Colisão do prefixo de 64 bits | ~3·10⁻¹⁰ na janela toda (D2); a comparação da amostra também confere a unicidade dos prefixos |
| `MERGE` e `INSERT` em dois *commits* | reexecutar a janela converge (§3.2); o DQ roda depois e acusa duas versões correntes se alguém olhar no meio |
| `optimizeWrite` (conf do Job) acrescenta uma troca antes do `INSERT` do mês denso | medir na T5; se pesar, desligar por tabela (`delta.autoOptimize.optimizeWrite = false`) e conferir o tamanho dos arquivos |
| *Deletion vectors* (se adotados) mudam o protocolo da tabela, sem volta | só depois de conferir os leitores (DuckDB local, se ler Delta direto) — D4 |
| A amostra não pega incompatibilidade com o Spark 3.5 | checagem automática da fase 1 + a 1ª execução real é a de medição (§6.2) |
| Pseudo-SQL desta spec com erro de detalhe | os cenários C1–C10 cobrem cada caminho da tabela de ações |

## 8. Tarefas (uma tabela por vez; cada uma com commit próprio)

- [ ] **T0 — Preparação** (sem escrita na base real)
  - Reautenticar o CLI (`databricks auth login --profile tcc`; hoje o *refresh token* está inválido) e rodar,
    num lote único no warehouse: Q1 `DESCRIBE DETAIL` + `SHOW TBLPROPERTIES` das 4 bronzes e 4 SCD2 (tamanho,
    nº de arquivos, partição, `delta.enableDeletionVectors`); Q2 `SELECT valid_since, COUNT(*)` por SCD2
    (versões novas por mês); Q6 `DESCRIBE HISTORY` das SCD2 (`numTargetRowsCopied`,
    `materializeSourceTimeMs`), para confirmar o diagnóstico do `MERGE`.
  - Na amostra: referência + `DEEP CLONE` (§6.1) e as duas consultas de confirmação (prefixo e cópias).
  - DQ de continuidade e unicidade em `dq_empresas`, `dq_estabelecimentos` e `dq_dados_simples`.
  - **Aceite:** números da Q1/Q2 anotados na §10; DQ novos passam na referência; prefixos únicos.
- [ ] **T1 — `dados_simples.sql`** no desenho novo (§3.2–3.5).
  - **Aceite:** C1–C9 com 0/0 e contagens por mês iguais; `EXPLAIN` conforme §3.8; DQ passa; checagem de
    compatibilidade passa.
- [ ] **T2 — `estabelecimentos.sql`** (mesmo molde). **Aceite:** igual à T1.
- [ ] **T3 — `empresas.sql`** (mesmo molde). **Aceite:** igual à T1.
- [ ] **T4 — `socios.sql`** (§3.6). **Aceite:** igual à T1 + C10 = 0; com o mês anterior faltando, a
  instrução 1 falha; `dq_socios` (saídas ≤ 5%) passa.
- [ ] **T5 — Medição na base real** (§6.2), com o usuário ligando NAT/cluster. **Aceite:** EXCEPT 0/0
  contra o clone (ou diferença explicada na §10); cada `si_*` pelo menos 2× mais rápida e com pelo menos 3×
  menos shuffle que o B20; custo de um mês incremental registrado.
- [ ] **T6 — Documentação:** `docs/specs/silver_intermediate.md` (padrão SCD2 novo), `src/silver_intermediate/readme.md`,
  `docs/decisions.md` (D-18 "SCD2 em duas fases", com as escolhas de D1–D5), `docs/backlog.md` (B20: parte
  silver feita) e sugestão em `docs/tcc.md` (se o texto descreve o `MERGE` do SCD2).
- [ ] **T7 — (se D4 = sim)** ligar *deletion vectors* nas 4 SCD2 e medir um mês incremental com e sem.

## 9. Decisões para o usuário

| Id | Escolha | Opções | Recomendação |
|---|---|---|---|
| D1 | Desenho | (a) **duas fases** (esta spec); (b) uma passada larga só: mesmo fluxo de ações, mas a bronze larga atravessa a troca uma vez (§10) | **(a)**. Ganho ~2–3× maior na troca e nas ordenações; o custo extra é a lista difundida e o mês denso. (b) fica como plano B se o *broadcast* der problema |
| D2 | Impressão digital curta | (a) prefixo de 16 hex do SHA-256 na fase 1; (b) hash completo de 64 chars | **(a)**. Probabilidade de erro ~3·10⁻¹⁰ na janela inteira; a linha estreita cai para ~40% |
| D3 | Escrita | (a) `MERGE` estreito + `INSERT` (dois *commits*); (b) um `MERGE` único (atômico) | **(a)**. O `MERGE` único precisa de join externo completo com as inserções e embaralha as linhas largas novas (100% na carga inicial); a reexecução conserta uma falha entre os dois |
| D4 | *Deletion vectors* nas SCD2 | ligar ou não | **ligar depois da T5**, se o DBR 15.4 os usar no `MERGE` (conferir `numDeletionVectorsAdded` no histórico) e se nenhum leitor externo quebrar. Muda o protocolo da tabela, sem volta |
| D5 | Mês faltando na bronze | (a) falhar (todas as tabelas); (b) pular em silêncio (hoje, nos três não-socios) | **(a)**. Com o desenho novo, um mês sem bronze num reprocessamento apagaria as versões dele; falhar antes é mais seguro e igual ao que socios já faz |
| D6 | Socios: snapshot parcial | acrescentar à instrução 1 "linhas(M) ≥ 95% de linhas(M−1)" ou deixar só o DQ de saídas (5%) | **acrescentar**: custa uma contagem por partição e impede gravar saídas falsas, em vez de só acusá-las depois |
| D7 | Diferenças intencionais da §4 (versão órfã apagada; `inserted_at` da bronze nova na substituição) | aceitar ou replicar o comportamento antigo | **aceitar**: só ocorrem com bronze alterada, e o novo é o correto |
| D8 | Tamanho do bloco | 6 ou 12 meses | **12** no piloto (guarda da instrução 4 protege); cair para 6 se a guarda falhar ou o tempo passar de ~1 h por bloco |

## 10. Alternativas consideradas

- **Uma passada larga (D1-b):** mesma estrutura de ações, mas a fase 1 leva as colunas largas brutas pela
  única troca e já sai com os atributos (limpeza depois do filtro). É mais simples (sem instruções 3, 4 e 6
  separadas) e já elimina ~2/3 do shuffle de hoje. Perde para as duas fases porque embaralha e ordena todas as
  linhas largas de todos os meses.
- **`INSERT OVERWRITE` da tabela inteira a cada execução:** simples, mas o custo cresce com o histórico e
  reescreve versões que não mudaram. Descartada: o `INSERT` *append* + `MERGE` estreito já cobre a carga inicial.
- **`MERGE` único com fonte mínima:** atômico, mas o join externo completo com as inserções embaralha as
  linhas largas novas (D3).
- **Laço mês a mês em Python** (busca com *broadcast* por mês, chave em texto): dispensaria a chave de 64
  bits, mas tira o SQL do formato do Job e multiplica o custo fixo por mês.
- **Particionar ou clusterizar a silver** (`is_current`, `valid_until`, pk) para podar o `MERGE`: as
  mudanças se espalham por todas as pks; o ganho é pequeno perto das *deletion vectors* (D4).
- **`CACHE TABLE` / *temp view*:** não funcionam no runner da amostra (instruções em sessões independentes).
  As tabelas de trabalho Delta funcionam nos dois runners e permitem inspecionar as ações.

## 11. Notas de execução

(preencher na implementação: resultados da T0, planos `EXPLAIN`, cenários da amostra, medição real)

- 2026-11-16: consultas de confirmação não rodadas — CLI Databricks com *refresh token* inválido (mesmo
  bloqueio da linha de base). Volumes da §2.1 vêm das contagens de 2026-11-15.
