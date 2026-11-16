# Feature: otimização do log de eventos, do fato empresa × mês e do dashboard (fase 3)

**Status:** **implementado e validado na amostra** (2026-11-16; resultados no §12) — T-L2..T-L5, T-F1, T-F1b,
T-F2, T-D1, T-D2, T-B1, T-B2, T-B4 e a tarefa adicional T-B5 (quadro "ponte" da página 1). **Pendente de cluster:**
T-D0 (EXPLAIN da dim), aceite de desempenho de todas as tarefas (§5.5, T-X2), T-X1 com as SCD2 da F2, T-B3
(aprovação do autor na prévia). U3, U4 e U7 continuam em aberto (§8).
**Camadas afetadas:** silver_intermediate (log), silver_final (dim_empresa, fato_mudanca_estado), gold (fato, painel,
tabelas novas de dashboard), dashboard Lakeview
**Branch:** `otimizacao_pipeline` (spec guarda-chuva: [`2026-11-16-otimizacao-pipeline.md`](2026-11-16-otimizacao-pipeline.md);
fase 2: [`2026-11-16-otimizacao-scd2.md`](2026-11-16-otimizacao-scd2.md), chamada aqui de **F2**)

---

## Roteiro de leitura (1 página)

**O problema, em uma frase por item.**

- **Log de eventos (873 s, era 241 s).** Para achar "o que mudou de um mês para o outro", a versão nova ordena **o
  histórico inteiro** de cada tabela SCD2, várias vezes, e só no fim olha para os meses pedidos. A versão antiga
  filtrava os meses primeiro. Com 40 meses de histórico, isso fica cada vez pior, mesmo rodando blocos pequenos.
- **Fato empresa × mês (84 GB de shuffle).** Ele "desdobra" cada empresa em uma linha por mês, **inclusive as ~30 milhões
  de empresas baixadas há anos**, que depois são jogadas fora porque não geram linha nenhuma. E faz um join depois do
  desdobramento, quando os dados já estão multiplicados.
- **Painel setorial.** É reconstruído do zero a partir do fato inteiro a cada execução; com 40 meses, ~900 milhões de
  linhas lidas para gravar 6 meses novos.
- **dim_empresa (spill de 7,6 GB).** Junta tabelas largas (endereço, contatos, CNAEs) por intervalo de datas; as linhas
  largas é que pesam na ordenação.
- **Dashboard.** O Lakeview **não corta** dataset grande; ele manda **cada clique** rodar de novo a consulta do dataset no
  SQL warehouse. Os datasets grandes fazem contas pesadas (varrem o fato de todos os meses, agregam 23 milhões de
  empresas) a cada clique. `mercado` não cresce com os meses (é só o último mês); `fluxos_empresa` cresce ~500 mil
  linhas por mês.

**O desenho, em uma frase por item.**

- **Log:** antes de ordenar, ficar só com as versões que **mudaram nos meses pedidos** e com a versão imediatamente
  anterior delas (é tudo o que um evento precisa). O nascimento da empresa olha só os CNPJs cuja matriz mudou na
  janela. O log passa a gravar `cnpj_basico`, o que poupa o `fato_mudanca_estado` de reler as quatro SCD2.
- **Fato:** empresa não ativa gera no máximo **uma** linha (no mês em que mudou), sem desdobrar; o "mês anterior" vem
  de um join simples, só para versões que começam na janela; o join do porte MEI (D-17b) vai para antes do
  desdobramento. O **painel** passa a gravar só os meses da janela.
- **dim_empresa:** os joins por intervalo passam a usar só as **chaves** de cada tabela; as colunas largas entram
  depois, por um join de igualdade.
- **Dashboard:** o Job grava **tabelas gold já no formato que cada gráfico usa**; o dataset do Lakeview vira um
  `SELECT` simples dessas tabelas. Os números ficam iguais por construção.

**O que não muda:** nenhum número (dashboard e DQ da gold), nenhuma regra de negócio (D-13, D-17, D-17b), nenhuma coluna
das SCD2. Cada mudança é provada igual na amostra de 1% comparando a tabela nova com a atual linha a linha.

**O que muda no esquema:** coluna nova `cnpj_basico` no log (com recriação do log em todos os meses) e as tabelas novas
`gold.dash_*`. Ambas exigem uma **carga completa na migração** (§3.6), senão as séries do dashboard ficariam cortadas.

**Decisões do autor.** Já tomadas (por omissão, comunicadas ao gerente): **U1** sim (`cnpj_basico` no log), **U5** sim
(painel incremental), **U6** manter o cruzamento bairro × CNAE. Em aberto: **U3** (desligar o *optimized write* no fato
— só depois de medir), **U4** (coluna de primeira aparição na `dim_empresa` — só se ainda pesar) e **U7** (o que fazer
se o filtro de bairro passar de 100 mil valores — medir primeiro).

---

Princípio que manda em tudo aqui (guarda-chuva §2): **otimização não muda número**. Cada mudança é validada na
amostra de 1% com `EXCEPT ALL` nos dois sentidos = 0 linhas; desempenho só se mede na base real.

Convenção de status: **implementado** = já está no código hoje; **proposto** = desenho desta spec; **a medir** =
hipótese que depende de dado ainda não consultado.

> Nota de execução (2026-11-16): as consultas de confirmação **não rodaram** — o CLI do Databricks está com o refresh
> token inválido. Tudo que depende de número está marcado **a medir**; as consultas estão no §5.4 e na revisão (§7).

---

## 1. Requisito

- **Problema:** antes de rodar a janela de ~40 meses (em blocos de 6–12 meses, cluster clássico DBR 15.4 / Spark 3.5,
  r5d.xlarge, 1–4 workers spot, sem serverless no Job), estes pontos custam muito ou não escalam:
  1. `si_log_eventos_negocio`: 873 s (241 s antes da nova semântica); `sf_fato_mudanca_estado` 1,7× mais lenta.
  2. `gold_fato_empresa_mes`: 84 GB de shuffle para 0,9 GB de entrada; o painel recalcula o fato inteiro.
  3. `sf_dim_empresa`: única tarefa com spill (7,6 GB).
  4. Dashboard Lakeview: datasets grandes (`mercado` 6,7 mi linhas, `mercado_bairros` 2,8 mi, `fluxos_empresa` 1,5 mi).
- **Quem consome:** o próprio Job (tempo e custo por bloco) e o dashboard oficial (tempo de resposta).
- **Fora de escopo:** reescrita dos SCD2 da silver (F2; colunas e semântica das SCD2 **não mudam**); bronze (B17);
  regras de negócio; bugs que não são de desempenho.

---

## 2. Diagnóstico por arquivo

### 2.1 `src/silver_intermediate/ingest/log_eventos_negocio.sql` (implementado)

**Leituras por execução** (cada `FROM`/`JOIN` de tabela; o Spark não materializa CTE: cada referência é recalculada, a
menos que o plano final seja idêntico e caia em *ReusedExchange*):

| Tabela | Onde é lida (linhas) | Leituras no texto |
|---|---|---|
| `estabelecimentos` | `params` MIN (49); `est_versoes` (65–81); `matriz_canonica` lado `e` (183); lado `melhor` do ANTI JOIN (188) | 4 |
| `dados_simples` | `params` MIN (50); `sim_versoes` (269–283) | 2 |
| `socios` | `params` MIN (51); `soc_versoes` (353–364) | 2 |
| `empresas` | `pk_empresa` (206–211); `emp_versoes` (237–245) | 2 |

O que pesa é o **plano**: `est_janela` é usado por 4 ramos (94–170), `sim_janela` por 6 ramos do `UNION ALL` (292–334)
e `soc_janela` por 4 (372–405). Cada ramo usa colunas diferentes do `LAG`; o *column pruning* do Spark tira de cada ramo
as expressões de janela que ele não usa, os sub-planos deixam de ser idênticos e o reaproveitamento do shuffle não
acontece. Resultado provável: **uma leitura + um shuffle + uma ordenação da SCD2 inteira por ramo** — o "lê cada SCD2
4–5×" do B20. **A medir** com `EXPLAIN FORMATTED` no cluster (§5.4).

**Por que ficou 3,6× mais lento depois da mudança de semântica** (commit `4f84c49`): a versão antiga detectava
transições por self-join `antigo JOIN novo ON antigo.pk = novo.pk AND antigo.valid_until = novo.valid_since WHERE
novo.valid_since BETWEEN ini AND fim`. O filtro da janela descia para o lado `novo` e, pela igualdade, o otimizador
inferia `antigo.valid_until BETWEEN ini AND fim` — **os dois lados chegavam ao shuffle já reduzidos à janela**. A versão
nova usa `LAG(...) OVER (PARTITION BY pk ORDER BY valid_since)` sobre a **tabela inteira** e só depois filtra a janela
(85–90, 284–289, 365–370). Um filtro em `valid_since` não pode descer para baixo de uma janela particionada por `pk`
(mudaria o resultado do `LAG`), então o Spark embaralha e ordena **todo o histórico**, em cada ramo. Somam-se dois
custos novos, também proporcionais ao histórico:

- `matriz_canonica` (177–198): ANTI JOIN de todas as versões de matriz contra todas as do mesmo `cnpj_basico`, com
  condição de sobreposição — antes, o nascimento era um filtro por `data_inicio_atividade`.
- `pk_empresa` (206–211): `GROUP BY cnpj_basico` sobre `empresas` inteira, para usar só as nascidas na janela.

Consequência para os 40 meses: **o custo do log cresce com o histórico, não com o tamanho do bloco.**

Observação: na execução de 4 meses a janela (2023-05..08) **contém o baseline**. Versões que começam no baseline nunca
geram evento (ordem 1 no baseline é carga inicial; ordem > 1 no baseline é impossível, porque o baseline é o menor
`valid_since`). Hoje elas passam pelo shuffle mesmo assim — e são a maioria das linhas.

### 2.2 `src/silver_final/ingest/fato_mudanca_estado.sql` (implementado)

- `mapeamento_pk` (34–50): `SELECT DISTINCT entidade_tipo, pk, cnpj_basico` das **quatro SCD2 inteiras** e `UNION`
  (deduplicação = mais um shuffle), só para descobrir o `cnpj_basico` de cada `pk` do log da janela.
- `primeira_versao_empresa` (88–98): `ROW_NUMBER` sobre a `dim_empresa` inteira.
- `versoes_pareadas` (99–129): dois joins com `dim_empresa` por `cnpj_raiz` + intervalo (`d_after` 117–121, `d_before`
  logo abaixo), sem restringir a dimensão à janela.
- `resultado` (131–147): `DISTINCT` sobre linhas largas.
- Causa provável da lentidão (1,7×), **a medir**: mais eventos passando pelos mesmos joins (C-16, D-17, primeira
  aparição), a `dim_empresa` maior (C-02) e o `DISTINCT` mais largo — o commit `8066676`, da mesma leva da semântica,
  acrescentou `data_evento`, `eh_matriz` e `codigo_motivo_norm` a `logs_base`, o join `dme.codigo_motivo <=> ...` e
  duas colunas no `DISTINCT`.

### 2.3 `src/silver_final/ingest/dim_empresa.sql` (implementado)

| Passo | Linhas | Custo |
|---|---|---|
| `est_matriz`: ANTI JOIN da matriz canônica | 136–187 (join em 177–182) | self-join de **linhas largas** (≈25 colunas: endereço, contatos, CNAEs) por `cnpj_basico` com condição de sobreposição; as duas pontas são todas as versões de matriz |
| `fronteiras`: 6 projeções com `UNION` | 213–220 | shuffle estreito |
| `fatias`: `LEAD` por `cnpj_basico` | 224–234 | shuffle estreito |
| `unificado_raw`: 3 joins por `cnpj_basico` + intervalo | 240–269 | sort-merge joins com o lado largo de `est_matriz`, `emp` e `sim` ordenado; o resultado intermediário carrega todas as colunas largas |

O spill (7,6 GB) é compatível com a ordenação de linhas largas no sort-merge; o *buffer* do sort-merge por chave também
pode contribuir (revisão R26). **A medir** no `EXPLAIN` e no event log antes de investir (§5.4). **Range join hint não
resolve:** a otimização do Databricks só aceita colunas numéricas, `DATE` ou `TIMESTAMP` na condição de intervalo (os
meses aqui são `STRING 'YYYY-MM'`) e, em `LEFT OUTER`, só o caso ponto-à-esquerda; ANTI JOIN não é coberto
([docs: range join](https://docs.databricks.com/aws/en/optimizations/range-join)). Já existe chave de igualdade
(`cnpj_basico`) com poucas versões por chave — o problema é a largura das linhas, não a explosão do intervalo.

### 2.4 `src/gold/fato_empresa_mes.sql` (implementado)

O fato **já é incremental na escrita** (`INSERT INTO ... REPLACE WHERE` da janela, 82–84). O que não escala é a
leitura/expansão:

| Passo | Linhas | Custo |
|---|---|---|
| `primeira_aparicao`: `GROUP BY cnpj_raiz` na dim inteira | 104–113 | shuffle estreito (2 colunas), proporcional ao histórico — aceitável |
| `versoes_janela` | 143–171 | inclui **todas as versões correntes**, também as ~30+ mi de empresas baixadas há anos (`fim_vigencia` NULL) |
| `versoes_com_anterior`: `LAG` por `cnpj_raiz` | 172–189 | shuffle + ordenação de **todas** as versões da janela, largas, **antes** do filtro que descarta as mortas (219–220) |
| `versoes_a_expandir`: `ATIVA OR inicio >= mes_ini` | 200–221 | na carga inicial `mes_ini` = baseline: **toda** versão começa em `mes_ini`, então as mortas também são expandidas; join com `ponte_primaria` (217) sem dica de broadcast |
| `meses_empresa`: `EXPLODE(SEQUENCE(...))` | 222–230 | mortas × 4 meses viram ~120 mi de linhas que o `WHERE` final (360–364) descarta |
| `LEFT JOIN mei_na_baixa` | 355–359 | **depois** da expansão e **antes** do `WHERE`: se virar sort-merge (o tamanho de `mei_na_baixa` não é conhecido no plano), embaralha as centenas de milhões de linhas expandidas |
| escrita com *optimized write* (conf. global do cluster, `gerar_job.py:87`) | — | um shuffle do resultado final (~23 mi linhas/mês) |
| 5 × `ALTER COLUMN ... COMMENT` | 58–69 | 5 commits Delta por execução, sem efeito: o `CREATE` (19–53) já tem os mesmos textos |

Hipótese para os 84 GB (**a medir**): expansão das mortas + join depois da expansão + shuffle do optimized write.

### 2.5 `src/gold/fato_indicadores_setoriais_mes.sql` (painel, implementado)

`CREATE OR REPLACE TABLE ... AS SELECT ... FROM fato_empresa_mes` **inteiro** a cada execução (~900 mi de linhas lidas
por bloco com 40 meses). É o "recalcula tudo" de verdade da gold.

### 2.6 Dashboard (`src/gold/dashboard/gerar_dashboard.py`)

**Limites oficiais do AI/BI (Lakeview)** ([limits](https://docs.databricks.com/aws/en/dashboards/limits),
[caching](https://docs.databricks.com/aws/en/dashboards/caching), consultados em 2026-11-16):

- "Dataset size has no hard limit, but large datasets are processed differently." **Não há corte de dataset.**
- Dataset **≤ 100.000 linhas e ≤ 100 MB**: o resultado vai para o navegador; filtros e agregações rodam lá.
- Dataset **> 100.000 linhas ou > 100 MB**: a consulta do dataset é embrulhada num `WITH` e **cada widget/filtro vira uma
  consulta no SQL warehouse**; aplicar filtro reexecuta a consulta (salvo cache de 24 h com os mesmos filtros).
- Gráficos desenham até 15 mil linhas (tabelas, 100 mil) — o resultado já agregado do widget.
- Filtros dropdown/multiselect listam até **100.000 valores distintos**.
- Materialização de datasets do próprio dashboard: Beta, roda em **serverless**, não aceita dataset com parâmetro
  ([materialization](https://docs.databricks.com/aws/en/dashboards/materialization)) — descartada (sem serverless no
  processamento).

O problema real não é "truncar": é que os datasets grandes **recalculam a consulta inteira no warehouse a cada
clique**, e o SQL deles é pesado (`fluxos_empresa` varre o fato de todos os meses; `mercado` agrega ~23 mi de linhas;
`baixas_motivo` junta fato × OBT).

**Quem consome cada dataset grande:**

| Dataset (linhas do gerador) | Cresce com os meses? | Widgets | Filtros da página | Agregações |
|---|---|---|---|---|
| `fluxos_empresa` (195–215) | **sim, linear** (~500 mil/mês → ~20 mi em 40 meses) | P2: `c2_mei_ab`, `c2_mei_bx`, `b2_porte`; P4: `c4_falencia`, `c4_idade`, `c4_menos_2` | P2: UF, área (572–573); P4: UF, porte, área (631–633) | `AVG(pct_*)`, `PERCENTILE(idade_baixa_anos, 0.5)`, `SUM(qtd)` por fluxo × porte |
| `mercado` (293–297) | **não** (só o último snapshot) | P5: `c5_ativas`, `b5_porte`, `b5_area` | UF, município, bairro, CNAE (675–678) | `SUM(empresas_ativas)` |
| `mercado_bairros` (301–320) | não | P5: `b5_bairro` | idem | `SUM` por bairro |
| `mercado_empresas` (323–330) | não (LIMIT 1000) | P5: `t5_empresas` | idem | linhas |
| `eventos` (253–265) | sim (grão inclui `data_referencia`) | P4: `b4_tipo` | UF, porte, área | `SUM(qtd_eventos)` por tipo |
| `baixas_idade` (217–222) | sim | P4: `b4_idade` | UF, porte, área | `SUM(qtd_baixas)` por faixa |
| `baixas_motivo` (228–250) | sim; join fato × OBT a cada consulta | P4: `b4_motivo` | UF, porte, área | `SUM(qtd_empresas)` por categoria |

Nenhum widget das páginas 2, 4 e 5 usa o mês; `data_referencia` nos grãos de `eventos`, `baixas_idade` e
`baixas_motivo` só multiplica linhas. Os datasets do painel (`painel`, `estoque_mes`, `resumo_mes`, taxas,
`top_cnaes`) ficam em ~1,3 mil linhas/mês → ~54 mil em 40 meses: continuam no navegador e **ficam como estão** (mas
dependem do painel ter **todos** os meses — §3.6).

---

## 3. Desenho proposto

**Compatibilidade (todas as propostas):** só Spark 3.5 / DBR 15.4 (`TRY_TO_TIMESTAMP` sim, `TRY_TO_DATE` não);
`OPTIMIZE` **sem** `ZORDER`; `QUALIFY` é permitido (o log já usa na linha 203 e rodou no cluster do Job); nada de `SET`
de configuração do Spark nos `.sql` (o SQL warehouse da amostra recusa, e a validação inteira pararia). A fonte de
verdade dessa lista é `scripts/checar_sql_cluster.py` (fase 1). `INSERT INTO ... REPLACE WHERE` é suportado em SQL desde
o DBR 12.2 LTS, também em tabela não particionada ou com liquid clustering, com predicado sobre colunas que não são de
partição ([selective overwrite](https://docs.databricks.com/aws/en/delta/selective-overwrite)); `REPLACE USING` /
`REPLACE ON` (DBR 16.3 / 17.1) **não** existem no 15.4. Mensagens de `ASSERT_TRUE` novas: números com
`CAST(... AS STRING)` (convenção de `dq_gold.sql:5`) e **sem as palavras `INSERT` ou `MERGE` no texto** — o
`run_sql.py` (`_eh_consulta`, 94–97) deixa de executar a asserção se as encontrar, mesmo dentro de string.

### 3.1 Log de eventos — só o que importa à janela

**Pré-requisito (da F2, tarefa T0):** DQ de contiguidade nas quatro SCD2 — `valid_until IS DISTINCT FROM
LEAD(valid_since)` = 0 (pega também versão fechada sem sucessora) e exatamente uma versão corrente por `pk`
(`sem_corrente` = 0), no molde de `dq_socios.sql:19-42`. A F3 **não** cria asserção própria: colocá-la no início do log
recriaria a ordenação do histórico inteiro que esta seção elimina. Sem essa garantia, um buraco numa `pk` (por exemplo,
uma execução da F2 interrompida e seguida de outra janela, revisão R3) faria o log abaixo gerar eventos falsos de
"primeira aparição".

**Ideia 1 — filtrar antes do `LAG` (o ganho principal).** Para um evento detectado no mês M da janela só importam:
(a) as versões que começam na janela **depois do baseline** e (b) a versão imediatamente anterior de cada uma. Como as
versões de uma `pk` são contíguas (`valid_until` = `LEAD(valid_since)`, `estabelecimentos.sql:183`, `empresas.sql:98`,
`dados_simples.sql:109`, `socios.sql:219`, garantido pelo DQ acima), a anterior de uma versão que começa em M termina
exatamente em M — tem `valid_until` na janela. Filtro:

```sql
WHERE (valid_since BETWEEN '{{mes_referencia_inicial}}' AND '{{mes_referencia_final}}'
       AND valid_since > <baseline da tabela>)
   OR  valid_until BETWEEN '{{mes_referencia_inicial}}' AND '{{mes_referencia_final}}'
```

Prova de equivalência (para a defesa): sobre o conjunto filtrado, `LAG` por `pk` ordenado por `valid_since` devolve,
para cada versão de (a), a mesma anterior que devolveria sobre a tabela inteira — ela está no conjunto e nenhuma outra
versão da mesma `pk` fica entre as duas. "É a primeira versão da pk" (`ordem_versao = 1`) vira "não tem anterior no
conjunto": se existisse anterior na tabela, ela terminaria dentro da janela e estaria no conjunto. Versões que começam
no baseline nunca geram evento (§2.1), então sair do conjunto não muda nada.

Implementação: cada CTE `*_versoes` (65–81, 237–245, 269–283, 353–364) passa a ler a SCD2 com esse `WHERE`; os ramos de
eventos (94–170, 247–264, 292–334, 372–405) ficam **como estão**, trocando `ordem_versao = 1` / `> 1` por
`eh_primeira_versao` / `NOT eh_primeira_versao`:

```sql
est_versoes AS (
    SELECT pk, cnpj_basico, identificador_matriz_filial, situacao_cadastral_desc, motivo_situacao_cadastral,
           data_situacao_cadastral, cnae_fiscal_principal, cnae_fiscal_secundaria, valid_since,
           LAG(valid_since) OVER w IS NULL         AS eh_primeira_versao,
           LAG(situacao_cadastral_desc)   OVER w   AS ant_situacao_desc,
           LAG(motivo_situacao_cadastral) OVER w   AS ant_motivo,
           LAG(cnae_fiscal_principal)     OVER w   AS ant_cnae_principal,
           LAG(cnae_fiscal_secundaria)    OVER w   AS ant_cnae_secundaria
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    CROSS JOIN params p
    WHERE (e.valid_since BETWEEN p.mes_ini AND p.mes_fim AND e.valid_since > p.baseline_est)
       OR  e.valid_until BETWEEN p.mes_ini AND p.mes_fim
    WINDOW w AS (PARTITION BY pk ORDER BY valid_since)
),
est_janela AS (   -- só as versões novas; as anteriores serviram só ao LAG (idem sim_janela, soc_janela)
    SELECT v.*, p.baseline_est FROM est_versoes v CROSS JOIN params p
    WHERE v.valid_since BETWEEN p.mes_ini AND p.mes_fim AND v.valid_since > p.baseline_est
)
```

(`params` precisa ganhar `baseline_emp` = `MIN(valid_since)` de `empresas`, que hoje não existe; os MIN são leituras de
uma coluna, baratas.) Mesmo que o Spark continue recalculando `est_versoes` em cada ramo, cada recálculo agora embaralha
só as versões que mudaram no bloco, não o histórico.

**Ideia 2 — uma passada por tabela (condicionada à medição).** Juntar os ramos de cada SCD2 num único `SELECT` que
monta um array de eventos possíveis e o explode (`LATERAL VIEW INLINE(FILTER(ARRAY(IF(cond, NAMED_STRUCT(...), NULL),
...), x -> x IS NOT NULL))`). Elimina as leituras repetidas, mas de um conjunto que a Ideia 1 já deixou pequeno, e o
código fica menos direto de explicar. **Só se faz** se a medição real da Ideia 1 mostrar os ramos repetidos ainda entre
os passos caros (T-L2b).

**Ideia 3 — nascimento sem self-join do histórico inteiro.** Só pode nascer na janela um CNPJ que tenha **alguma versão
de matriz começando na janela** — e essas versões já estão em `est_versoes`. Para esses candidatos, lê-se o histórico
completo de matriz (colunas estreitas) e aplica-se a regra da matriz canônica de hoje:

```sql
candidatos AS (
    SELECT DISTINCT cnpj_basico FROM est_janela
    WHERE identificador_matriz_filial = '1' AND cnpj_basico IS NOT NULL
),
hist_matriz AS (   -- histórico COMPLETO de matriz, só dos candidatos, só colunas estreitas
    SELECT e.pk, e.cnpj_basico, e.cnpj_ordem, e.data_inicio_atividade, e.valid_since, e.valid_until
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    LEFT SEMI JOIN candidatos c ON c.cnpj_basico = e.cnpj_basico
    WHERE e.identificador_matriz_filial = '1'
),
matriz_canonica AS (   -- MESMA regra de hoje (177–198), sobre o conjunto reduzido
    SELECT h.cnpj_basico, h.pk, h.data_inicio_atividade, h.valid_since
    FROM hist_matriz h
    LEFT ANTI JOIN hist_matriz melhor
        ON melhor.cnpj_basico = h.cnpj_basico AND melhor.cnpj_ordem < h.cnpj_ordem
       AND melhor.valid_since < COALESCE(h.valid_until, '9999-12')
       AND COALESCE(melhor.valid_until, '9999-12') > h.valid_since
    WHERE h.valid_since IS NOT NULL AND h.valid_since < COALESCE(h.valid_until, '9999-12')
),
primeira_versao_matriz AS (   -- igual a hoje (200–204), com QUALIFY
    SELECT cnpj_basico, data_inicio_atividade, valid_since FROM matriz_canonica
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cnpj_basico ORDER BY valid_since, pk) = 1
),
pk_empresa AS (   -- só para os CNPJs candidatos, não para empresas inteira
    SELECT emp.cnpj_basico, MIN(emp.pk) AS pk
    FROM tcc_receita.silver_intermediate.empresas emp
    LEFT SEMI JOIN primeira_versao_matriz mat ON mat.cnpj_basico = emp.cnpj_basico
    WHERE emp.cnpj_basico IS NOT NULL
    GROUP BY emp.cnpj_basico
)
-- ev_empresa_nasceu: igual a hoje (214–232), com os filtros > baseline_est e BETWEEN mes_ini AND mes_fim.
```

Equivalência: a primeira versão canônica de um CNPJ que nasce na janela é uma versão de matriz que começa na janela,
então o CNPJ está em `candidatos`; para ele, `hist_matriz` tem **todo** o histórico de matriz (o lado `melhor` continua
sem filtro de validade, como hoje), e a regra dá o mesmo resultado.

**Ideia 4 — `cnpj_basico` no log (U1, decidida: sim).** O log já lê as SCD2, que têm `cnpj_basico`. Cada ramo passa a
projetar `cnpj_basico` da própria linha da SCD2 (no nascimento, o de `mat`), gravado como **última coluna** do log. Não é
extrair CNPJ da `pk`: o valor vem da coluna `cnpj_basico` da linha de origem. Migração e carga completa: §3.6.

### 3.2 `fato_mudanca_estado`

- `logs_base` (51–71) usa `l.cnpj_basico AS cnpj_raiz`; o `mapeamento_pk` (34–50) é removido.
- `d_after`, `d_before` e `primeira_versao_empresa` passam a ler só a `dim_empresa` dos CNPJs presentes no log da janela
  (`LEFT SEMI JOIN` por `cnpj_raiz`), com as mesmas condições. Um evento só casa versões do próprio CNPJ, então a saída
  não muda.

### 3.3 `fato_empresa_mes` — expandir só o que vira linha

O resultado não muda; muda o caminho.

**Corte A — versão não ATIVA gera no máximo uma linha, no mês em que começa.** Dentro de uma versão a situação não
muda. Numa versão não ATIVA, para todo mês M > início: `situacao_inicio_mes` = a própria situação, então
`ativa_inicio_mes` e `ativa_fim_mes` são FALSE, não há abertura nem mudança de situação → o `WHERE` final (360–364)
descarta. Só o mês de início pode gerar linha, e só se for > baseline e estiver na janela. Logo: versões ATIVA expandem
como hoje; versões não ATIVA geram uma linha (`data_referencia` = `data_inicio_vigencia`) quando `inicio_vigencia
BETWEEN mes_ini AND mes_fim` e `inicio_vigencia > mes_baseline`; as demais não ATIVA não geram nada.

**Corte B — o "mês anterior" por join de igualdade, só para versões que começam na janela.** O `LAG` (172–189) só é
usado no mês de início da versão (nos outros meses, `situacao_inicio_mes` é a própria situação, e o porte anterior só
entra com `saiu_estoque_mes`, impossível no meio da versão); e o valor do `LAG` só vale se `fim_vigencia_anterior =
inicio_vigencia`. Então:

```sql
novas AS (          -- começam na janela, depois do baseline (pequeno: só o que mudou)
    SELECT v.* FROM versoes_janela v CROSS JOIN params p CROSS JOIN limites_dim l
    WHERE v.inicio_vigencia BETWEEN p.mes_ini AND p.mes_fim AND v.inicio_vigencia > l.mes_baseline
),
anteriores AS (     -- candidatas a anterior: terminam dentro da janela (estreito)
    SELECT v.cnpj_raiz, v.fim_vigencia, v.situacao_cadastral AS situacao_versao_anterior,
           v.enquadramento_porte_analitico AS porte_versao_anterior,
           v.opcao_pelo_mei AS mei_versao_anterior, v.opcao_pelo_simples AS simples_versao_anterior
    FROM versoes_janela v CROSS JOIN params p
    WHERE v.fim_vigencia BETWEEN p.mes_ini AND p.mes_fim
),
novas_com_anterior AS (
    SELECT n.*, a.fim_vigencia AS fim_vigencia_anterior, a.situacao_versao_anterior, a.porte_versao_anterior,
           a.mei_versao_anterior, a.simples_versao_anterior,
           (pa.cnpj_raiz IS NOT NULL) AS eh_primeira_versao,
           (mb.cnpj_raiz IS NOT NULL) AS eh_mei_na_baixa            -- Corte C
    FROM novas n
    LEFT JOIN anteriores a ON a.cnpj_raiz = n.cnpj_raiz AND a.fim_vigencia = n.inicio_vigencia
    LEFT JOIN primeira_aparicao pa ON pa.cnpj_raiz = n.cnpj_raiz AND pa.mes_primeira_aparicao = n.inicio_vigencia
    LEFT JOIN mei_na_baixa mb ON mb.cnpj_raiz = n.cnpj_raiz AND pa.cnpj_raiz IS NOT NULL
),
antigas AS (        -- começaram antes da janela, ou no baseline: as colunas de "anterior" não são usadas
    SELECT v.*, CAST(NULL AS STRING) AS fim_vigencia_anterior, CAST(NULL AS STRING) AS situacao_versao_anterior,
           CAST(NULL AS STRING) AS porte_versao_anterior, CAST(NULL AS BOOLEAN) AS mei_versao_anterior,
           CAST(NULL AS BOOLEAN) AS simples_versao_anterior, FALSE AS eh_primeira_versao, FALSE AS eh_mei_na_baixa
    FROM versoes_janela v CROSS JOIN params p CROSS JOIN limites_dim l
    WHERE (v.inicio_vigencia < p.mes_ini OR v.inicio_vigencia = l.mes_baseline)
      AND v.situacao_cadastral = 'ATIVA'                               -- Corte A
)
-- versoes_a_expandir = (novas_com_anterior UNION ALL antigas) + CNAE (como hoje, 200–221);
-- nas não ATIVA, data_ultimo_mes = data_primeiro_mes = data_inicio_vigencia (Corte A).
```

Por que é igual: no baseline nada termina, então versão que começa no baseline nunca tem anterior contígua — o `LAG` de
hoje dá NULL e `eh_baseline` zera os fluxos; `antigas` reproduz isso. Sem sobreposição de versões (DQ de colisão de SK
da dim), a versão com `fim = inicio` é exatamente a anterior do `LAG`.

O caminho largo (`antigas`, ~23 mi de ativas) **não passa mais pelo `LAG`** nem pelo join do D-17b. Ele ainda passa
pelo join com `ponte_primaria`/`dim_cnae` (217–218), como hoje: sem dica de broadcast e com o tamanho de
`dim_grupo_cnae` desconhecido (C-11: cardinalidade alta), esse join pode embaralhar `antigas`. **A medir** (revisão D4);
se `ponte_primaria` couber em broadcast, acrescentar `/*+ BROADCAST(b) */`; senão, fica como está.

**Corte C — D-17b antes da expansão.** `mei_na_baixa` (114–142) não muda. O join com ele sai do fim (355–359) e vai
para `novas_com_anterior`. No `SELECT` final, `WHEN mb.cnpj_raiz IS NOT NULL THEN 'MEI'` vira `WHEN f.eh_mei_na_baixa
AND f.teve_abertura_mes AND f.teve_baixa_mes THEN 'MEI'` (idem `opcao_pelo_mei`) — as mesmas condições do `ON` de hoje.

**Comentários.** Os 5 `ALTER COLUMN ... COMMENT` (58–69) saem: o `CREATE TABLE IF NOT EXISTS` já tem os mesmos textos e a
tabela real já recebeu esses comentários na execução de 2026-11-14 (5 commits Delta a menos por execução).

**Escrita (U3, em aberto).** O shuffle do *optimized write* vem da conf global do cluster (`gerar_job.py:87`). Se a
medição mostrar que ele pesa, a mitigação é a propriedade de tabela `ALTER TABLE ... SET TBLPROPERTIES
('delta.autoOptimize.optimizeWrite' = 'false')` + `OPTIMIZE` simples ao fim do bloco — **não** `SET` no `.sql`. Antes,
confirmar no cluster se a conf de sessão tem precedência sobre a propriedade de tabela (revisão D8); se tiver, a única
saída é tirar a conf global do cluster e ligar o optimized write por tabela onde ele ajuda.

**Painel incremental (U5, decidida: sim).** `fato_indicadores_setoriais_mes` troca `CREATE OR REPLACE TABLE AS` por
`CREATE TABLE IF NOT EXISTS` (mesmo schema, `PARTITIONED BY (mes_referencia)`) + `INSERT INTO ... REPLACE WHERE
mes_referencia BETWEEN` a janela, lendo só esses meses do fato e do macro. Cada mês do painel depende só do mesmo mês
do fato. Consequência aceita: uma revisão de Selic/IPCA de mês antigo só entra ao reprocessar aquele mês. Migração e
carga completa: §3.6.

### 3.4 `dim_empresa` — joins por intervalo só com chaves

Continua rebuild total (D-08). Duas mudanças, escolhidas por serem fáceis de explicar e por não mudarem o modo de falha.

**(a) Matriz canônica pelo caminho estreito.** O ANTI JOIN só pode descartar algo em CNPJ com **mais de uma ordem**
marcada como matriz (B19 — tamanho **a medir**):

```sql
mat_chaves AS (   -- só chaves
    SELECT pk, cnpj_basico, cnpj_ordem, valid_since, valid_until
    FROM tcc_receita.silver_intermediate.estabelecimentos WHERE identificador_matriz_filial = '1'
),
multi AS (SELECT cnpj_basico FROM mat_chaves GROUP BY cnpj_basico HAVING COUNT(DISTINCT cnpj_ordem) > 1),
descartadas AS (  -- mesma condição de hoje (177–182), só nos CNPJs com mais de uma ordem
    SELECT e.pk, e.valid_since
    FROM mat_chaves e
    LEFT SEMI JOIN multi m ON m.cnpj_basico = e.cnpj_basico
    LEFT SEMI JOIN mat_chaves melhor
        ON melhor.cnpj_basico = e.cnpj_basico AND melhor.cnpj_ordem < e.cnpj_ordem
       AND melhor.valid_since < COALESCE(e.valid_until, '9999-12')
       AND COALESCE(melhor.valid_until, '9999-12') > e.valid_since
),
est_matriz AS (   -- colunas e expressões de hoje (137–171) + pk; sem dica de broadcast (tamanho de
                  -- `descartadas` não medido; o AQE escolhe)
    SELECT e.pk, <mesmas colunas de hoje>
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    LEFT ANTI JOIN descartadas d ON d.pk = e.pk AND d.valid_since = e.valid_since
    WHERE <mesmos filtros de hoje, 183–186>
)
```

Igual a hoje: em CNPJ com uma ordem só, `melhor.cnpj_ordem < e.cnpj_ordem` nunca vale. `est_matriz` hoje **não projeta
`pk`** (137–171) e passa a projetar.

**(b) Joins por intervalo só com as chaves; colunas largas depois, por igualdade.** `fronteiras` e `fatias` ficam
**como estão**. Os três joins por intervalo de `unificado_raw` (256–268) passam a usar só as chaves de cada fonte; as
colunas largas entram depois, por join de igualdade em `(pk, valid_since)`:

```sql
est_chaves AS (SELECT cnpj_basico, pk, valid_since, valid_until FROM est_matriz),
emp_chaves AS (SELECT cnpj_basico, pk, valid_since, valid_until FROM emp_limpa),
sim_chaves AS (SELECT cnpj_basico, pk, valid_since, valid_until FROM sim_limpa),
fatias_com_chaves AS (   -- mesmas condições de intervalo de hoje; só colunas estreitas
    SELECT f.cnpj_basico, f.inicio_fatia, f.fim_fatia,
           est.pk AS pk_est, est.valid_since AS vs_est,
           emp.pk AS pk_emp, emp.valid_since AS vs_emp,
           sim.pk AS pk_sim, sim.valid_since AS vs_sim
    FROM fatias f
    INNER JOIN est_chaves est
        ON f.cnpj_basico = est.cnpj_basico AND est.valid_since <= f.inicio_fatia AND f.inicio_fatia < est.valid_until
    LEFT JOIN emp_chaves emp
        ON f.cnpj_basico = emp.cnpj_basico AND emp.valid_since <= f.inicio_fatia AND f.inicio_fatia < emp.valid_until
    LEFT JOIN sim_chaves sim
        ON f.cnpj_basico = sim.cnpj_basico AND sim.valid_since <= f.inicio_fatia AND f.inicio_fatia < sim.valid_until
),
unificado_raw AS (       -- atributos largos por igualdade (chave quase única); mesmas colunas de hoje (241–255)
    SELECT est.*, k.inicio_fatia AS valid_since_final, k.fim_fatia AS valid_until_final,
           (emp.pk IS NOT NULL) AS possui_cadastro_empresa, emp.razao_social, emp.natureza_juridica, ...,
           sim.opcao_pelo_mei_bool, sim.opcao_pelo_simples_bool, <nk_grupo_cnae_calc como hoje>
    FROM fatias_com_chaves k
    JOIN est_matriz est ON est.pk = k.pk_est AND est.valid_since = k.vs_est
    LEFT JOIN emp_limpa emp ON emp.pk = k.pk_emp AND emp.valid_since = k.vs_emp
    LEFT JOIN sim_limpa sim ON sim.pk = k.pk_sim AND sim.valid_since = k.vs_sim
)
-- dim_enriquecida, ghost_row e o INSERT OVERWRITE: sem mudança (est.* agora inclui pk; não é projetada na saída).
```

Por que é igual e por que não precisa de asserção nova: as condições de intervalo são as de hoje, então cada fatia casa
as mesmas versões; se uma fonte tiver versões sobrepostas, a fatia **continua duplicando** e o DQ de colisão de SK
continua acusando (D-09). O join de igualdade casa uma linha por chave, porque `(pk, valid_since)` é único em cada SCD2.

**O que se perde em relação ao desenho anterior ("ponteiros preenchidos para frente"), rejeitado por simplicidade:**
os três joins por intervalo continuam existindo (agora estreitos) e há três joins de igualdade a mais, ou seja, mais
etapas de shuffle no total — todas estreitas, salvo uma por fonte para trazer as colunas largas. O desenho de ponteiros
eliminava os joins por intervalo (uma janela só) e teria um pouco menos de shuffle, ao custo de `LAST(..., TRUE)`,
structs e uma asserção extra para não perder a detecção de sobreposição. Para o spill, que vem das linhas largas, os dois
devem dar efeito parecido; **olhar o `EXPLAIN` da dim atual antes de implementar** (§5.4): se o spill vier do *buffer*
do sort-merge e não da largura, nenhum dos dois resolve e a tarefa é revista.

**Opcional (U4, em aberto):** coluna `inicio_vigencia_empresa` (`MIN(valid_since_final) OVER (PARTITION BY
cnpj_basico)`) no fim da dimensão, para os três leitores que refazem o `GROUP BY`/`ROW_NUMBER` na dim inteira
(`fato_empresa_mes.primeira_aparicao`, `fato_mudanca_estado.primeira_versao_empresa`, DQ 2 da gold).

### 3.5 Dashboard — tabelas gold no formato de cada gráfico

**Princípio: o dataset do Lakeview passa a ler uma tabela já pronta, gravada pelo Job; o SQL do dataset vira um
`SELECT` simples.** Todas as tabelas novas ficam em `src/gold/dashboard_agregados.sql` (um `CREATE`/`INSERT` por
tabela), registrado em `SQL_INGEST` do `gerar_job.py` depois de `fato_empresa_mes`, `obt_dashboard_eventos` e
`dashboard_mercado_ativo` (o gerador não detecta `.sql` novo — C-24). Nenhuma tem CNPJ, razão social ou endereço além de
município/bairro (LGPD); `dash_mercado_empresas` mantém o filtro `exibir_razao_social`. Tabelas com `CLUSTER BY`
recebem só `OPTIMIZE` simples.

| Tabela nova (gold) | Conteúdo / grão | Carga | Dataset passa a ser | Widgets |
|---|---|---|---|---|
| `dash_fluxos_mes` | **o SQL atual de `fluxos_empresa`** (195–215) materializado: uma linha por empresa × fluxo no mês, colunas `mes_referencia, data_referencia, uf, porte_empresa, grande_area_cnae, fluxo, qtd, pct_mei_abertura, pct_mei_baixa, pct_falencia_baixa, idade_baixa_anos, pct_baixa_menos_2_anos`; `PARTITIONED BY (mes_referencia)` | `REPLACE WHERE` da janela | `SELECT * FROM gold.dash_fluxos_mes WHERE <SEM_CARGA_INICIAL>` | P2 e P4 (contadores e `b2_porte`) |
| `dash_eventos_mes` | `mes_referencia, tipo_evento, uf, porte_empresa, grande_area_cnae, qtd_eventos` (da OBT) | `REPLACE WHERE` | `SELECT tipo_evento, uf, porte_empresa, grande_area_cnae, SUM(qtd_eventos) AS qtd_eventos FROM ... GROUP BY ALL` | `b4_tipo` |
| `dash_baixas_idade_mes` | `mes_referencia, uf, porte, área, faixa_idade_baixa, qtd_baixas` (do fato) | `REPLACE WHERE` | idem, somando os meses | `b4_idade` |
| `dash_baixas_motivo_mes` | `mes_referencia, uf, porte, área, categoria_macro_evento, qtd_empresas` (fato × OBT, mesmo SQL de hoje com filtro de janela) | `REPLACE WHERE` | idem | `b4_motivo` |
| `dash_mercado` | SQL atual de `mercado` (uf, municipio, bairro normalizado, porte, área, nome_cnae, contagem); `CLUSTER BY (uf, municipio)` | `CREATE OR REPLACE` (snapshot) | `SELECT * FROM ...` | `c5_ativas`, `b5_porte`, `b5_area` |
| `dash_mercado_bairros` | SQL atual de `mercado_bairros` (top 20 bairros por município × CNAE) | idem | `SELECT * FROM ...` | `b5_bairro` |
| `dash_mercado_empresas` | SQL atual de `mercado_empresas` (1.000 maiores, razão liberada) | idem | `SELECT * FROM ...` | `t5_empresas` |

Por que os números ficam iguais: `dash_fluxos_mes` e as três `dash_mercado*` são **exatamente** o SQL atual do dataset
gravado em tabela — mesmas linhas, logo mesmos `AVG`, mediana e somas para qualquer filtro. As outras três somam os
mesmos fatos num grão sem `data_referencia` (que nenhum widget usa), e soma de soma é a mesma soma.

**Onde ficou o `EXPLODE`:** em lugar nenhum. A versão anterior desta spec reagregava `fluxos_empresa` e reconstruía as
linhas com `EXPLODE`; foi trocada (revisão R27, decisão do gerente) pela materialização direta, mais fácil de explicar.
O preço: `dash_fluxos_mes` tem uma linha por empresa × fluxo (~500 mil/mês, ~20 mi em 40 meses, colunas estreitas), e o
dataset continua "grande" para o Lakeview (cada clique é uma consulta no warehouse) — mas a consulta agora lê uma tabela
estreita em vez de varrer o fato. **Plano B**, só se a medição do dashboard mostrar lentidão: agregar por
`(uf, porte, área, fluxo, teve_falencia_mes, idade_na_baixa_meses)` com contagem, usar *custom calculations* do dataset
(`SUM(num)/SUM(den)`, [docs](https://docs.databricks.com/aws/en/dashboards/manage/data-modeling/custom-calculations)) para
os percentuais, e `EXPLODE` **só no dataset da mediana**, porque mediana ponderada não existe no Lakeview — é o único
lugar sem alternativa simples.

**`mercado` (U6, decidida: manter o cruzamento).** Continua com ~6,7 mi linhas: o grão é o próprio cruzamento dos quatro
filtros com porte e área. Não cresce com os meses; a materialização tira o `GROUP BY` de 23 mi de linhas de cada clique.

### 3.6 Migração e carga completa (bloqueante, revisão R2 e R20)

Toda tabela que passa a ser gravada por `REPLACE WHERE` da janela **só recebe os meses da execução**. Se a migração
fizer só `DROP`/`CREATE`, o painel e as `dash_*_mes` ficariam com os meses do primeiro bloco e o dashboard mostraria a
série cortada, sem erro (o DQ de hoje só olha a janela). Por isso:

1. **Script de migração** `src/jobs/migracoes/<data>_fase3.sql` (no molde de `2026-11-14_semantica_eventos.sql`):
   `DROP TABLE IF EXISTS` do log (U1), do painel e das `dash_*` (as que já existirem).
2. **Carga completa logo em seguida, numa execução só, com a janela inteira:** o log é reconstruído em
   `[2023-05, último mês das SCD2]`; o painel e as `dash_*_mes`, em `[MIN, MAX]` de `mes_referencia` de
   `fato_empresa_mes`. Na prática: o Job (ou `run_sql.py`) com `mes_referencia_inicial` = primeiro mês e
   `mes_referencia_final` = último mês, sobre as tarefas `si_log_eventos_negocio`, `gold_fato_indicadores_setoriais_mes`
   e `gold_dashboard_agregados` e seus DQ. Com o desenho novo do log, a janela inteira custa o mesmo que uma carga
   inicial; o `fato_mudanca_estado` e a OBT não precisam ser refeitos (os valores não mudam; só o log ganhou coluna).
3. **DQ de cobertura de meses, independente da janela** (novo bloco em `dq_gold.sql`, e no DQ do log): os meses
   distintos de cada tabela de série têm de bater com os da fonte.

```sql
-- painel × fato: mesmo conjunto de meses (nos dois sentidos)
WITH f AS (SELECT DISTINCT mes_referencia FROM tcc_receita.gold.fato_empresa_mes),
     s AS (SELECT DISTINCT mes_referencia FROM tcc_receita.gold.fato_indicadores_setoriais_mes)
SELECT ASSERT_TRUE(
    (SELECT COUNT(*) FROM (SELECT * FROM f EXCEPT SELECT * FROM s)) = 0
    AND (SELECT COUNT(*) FROM (SELECT * FROM s EXCEPT SELECT * FROM f)) = 0,
    CONCAT('ERRO DQ [fato_indicadores_setoriais_mes]: meses do painel diferem dos do fato (',
           CAST((SELECT COUNT(*) FROM s) AS STRING), ' x ', CAST((SELECT COUNT(*) FROM f) AS STRING), ').'));
-- dash_fluxos_mes, dash_baixas_*_mes: meses = meses do fato depois do baseline (no baseline não há fluxo);
-- dash_eventos_mes: meses = meses da OBT;
-- log (dq_log_eventos_negocio.sql): COUNT_IF(cnpj_basico IS NULL) = 0 em TODOS os meses, não só na janela.
```

O custo é ler a coluna de partição dessas tabelas (barato). Sem esses DQ, uma migração incompleta passaria verde.

---

## 4. Invariantes (o que não pode mudar) e como cada uma se mantém

| # | Invariante | Onde é verificada | Como se mantém |
|---|---|---|---|
| I1 | Identidade de estoque: `ativa_fim = ativa_inicio + entrou − saiu`, linha a linha (fora do baseline) | DQ gold 2 (1) | flags pelas mesmas expressões (`estado_mes` → `fluxos_mes` intactos); só muda de onde vêm `situacao_inicio_mes` e o porte anterior (Corte B) |
| I2 | Continuidade: `estoque_inicial(M) = estoque_final(M−1)` | DQ gold 2 (2) | `ativa_inicio_mes` vem da **dim** (versão que cobre M−1), nunca do fato do bloco anterior |
| I3 | Aberturas = primeiras aparições na dim (histórico inteiro para trás) | DQ gold 2 | `primeira_aparicao` continua sobre a dim inteira |
| I4 | D-17: primeira aparição pós-baseline já BAIXADA/INAPTA/SUSPENSA = abertura **e** evento | DQ gold 2 | a versão não ATIVA nova gera a sua linha no mês de início (Corte A) |
| I5 | D-17b: porte MEI pelas datas do Simples, sem olhar o futuro | DQ gold 2c | `mei_na_baixa` intacto; o join sobe para o nível de versão (Corte C) |
| I6 | B23: porte/MEI/Simples de M−1 na saída do estoque | comparação | versão anterior contígua (Corte B) |
| I7 | Baseline sem fluxo; sem eventos no log | DQ gold 2 (5); DQ log | baseline = `MIN` da própria tabela; versões do baseline não entram no `LAG` do log |
| I8 | Dois relógios (D-13) | DQ log | o log continua usando `valid_since` como mês e as mesmas datas |
| I9 | Unicidade `(cnpj_raiz, mes)` no fato; SK sem colisão na dim | DQ gold 2 (3); `dq_dim_empresa` | Corte A: no máximo uma linha por versão não ATIVA; joins por intervalo da dim iguais aos de hoje (sobreposição continua duplicando e sendo acusada) |
| I10 | Fato × OBT: baixas de matriz fecham (≤ 0,5%/mês) | DQ gold 2b | nem fato nem OBT mudam |
| I11 | Painel = somas do fato | DQ gold 3 | o painel incremental lê os mesmos meses que substitui |
| I12 | **Séries completas**: painel, `dash_*_mes` e log cobrem todos os meses da fonte | **novo**, §3.6 | carga completa na migração + DQ de cobertura |
| I13 | Cada widget mostra o mesmo número para qualquer filtro | §5.3 | mesmo SQL materializado, ou soma no mesmo grão |
| I14 | Contiguidade e uma versão corrente por `pk` nas SCD2 | DQ da F2 (T0) | pré-requisito da Ideia 1 do log |

**Colunas que olham para trás ou para frente no tempo:**

- `ativa_inicio_mes`, `situacao_inicio_mes`, `teve_reativacao_mes`, porte de saída (B23): **um mês para trás**, sempre na
  dim (rebuild total). Um bloco novo não depende das linhas do fato de blocos anteriores.
- `eh_primeira_aparicao`, `EMPRESA NASCEU`, `ENTROU NO MEI/SIMPLES/QSA` de primeira versão: **todo o histórico para
  trás**. Estáveis enquanto a silver anterior a M não mudar.
- `idade_na_baixa_meses`, `idade_meses`: datas da própria versão.
- `mei_na_baixa`: para trás (Simples até o mês da aparição).
- `data_ultimo_mes_dim`: único "olhar para frente" — limita a expansão ao último snapshot.
- `fim_vigencia` muda em blocos futuros, mas o fato não o grava; `sk_empresa` = hash(cnpj|início) não muda quando a
  versão é fechada depois.

**Reprocessar um mês do meio (M):**

- Só a gold, com a silver igual: M é função pura da dim e do Simples até M → sai idêntico; outros meses intactos. O
  mesmo vale para log, painel e `dash_*_mes`.
- A silver de M mudou: mudam as versões que começam em M e, com elas, a "versão anterior" de M+1, a primeira aparição e
  os `sk_empresa` dos meses seguintes. **Regra: reprocessar log, dim, fato, painel e `dash_*_mes` de M até o último
  mês** — não só M. Isso já é verdade hoje; a fase 4 (blocos) tem de obedecer.

---

## 5. Plano de validação

### 5.1 Ambiente e preservação

- Tudo na amostra de 1% (`tcc_receita.amostra_*`), com `scripts/amostra/rodar_no_warehouse.py --arquivos ...`. A base
  real não é tocada até o §5.5.
- Antes de cada comparação: `CREATE OR REPLACE TABLE tcc_receita.amostra_<camada>.<tabela>__antes DEEP CLONE
  tcc_receita.amostra_<camada>.<tabela>`. Rodar o SQL novo (sobrescreve só a tabela da amostra) e comparar com
  `__antes`. Ao final, apagar os `__antes`.
- Os dois lados usam **as mesmas SCD2 de entrada**. Se a F2 trocar as SCD2 da amostra no meio, refazer o `__antes`.
- Limite conhecido: a amostra roda no warehouse, com ANSI ligado; o cluster roda com ANSI desligado. A igualdade na
  amostra não prova o comportamento do cluster com dado sujo (que a amostra não tem). Nenhuma expressão nova depende
  de ANSI.

### 5.2 Equivalência linha a linha (tabelas)

`EXCEPT ALL` nos dois sentidos, fora as colunas de carimbo de tempo; esperado **0 e 0**:

```sql
WITH novo  AS (SELECT * EXCEPT (gerado_em, cnpj_basico) FROM tcc_receita.amostra_silver_intermediate.log_eventos_negocio),
     antes AS (SELECT * EXCEPT (gerado_em) FROM tcc_receita.amostra_silver_intermediate.log_eventos_negocio__antes)
SELECT (SELECT COUNT(*) FROM (SELECT * FROM novo EXCEPT ALL SELECT * FROM antes)) AS so_no_novo,
       (SELECT COUNT(*) FROM (SELECT * FROM antes EXCEPT ALL SELECT * FROM novo)) AS so_no_antes
```

Mesmo molde para `fato_mudanca_estado`, `dim_empresa`, `fato_empresa_mes`, `fato_indicadores_setoriais_mes` e
`obt_dashboard_eventos` (esta só muda se a entrada mudar — teste de regressão da cadeia).

Testes específicos:

1. **Log, janelas:** (a) 2023-05..08 inteira (inclui o baseline); (b) só 2023-07 contra o recorte de julho do
   `__antes`; (c) dois blocos (05..06, depois 07..08) contra uma execução única.
2. **Fato, blocos:** idem (a)/(b)/(c); (b) é o "reprocessar um mês do meio": julho igual e junho/agosto intactos
   (comparar a tabela inteira).
3. **Dim:** todas as colunas + `nk_empresa` único + mesmo número de fatias por CNPJ.
4. **`cnpj_basico` no log (U1):** igual ao `cnpj_basico` da SCD2 de origem para toda `pk` (0 divergências); nenhuma `pk`
   com dois `cnpj_basico` (se houver, hoje o fato duplica a linha: registrar e decidir antes de seguir). **E
   reprocessamento de mês antigo depois da migração:** rodar o `fato_mudanca_estado` só em 2023-06 e comparar com o
   `__antes` (prova que os meses fora da última janela têm `cnpj_basico`).
5. **Migração (§3.6), a partir das tabelas existentes:** na amostra, executar o script de migração e a carga completa,
   e comparar painel e `dash_*_mes` com a versão reconstruída do zero; depois rodar só 2023-08 e conferir que os outros
   meses continuam lá. Também provar que o DQ de cobertura **falha** quando a carga completa é pulada (rodar a migração
   seguida de uma janela de um mês só, numa cópia).
6. **Painel por blocos:** 05..06 e depois 07..08 = 05..08.
7. **DQ:** `--etapas si,sf,gold` completo passando, com os DQ novos (cobertura de meses, `cnpj_basico` não nulo).

### 5.3 Equivalência do dashboard (widget a widget)

Script `scripts/amostra/comparar_widgets.py` (ou extensão do `testar_datasets.py` de 2026-11-15): para cada widget das
páginas 2, 4 e 5, a consulta que o Lakeview faria — `SELECT <agregação> FROM (<SQL do dataset>) WHERE <filtros> GROUP BY
<categoria>` — com o SQL **antigo** e o **novo**; igualdade exata em contagens e somas, tolerância 1e-9 em `AVG` e
`PERCENTILE`. Cenários: P2/P4 — nenhum filtro; `uf = 'SP'`; `uf IN ('RJ','MG')`; `porte_empresa = 'MEI'` (P4);
`grande_area_cnae = 'COMERCIO'`; UF + porte + área. P5 — nenhum; UF; UF + maior município; + maior bairro; + CNAE mais
frequente; só CNAE. Para cada filtro, também a lista de valores do dropdown antigo × novo. Depois, prévia com
`scripts/amostra/publicar_dashboard_amostra.py --id <prévia>` e conferência visual do autor (o oficial não é tocado).

### 5.4 Diagnóstico pendente (quando o CLI reautenticar)

**No warehouse 2X-Small, em lote** (complementa a §7 da revisão):

```sql
-- contiguidade e versão corrente das SCD2 (I14; é a T0 da F2): esperado 0 e 0
SELECT COUNT(*) FROM (SELECT valid_until, LEAD(valid_since) OVER (PARTITION BY pk ORDER BY valid_since) AS prox
                      FROM tcc_receita.silver_intermediate.<scd2>) WHERE valid_until IS DISTINCT FROM prox;
SELECT COUNT(*) FROM (SELECT pk FROM tcc_receita.silver_intermediate.<scd2> GROUP BY pk HAVING COUNT_IF(is_current) <> 1);
-- versões por valid_since (ganho da Ideia 1)
SELECT valid_since, COUNT(*), COUNT_IF(valid_until IS NULL) FROM tcc_receita.silver_intermediate.<scd2> GROUP BY 1;
-- CNPJs com mais de uma ordem marcada matriz (tamanho de `multi`/`descartadas`, §3.4a)
SELECT COUNT(*) FROM (SELECT cnpj_basico FROM tcc_receita.silver_intermediate.estabelecimentos
                      WHERE identificador_matriz_filial = '1' GROUP BY 1 HAVING COUNT(DISTINCT cnpj_ordem) > 1);
-- versões correntes não ATIVA (o que o Corte A deixa de expandir)
SELECT inicio_vigencia, COUNT(*), COUNT_IF(fim_vigencia IS NULL AND situacao_cadastral_matriz <> 'ATIVA')
FROM tcc_receita.silver_final.dim_empresa GROUP BY 1;
-- grupos de CNAE (dá para difundir ponte_primaria no fato?)
SELECT COUNT(*) FROM tcc_receita.silver_final.dim_grupo_cnae;
-- linhas por mês de dash_fluxos_mes (= aberturas + baixas do mês)
SELECT mes_referencia, COUNT_IF(teve_abertura_mes) + COUNT_IF(teve_baixa_mes)
FROM tcc_receita.gold.fato_empresa_mes GROUP BY 1;
-- bairros distintos no filtro da P5 (limite de 100 mil do dropdown, U7)
SELECT COUNT(DISTINCT COALESCE(NULLIF(TRIM(bairro), ''), 'NAO INFORMADO')) FROM tcc_receita.gold.dashboard_mercado_ativo;
```

**No cluster do Job, sobre as tabelas reais** (não no warehouse: o warehouse usa Photon e um Spark mais novo, e a bronze
da amostra é criada por CTAS sem partição, `criar_amostra_bronze.sql:12-25`, então o plano não seria o do Job). Um
notebook único, na mesma subida de cluster da medição do §5.5: `EXPLAIN FORMATTED` do log atual e do novo (contar
`Scan` e `Exchange` por SCD2), da `dim_empresa` atual (quantas vezes `est_matriz` passa por shuffle largo; de onde vem o
spill) e do fato novo (se `antigas` passa por `Exchange` no join de CNAE); e o teste de precedência do optimized write
(revisão D8).

### 5.5 O que medir na base real

A **mesma execução descrita na F2** (revisão R17), numa subida de cluster: (1) carga inicial `2023-05..08` de uma vez —
comparável ao B20; (2) em seguida, **um mês incremental** (reexecutar `2023-08` ou rodar `2023-09` se a bronze já tiver),
para ter o custo por mês que a fase 4 precisa. Por tarefa (event log / Spark UI; duração também por
`system.lakeflow.job_task_run_timeline`): duração, bytes lidos, shuffle, spill, nº de stages — **incluindo as tarefas
de DQ**, que também crescem com o histórico e não estão na linha de base.

| Tarefa | Linha de base | Alvo |
|---|---|---|
| `si_log_eventos_negocio` | 873 s | ≤ 241 s na carga inicial; mês incremental bem abaixo disso |
| `sf_fato_mudanca_estado` | 1,7× o anterior | ≤ tempo anterior à nova semântica |
| `sf_dim_empresa` | spill 7,6 GB | spill 0 |
| `gold_fato_empresa_mes` | 84 GB shuffle | shuffle ≤ ~2× o tamanho da saída escrita |
| `gold_fato_indicadores_setoriais_mes` | (medir) | proporcional aos meses do bloco, não ao histórico |
| tarefas de DQ | (medir) | registrar; não pode dominar o tempo |

Dashboard: tempo das consultas dos datasets das páginas 2, 4 e 5, com e sem filtro, cache frio, no histórico de
consultas do warehouse. Sugestão para a guarda-chuva (não editada aqui): o critério "dashboard abre sem corte de linhas
com 12 meses" deve virar "tempo de resposta", já que o Lakeview não corta datasets (§2.6).

---

## 6. Ganho esperado e riscos

| Mudança | Ganho | Por quê | Risco |
|---|---|---|---|
| Log, Ideia 1 | **alto** (o maior da fase) | shuffle passa de "histórico × nº de ramos" para "versões que mudaram no bloco × nº de ramos"; na carga inicial também, porque as versões do baseline que nunca mudaram saem | depende da contiguidade → DQ da F2 (T0) |
| Log, Ideia 2 | baixo depois da Ideia 1 | tira leituras repetidas de um conjunto já pequeno | só se a medição pedir |
| Log, Ideia 3 | médio | self-join só dos CNPJs com matriz mudando na janela | candidatos podem ser milhões num bloco de 12 meses (shuffle estreito) |
| U1 (`cnpj_basico` no log) | médio no `fato_mudanca_estado` | some a releitura e a dedup das quatro SCD2 | exige recriar o log em todos os meses (§3.6) |
| Fato, Cortes A+B+C | **alto** | some a expansão das mortas (~120 mi de linhas na carga inicial), o join depois da expansão e o `LAG` sobre ~56 mi de linhas largas | mais código para revisar (dois caminhos); coberto pelo `EXCEPT` e pelos DQ |
| Fato, comentários | pequeno | 5 commits Delta a menos por execução | nenhum |
| Painel incremental | médio agora, **alto** em 40 meses | lê só os meses do bloco | série cortada se a migração não fizer carga completa → §3.6 |
| Optimized write (U3) | médio, condicional | tira um shuffle do tamanho da saída | arquivos pequenos; precedência da conf a medir |
| Dim, (a)+(b) | médio; deve zerar o spill | linhas largas só no join de igualdade | se o spill vier do sort-merge e não da largura, não resolve → `EXPLAIN` antes |
| Dashboard, `dash_*` | **alto** no tempo de resposta | cada clique lê tabela pronta | `dash_fluxos_mes` continua "grande" no Lakeview (consulta no warehouse, barata); bairro pode passar de 100 mil valores |

Riscos transversais: (1) a F2 pode mudar a ordem física das colunas SCD2 — os esqueletos usam nomes, não posição; (2) a
amostra não pega incompatibilidade com o 15.4 → `scripts/checar_sql_cluster.py` antes da execução real; (3) desempenho
só se prova na base real; (4) a reextração de `empresas` com a máscara da D-16 (fase 4) muda o hash de todas as linhas
de uma vez e abre uma onda de versões; ela tem de vir **antes** da carga da janela completa (revisão R22).

---

## 7. Tarefas (pequenas, independentes, com aceite)

**F2** = depende da fase 2 mesclada (para a rodada final de comparação; o código não depende). Cada tarefa: commit
atômico próprio e linha no backlog.

**Pré-requisito**

- [ ] **T-P0 — DQ de contiguidade e de versão corrente nas SCD2** — é a **T0 da F2**; a F3 só depende dela (não há
  T-L1 própria).

**Log (silver_intermediate)**

- [x] **T-L2 — Ideia 1** (filtro antes do `LAG` nas quatro SCD2; `baseline_emp` em `params`). *Aceite:* `EXCEPT ALL` 0/0
  nos cenários (a)(b)(c) do §5.2; no `EXPLAIN` do cluster, **uma leitura com janela (`LAG`) por SCD2 com o filtro da
  janela**, além das leituras de uma coluna (`MIN`) e do `hist_matriz`. Depende de T-P0.
- [ ] **T-L2b — Ideia 2**, só se a medição de T-L2 mostrar os ramos repetidos entre os passos caros. *Aceite:* `EXCEPT
  ALL` 0/0.
- [x] **T-L3 — Ideia 3 (nascimento por candidatos).** *Aceite:* `EXCEPT ALL` 0/0 em `EMPRESA NASCEU` e no log inteiro.
- [x] **T-L4 — `cnpj_basico` no log (U1).** DDL com a coluna no fim; script de migração com `DROP` (§3.6); DQ
  `cnpj_basico` não nulo em todos os meses. *Aceite:* teste 4 do §5.2.
- [x] **T-L5 — `fato_mudanca_estado` com `cnpj_basico` do log e dim restrita aos CNPJs da janela (§3.2).** *Aceite:*
  `EXCEPT ALL` 0/0; `dq_fato_mudanca_estado` passa. Depende de T-L4.

**Dimensão (silver_final)**

- [ ] **T-D0 — `EXPLAIN` da dim atual no cluster** (de onde vem o spill). Se não vier da largura das linhas, rever
  T-D1/T-D2 antes de implementar.
- [x] **T-D1 — Matriz canônica estreita (§3.4a).** *Aceite:* `EXCEPT ALL` 0/0 na `dim_empresa`.
- [x] **T-D2 — Joins por intervalo só com chaves (§3.4b).** *Aceite:* `EXCEPT ALL` 0/0; `dq_dim_empresa` passa.
- [ ] **T-D3 — (se U4) `inicio_vigencia_empresa`** e uso nos três leitores. *Aceite:* `EXCEPT ALL` 0/0 nas leitoras;
  coluna = `MIN(inicio_vigencia)` por CNPJ.

**Fato e painel (gold)**

- [x] **T-F1 — Cortes A+B+C em `fato_empresa_mes`.** *Aceite:* `EXCEPT ALL` 0/0 nos cenários (a)(b)(c); DQ gold passa
  (identidade, continuidade, D-17, D-17b, 2b).
- [x] **T-F1b — Remover os 5 `ALTER COLUMN ... COMMENT`** (58–69). *Aceite:* `DESCRIBE TABLE` com os mesmos comentários.
- [x] **T-F2 — Painel incremental (U5).** `CREATE TABLE IF NOT EXISTS` + `REPLACE WHERE`; migração e carga completa
  (§3.6); DQ de cobertura de meses. *Aceite:* `EXCEPT ALL` 0/0 contra o painel reconstruído do zero; testes 5 e 6 do
  §5.2; DQ 3 e o de cobertura passam.
- [ ] **T-F3 — (se U3) optimized write por propriedade de tabela** + `OPTIMIZE` ao fim. Só depois do teste de
  precedência; só na base real. *Aceite:* shuffle da tarefa cai; nº de arquivos por partição registrado.

**Dashboard**

- [x] **T-B1 — `src/gold/dashboard_agregados.sql`** com as tabelas do §3.5, registro em `SQL_INGEST`, YAML regenerado
  (`python -m src.jobs.gerar_job`), migração/carga completa e DQ de cobertura (§3.6). *Aceite:* tabelas na amostra;
  `dash_fluxos_mes` = SQL atual de `fluxos_empresa` (`EXCEPT ALL` 0/0); somas por mês das demais = somas no fato/OBT;
  `dash_mercado*` = SQL atual (`EXCEPT ALL` 0/0).
- [x] **T-B2 — Gerador aponta para as tabelas novas** (`DATASETS`, `COLUNAS`, `validar()`), JSON regenerado. *Aceite:*
  `validar()` ok; `comparar_widgets.py` sem diferença em todos os cenários.
- [ ] **T-B3 — Prévia publicada e conferida pelo autor;** só então republicar o oficial. *Aceite:* autor aprova.
- [x] **T-B4 — DQ das `dash_*`** em `dq_gold.sql`: linhas de `dash_fluxos_mes` por mês = aberturas + baixas do fato;
  soma de `dash_mercado` = `COUNT(*)` de `dashboard_mercado_ativo`.
- [x] **T-B5 — (tarefa adicional, pedido do autor via gerente) quadro "ponte" na página 1:** por que o crescimento
  líquido ≠ aberturas − baixas, no último mês: aberturas − baixas + reativações + baixas de quem já não estava ativa −
  aberturas que não entraram como ativas − saídas que não são baixa = crescimento líquido. Dataset `ponte_mes` sobre o
  painel (reage aos filtros de UF, porte e setor da página); sinais calculados no SQL do dataset. DQ 6 em
  `dq_gold.sql`: a identidade fecha por mês no fato, e as duas igualdades que permitem montá-la a partir do painel
  (entradas sem abertura = reativações; baixas com saída = baixas de ativas). Feita **depois** da comparação widget a
  widget, em commit próprio. *Aceite:* `validar()` ok; identidade fecha na amostra; prévia publicada.

**Fechamento**

- [ ] **T-X1 — Rodada completa na amostra** (`--etapas si,sf,gold`) **com as SCD2 da F2** (F2).
- [ ] **T-X2 — Execução na base real e medição** (§5.5: `05..08` de uma vez + um mês incremental, na mesma subida de
  cluster da F2, com o notebook de `EXPLAIN`).
- [ ] **T-X3 — Docs:** specs de camada afetadas, `docs/backlog.md`, `docs/decisions.md` (U1, U5, U6 e as que forem
  decididas), `docs/tcc.md` se o texto do TCC descreve a construção do log/fato (só sugestão, nunca edição do repo do
  TCC).

Ordem: T-P0 → T-L2 → T-L3 → T-L4 → T-L5 (T-L2b só depois da medição) | em paralelo: T-F1 → T-F1b → T-F2 | T-D0 → T-D1
→ T-D2 | T-B1 → T-B2 → T-B3 → T-B4; por fim T-X1 → T-X2 → T-X3. A migração do log (T-L4), do painel (T-F2) e das
`dash_*` (T-B1) pode ser um script só, executado uma vez na base real antes da carga completa.

---

## 8. Decisões do autor

| ID | Pergunta | Estado | Recomendação / decisão |
|---|---|---|---|
| U1 | Gravar `cnpj_basico` no log? | **decidida: sim** (por omissão, via gerente, 2026-11-16) | recriar o log e recarregar todos os meses (§3.6) |
| U2 | Datasets de percentuais/mediana: `EXPLODE` ou *custom calculations*? | **retirada** | trocada pela materialização direta de `fluxos_empresa` (§3.5); `EXPLODE` + *custom calculations* ficam como plano B |
| U3 | Desligar o optimized write no fato? | em aberto | decidir **depois** da medição; se o shuffle da escrita for > ~30% da tarefa, desligar por propriedade de tabela (se a precedência permitir) |
| U4 | Coluna `inicio_vigencia_empresa` na `dim_empresa`? | em aberto | só se, depois de T-F1 e T-L5, o `GROUP BY` da dim ainda estiver entre os passos caros |
| U5 | Painel incremental? | **decidida: sim** | revisão de Selic/IPCA antiga só entra ao reprocessar o mês |
| U6 | Perder algum filtro cruzado do "Mercado ativo" por tamanho? | **decidida: manter o cruzamento bairro × CNAE** | — |
| U7 | Filtro de bairro com mais de 100 mil valores distintos (limite do dropdown) | em aberto | medir primeiro (§5.4); se passar, aceitar (com UF/município escolhidos a lista encolhe) e pôr nota na página |

Nenhuma decisão muda número do dashboard.

---

## 9. Fontes externas

- Limites do AI/BI dashboard: <https://docs.databricks.com/aws/en/dashboards/limits>
- Datasets pequenos × grandes e cache: <https://docs.databricks.com/aws/en/dashboards/caching>
- Materialização de datasets (Beta, serverless): <https://docs.databricks.com/aws/en/dashboards/materialization>
- *Custom calculations*: <https://docs.databricks.com/aws/en/dashboards/manage/data-modeling/custom-calculations>
- `REPLACE WHERE` e versões de runtime: <https://docs.databricks.com/aws/en/delta/selective-overwrite>
- Range join (tipos suportados): <https://docs.databricks.com/aws/en/optimizations/range-join>

---

## 10. Achados da revisão → o que foi feito

Revisão: [`2026-11-16-otimizacao-revisao.md`](2026-11-16-otimizacao-revisao.md). Só os achados que tocam esta spec; os
demais (R1, R4, R5, R7, R14, R18, R23–R25, R29, R30) são da F2.

| Achado | Severidade | Procede? | O que foi feito |
|---|---|---|---|
| R2 — migração do painel/`dash_*_mes` preenche só a janela; DQ não percebe | bloqueia | sim (`fato_indicadores_setoriais_mes.sql` é `CREATE OR REPLACE AS`; `dq_gold.sql` 3 filtra a janela) | §3.6: carga completa `[MIN, MAX]` do fato logo após o `DROP`; DQ de cobertura de meses independente da janela; invariante I12; testes 5 e 6 do §5.2 |
| R3 — buraco na SCD2 gera "primeira aparição" falsa no log | bloqueia (F2) | sim, para a Ideia 1 | §3.1: dependência explícita do DQ da F2 (contiguidade com `IS DISTINCT FROM` + `sem_corrente`); I14 |
| R6 — observação do `d_before` errada | observação | sim (versões da dim não se sobrepõem; só uma casa) | observação e consulta removidas |
| R8 — linhas desatualizadas do `fato_mudanca_estado` | observação | sim | §2.2 com 34–50, 88–98, 99–129, 131–147 |
| R9 — o arquivo mudou com a semântica (`8066676`) | observação | sim | §2.2: causa inclui o `DISTINCT` mais largo; "a medir" |
| R10 — `QUALIFY` é permitido | corrigir antes | sim (log linha 203 rodou no cluster) | §3 (compatibilidade) e Ideia 3 usam `QUALIFY`; lista de regras = `checar_sql_cluster.py` |
| R11 — critério "1 Scan por SCD2" contradiz o desenho | observação | sim | aceite de T-L2: uma leitura com janela por SCD2 + `MIN` + `hist_matriz` |
| R12 — `SET` de optimized write quebra o runner da amostra | corrigir antes | sim (`rodar_no_warehouse.py` manda cada instrução ao warehouse) | §3.3: propriedade de tabela, nunca `SET` no `.sql`; teste de precedência antes (D8); proibição registrada no §3 |
| R13 — `EXPLAIN` vale no cluster, sobre tabelas reais | corrigir antes | sim (bronze da amostra sem partição) | §5.4: notebook único no cluster sobre as tabelas reais, junto com a medição |
| R15 — `CAST` nas mensagens; `INSERT`/`MERGE` no texto desliga a asserção | observação | sim (`run_sql.py:94-97`) | regra no §3 para todas as asserções novas |
| R16 — ANSI ligado na amostra, desligado no cluster | observação | sim | §5.1 registra o limite |
| R17 — medição não comparável | corrigir antes | sim | §5.5 e T-X2: `05..08` de uma vez + um mês incremental, mesma execução da F2 |
| R19 — contiguidade em duas tarefas; consulta não pega fechada sem sucessora | corrigir antes | sim | T-L1 removida; F3 depende da T0 da F2; consulta do §5.4 com `IS DISTINCT FROM` |
| R20 — `cnpj_basico` por `ADD COLUMN` deixa meses antigos nulos | corrigir antes | sim (inserção posicional, `REPLACE WHERE` da janela) | §3.6: recriação do log + recarga `[2023-05, último mês]`; DQ não nulo em todos os meses; teste de mês antigo (§5.2, 4) |
| R21 — clustering por `valid_since` contra a F2 | observação | sim | sugestão removida (a decisão é da F2) |
| R22 — `ALTER COLUMN COMMENT` no fato; critério "sem corte"; reextração da D-16 | observação | sim | T-F1b remove os 5 `ALTER`; §5.5 sugere trocar o critério da guarda-chuva por tempo de resposta (não editada); §6 risco (4) sobre a reextração antes da carga completa |
| R26 — dim por "ponteiros" difícil de defender | observação | sim | §3.4b: joins por intervalo só com chaves + igualdade para as colunas largas; asserção extra dispensada (a sobreposição continua duplicando e o DQ de SK acusa); perda descrita; `pk` acrescentada a `est_matriz`; T-D0 (`EXPLAIN` antes) |
| R27 — `EXPLODE` no dataset de fluxos | observação | sim | §3.5: `fluxos_empresa` materializado como está (`dash_fluxos_mes`); `EXPLODE` só no plano B, para a mediana |
| R28 — Ideia 2 condicional; broadcast de `descartadas`; `antigas` "sem shuffle" | observação | sim, nos três pontos | Ideia 2 virou T-L2b condicionada; dica de broadcast de `descartadas` retirada (AQE decide); §3.3 corrige: `antigas` ainda passa pelo join de CNAE, tamanho de `dim_grupo_cnae` a medir |
| R31 — testes de migração, de U1 em mês antigo e tempo dos DQ | observação | sim | §5.2 testes 4–5; §5.5 mede as tarefas de DQ |

Nenhum achado que toca esta spec foi rejeitado.

---

## 11. Notas de execução

- 2026-11-16: spec escrita a partir do código; consultas da §5.4 não rodaram (CLI sem autenticação).
- 2026-11-16: revisão crítica incorporada (§10); simplificações do gerente aplicadas (dim por chaves estreitas,
  `fluxos_empresa` materializado sem `EXPLODE`, U1/U5/U6 decididas). Nenhum código de produção alterado.
- 2026-11-16: implementação (agente da fase 3, em paralelo à F2). Resultados na amostra no §12.

---

## 12. Implementação: resultados na amostra, desvios e pendências

### 12.1 Ambiente

- Entrada: as SCD2 atuais de `tcc_receita.amostra_silver_intermediate` (não alteradas; contiguidade conferida: 0
  buracos nas quatro, nenhuma `pk` com dois `cnpj_basico`). A F2 trabalha em `amostra_f2`; T-X1 refaz a rodada com as
  SCD2 dela.
- Referência: `tcc_receita.amostra_f3_ref` = `DEEP CLONE` das tabelas da amostra **geradas pelo código anterior**
  (log, `dim_empresa`, `fato_mudanca_estado`, fato, painel, OBT, mercado ativo, macro) + `ds_<dataset>` = resultado
  de cada dataset antigo do dashboard. Conferido antes: o código anterior, rodado de novo em `amostra_f3`, reproduz a
  referência com `EXCEPT ALL` 0/0 em todas as tabelas.
- Saída do código novo: `tcc_receita.amostra_f3`, pelo runner novo `scripts/amostra/rodar_em_schema.py` (lê bronze e
  SCD2 da amostra e grava log/silver_final/gold no schema de teste). Motivo: sobrescrever as tabelas `amostra_*` foi
  bloqueado pelo controle de permissões do ambiente; o schema separado também evita disputa com a F2.

### 12.2 Resultados (`EXCEPT ALL` nos dois sentidos, fora carimbos de tempo)

| Tarefa | Teste | Resultado |
|---|---|---|
| T-L2 | log: (c) 05..06 + 07..08, (b) só 07, (a) 05..08 | 0/0 em 57.976 linhas, nos três; DQ do log passa |
| T-L3 | idem (nascimento por candidatos) | 0/0 nos três |
| T-L4 | idem, sem a coluna nova; `cnpj_basico` × SCD2 de origem | 0/0; 0 nulos e 0 divergências nos 57.976 eventos (4 entidades) |
| T-L4 | migração (`DROP`) + só 2023-08 → DQ do log | **falha** como esperado ("2 faltando, ex.: 2023-06"); carga completa e depois só 2023-08 passam e voltam a 0/0 |
| T-L5 | `fato_mudanca_estado`: (a)(b)(c) + reprocessar só 2023-06 depois da migração do log | 0/0 em 57.976 linhas nos quatro; `dq_fato_mudanca_estado` passa |
| T-F1 | fato: (a)(b)(c) | 0/0 em 903.438 linhas nos três; `dq_gold` passa em todos |
| T-F1b | tabela recriada | 33 comentários de coluna iguais aos da referência; 0/0 |
| T-F2 | painel: (a)(b)(c) | 0/0 em 88.404 linhas; DQ 3 e 3b passam |
| T-F2 | painel recriado + só 2023-08 → `dq_gold` | **falha** no 3b ("3 faltando, ex.: 2023-05"); carga completa e depois só 2023-08 passam e voltam a 0/0 |
| T-D1 | `dim_empresa` | 0/0 em 563.421 linhas; `dq_dim_empresa` passa. A amostra **não tem** matriz sobreposta (0 descartadas): a regra foi provada também num caso sintético (9 versões, 2 descartadas, antigo = novo) |
| T-D2 | `dim_empresa` | 0/0 (logo `nk_empresa` e fatias por CNPJ iguais); `dq_dim_empresa` passa |
| T-B1 | `dash_fluxos_mes` × SQL antigo de `fluxos_empresa` | 0/0 em 15.603 linhas |
| T-B1 | `dash_mercado` / `_bairros` / `_empresas` × SQL antigo | 0/0 em 199.225 / 131.575 / 1.000 linhas |
| T-B1 | eventos, idade, motivo no grão dos widgets | 0/0 (2.669 / 808 / 347 grupos; somas 57.976 / 5.361 / 5.361) |
| T-B1/T-B4 | `dash_*_mes` recriadas + só 2023-08 → `dq_gold` | **falha** no 5 ("8 faltando"); blocos 05..06 + 07..08, só 07 e 05..08 passam e batem com a referência |
| T-B2 | `comparar_widgets.py`, páginas 2, 4 e 5 | ver §12.4 |
| rodada final | `rodar_em_schema.py --etapas si,sf,gold` (log + toda a silver_final e gold, com todos os DQ, inclusive os novos), SCD2 atuais | tudo passa; log, dim, `fato_mudanca_estado`, fato, painel, OBT e mercado ativo 0/0 contra a referência |

### 12.3 Desvios da implementação

1. **Runner próprio e schemas de teste** (§12.1) em vez de sobrescrever `amostra_*` + `__antes`. Mesmo critério de
   comparação.
2. **DQ de cobertura limitado a `mes_referencia <= fim da janela`** (log, painel e `dash_*_mes`). Sem isso, numa carga
   em blocos (fase 4) o primeiro bloco falharia por não ter os meses dos blocos seguintes. Continua pegando o caso da
   revisão (migração seguida de uma janela parcial): os meses **antes** da janela faltam e o DQ falha (provado acima).
3. **DQ de cobertura também no log** (a spec pedia só `cnpj_basico` não nulo, que não pega uma janela parcial depois do
   `DROP`): meses do log = meses com versões novas de `estabelecimentos` depois do baseline, até o fim da janela.
4. **`dash_fluxos_mes` já grava sem o primeiro mês** (era o `WHERE SEM_CARGA_INICIAL` do dataset); o dataset novo é um
   `SELECT` das colunas, sem filtro. Mesmas linhas.
5. **`dash_eventos_mes` e o dataset `eventos`** ficaram só com as colunas que o widget e os filtros usam (tipo, UF,
   porte, área); saem `categoria_macro_evento`, `entidade_tipo`, `eh_matriz` e `data_referencia`, que nenhum widget
   usava. Idem `data_referencia` em `baixas_idade` e `baixas_motivo`.
6. **Painel com schema explícito**: as taxas são `DECIMAL(30,4)` (o tipo que o `CREATE OR REPLACE AS` produzia) com
   `CAST` explícito no `SELECT`; comentários de coluna escritos no `CREATE` (antes vinham do fato pelo CTAS).
7. **T-B4 feita junto com T-B1** (mesmo bloco 5 do `dq_gold.sql`), com duas conferências a mais: soma de
   `dash_baixas_idade_mes` e de `dash_baixas_motivo_mes` = baixas do fato, e soma de `dash_eventos_mes` = linhas da OBT.
8. **T-D1/T-D2 implementadas antes do T-D0** (instrução do gerente: só se a equivalência na amostra fechasse — fechou).
   Se o `EXPLAIN`/medição no cluster mostrar que o spill não vem da largura das linhas, reverter os commits
   `4d5758f` e `6becf59` é seguro (o resultado é o mesmo).
9. **Sem dica `BROADCAST` em `ponte_primaria`** no fato (§3.3 sugeria "se couber"): a linha de base mediu
   `dim_grupo_cnae` com 6,96 mi linhas / 0,46 GB e `ponte_cnae` com 55,6 mi / 2,7 GB — não cabe. A dica existente é em
   `dim_cnae` (≈1,3 mil linhas) e foi mantida.
10. **`dash_mercado_empresas` mantém o `ORDER BY capital_social DESC LIMIT 1000` do dataset antigo**: com empate de
    capital na milésima posição, a escolha entre empatados é arbitrária (já era). Na amostra não há empate na borda.

### 12.4 Comparação widget a widget (T-B2)

`scripts/amostra/comparar_widgets.py` (novo): para cada widget, a consulta que o Lakeview faz sobre o dataset
(`SELECT <campos> FROM (<dataset>) WHERE <filtros> GROUP BY <categorias>`), com o JSON antigo sobre `amostra_f3_ref` e
o novo sobre `amostra_f3`; contagens/somas exatas, `AVG`/`PERCENTILE` com tolerância 1e-9; e a lista de valores de cada
dropdown. Cenários do §5.3 (P2: sem filtro, `uf = SP`, `uf IN (RJ, MG)`, área mais frequente, UF + área; P4: os
mesmos + `porte = MEI` e UF + porte + área; P5: sem filtro, maior UF, + maior município, + maior bairro, + CNAE mais
frequente, só CNAE), cada filtro aplicado só aos datasets que ele alcança. **Resultado: 126 comparações, 0 com
diferença** (página 2: 21, das quais 2 de dropdown; página 4: 63 / 15; página 5: 42 / 12). `validar()` ok.

**Prévia** (`01f1e03111521e8fa0a9cf1aa4f96e4e`, nunca o oficial) republicada sobre `tcc_receita.amostra_f3` e lida de
volta pela API: 6 páginas, 57 widgets, 19 datasets, todos apontando para `amostra_f3`. Falta a conferência visual do
autor (T-B3).

**T-B5 (quadro "ponte")**: dataset `ponte_mes` com 3.920 linhas na amostra (abaixo de 100 mil: vai para o navegador). A
identidade fecha em Brasil (4.060 − 1.943 + 391 + 372 − 117 − 23 = 2.740), SP (783) e MEI (1.928); `dq_gold` com o
bloco 6 passa. Detalhes em `2026-11-14-dashboard-v2-narrativa.md` §2.8.

### 12.5 Riscos registrados (linha de base, gerente)

- **Filtro de bairro (U7):** 89.189 bairros distintos na base real com 4 meses = 89% do limite de 100 mil valores do
  dropdown. O número cresce pouco com os meses (é a fotografia do último mês), mas pode passar. Não foi possível
  confirmar barato se o filtro de bairro do Lakeview já fica restrito ao município escolhido (filtros em cascata);
  fica para a conferência do autor na prévia. Se passar do limite: aceitar (com UF/município escolhidos a lista
  encolhe) e pôr nota na página (recomendação do §8).
- Confirmados pela linha de base: 256 CNPJs com mais de uma ordem de matriz (tamanho de `multi_matriz`); 58% das
  versões da `dim_empresa` na carga inicial são não ATIVA (o que o Corte A deixa de expandir); fluxos ≈ 2% das linhas
  do fato (tamanho de `dash_fluxos_mes`).
- `sf_dq_dim_empresa` subiu de 89 s para 198 s entre 11-08 e 11-14: não foi alterado nesta fase.

### 12.6 Pendente de cluster

- T-D0 e o notebook de `EXPLAIN` (§5.4): log (uma leitura com janela por SCD2), dim (origem do spill), fato (se
  `antigas` passa por `Exchange` no join de CNAE).
- T-X2: medição da carga 05..08 + um mês incremental (§5.5), inclusive das tarefas de DQ novas (cobertura).
- Migração real: rodar `src/jobs/migracoes/2026-11-16_fase3.sql` e, em seguida, a carga com a janela inteira das
  tarefas `si_log_eventos_negocio`, `gold_fato_indicadores_setoriais_mes`, `gold_dashboard_agregados` e DQ.
- Job no workspace: a tarefa nova `gold_dashboard_agregados` está só no YAML (o Job do Databricks não foi alterado).
