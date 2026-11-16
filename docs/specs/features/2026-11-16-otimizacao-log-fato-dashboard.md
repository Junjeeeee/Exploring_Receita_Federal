# Feature: otimização do log de eventos, do fato empresa × mês e do dashboard (fase 3)

**Status:** rascunho (aguarda aprovação do autor; decisões no §8)
**Camadas afetadas:** silver_intermediate (log), silver_final (dim_empresa, fato_mudanca_estado), gold (fato, painel,
tabelas novas de dashboard), dashboard Lakeview
**Branch:** `otimizacao_pipeline` (spec guarda-chuva: [`2026-11-16-otimizacao-pipeline.md`](2026-11-16-otimizacao-pipeline.md))

Princípio que manda em tudo aqui (guarda-chuva §2): **otimização não muda número**. Cada mudança é validada na
amostra de 1% com `EXCEPT ALL` nos dois sentidos = 0 linhas; desempenho só se mede na base real.

Convenção de status usada no texto: **implementado** = já está no código hoje; **proposto** = desenho desta spec;
**a medir** = hipótese que depende de dado que ainda não foi consultado.

> Nota de execução desta spec (2026-11-16): as consultas de confirmação no SQL warehouse **não rodaram** — o CLI do
> Databricks está com o refresh token inválido (mesmo bloqueio da linha de base, `docs/medicoes/2026-11-16-linha-de-base.md`).
> Tudo que depende de número está marcado como **a medir**, com a consulta pronta no §5.4.

---

## 1. Requisito

- **Problema:** antes de rodar a janela de ~40 meses (em blocos de 6–12 meses, cluster clássico DBR 15.4 / Spark 3.5,
  r5d.xlarge, 1–4 workers spot, sem serverless no Job), quatro pontos custam muito ou não escalam:
  1. `si_log_eventos_negocio`: 873 s (241 s antes da nova semântica de eventos); `sf_fato_mudanca_estado` 1,7× mais lenta.
  2. `gold_fato_empresa_mes`: 84 GB de shuffle para 0,9 GB de entrada.
  3. `sf_dim_empresa`: única tarefa com spill (7,6 GB).
  4. Dashboard Lakeview: datasets grandes (`mercado` 6,7 mi linhas, `mercado_bairros` 2,8 mi, `fluxos_empresa` 1,5 mi).
- **Quem consome:** o próprio Job (tempo e custo por bloco de meses) e o dashboard oficial (tempo de resposta).
- **Fora de escopo:** reescrita dos SCD2 da silver (fase 2, `2026-11-16-otimizacao-scd2.md`; as colunas e a semântica
  das tabelas SCD2 **não mudam**); bronze (B17); mudança de qualquer regra de negócio; bugs conhecidos que não são de
  desempenho (eles são apontados quando a otimização encosta neles, mas não são corrigidos aqui).

---

## 2. Diagnóstico por arquivo

### 2.1 `src/silver_intermediate/ingest/log_eventos_negocio.sql` (implementado hoje)

**Leituras por execução** (contando cada `FROM`/`JOIN` de tabela; o Spark não materializa CTE, cada referência é
recalculada a menos que o plano final seja idêntico e caia em *ReusedExchange*):

| Tabela | Onde é lida (linhas) | Leituras no texto |
|---|---|---|
| `estabelecimentos` | `params` MIN (49); `est_versoes` (65–81); `matriz_canonica` lado `e` (183); lado `melhor` do ANTI JOIN (188) | 4 |
| `dados_simples` | `params` MIN (50); `sim_versoes` (269–283) | 2 |
| `socios` | `params` MIN (51); `soc_versoes` (353–364) | 2 |
| `empresas` | `pk_empresa` (206–211); `emp_versoes` (237–245) | 2 |

Mas o número que pesa não é o do texto, é o do **plano**: `est_janela` é usado por 4 ramos (`ev_estado_cadastral`,
`ev_estado_cadastral_primeira`, `ev_cnae_principal`, `ev_cnae_secundario`), `sim_janela` por 6 ramos do
`UNION ALL` (292–334) e `soc_janela` por 4 (372–405). Cada ramo usa colunas diferentes do `LAG`, e a otimização
de *column pruning* do Spark remove as expressões de janela não usadas em cada ramo — os sub-planos deixam de ser
idênticos e o *exchange reuse* não acontece. Resultado provável: **uma leitura + um shuffle + uma ordenação da
tabela SCD2 inteira por ramo** (≈4× estabelecimentos, até 6× dados_simples, 4× socios). Isso é o "lê cada SCD2
4–5×" do B20. **A medir** com `EXPLAIN FORMATTED` (contar `Scan` e `Exchange hashpartitioning(pk...)` por tabela; §5.4).

**Por que ficou 3,6× mais lento depois da mudança de semântica** (commit `4f84c49`): a versão antiga detectava
transições por self-join `antigo JOIN novo ON antigo.pk = novo.pk AND antigo.valid_until = novo.valid_since WHERE
novo.valid_since BETWEEN ini AND fim`. O filtro da janela era empurrado para o lado `novo` e, pela igualdade, o
otimizador inferia `antigo.valid_until BETWEEN ini AND fim` — **os dois lados chegavam ao shuffle já reduzidos às
versões da janela**. A versão nova troca o self-join por `LAG(...) OVER (PARTITION BY pk ORDER BY valid_since)`
sobre a **tabela inteira** e só depois filtra a janela (linhas 85–90, 284–289, 365–370). Um filtro em
`valid_since` não pode descer para baixo de uma janela particionada por `pk` (mudaria o resultado do `LAG`), então
o Spark embaralha e ordena **todo o histórico**, com todas as colunas do `LAG`, em cada ramo. Somam-se dois custos
novos que também são proporcionais ao histórico e não à janela:

- `matriz_canonica` (177–198): ANTI JOIN de todas as versões de matriz contra todas as versões de matriz do mesmo
  `cnpj_basico`, com condição de sobreposição de intervalo — antes o nascimento era um filtro simples por
  `data_inicio_atividade`.
- `pk_empresa` (206–211): `GROUP BY cnpj_basico` sobre `empresas` inteira, para depois usar só as empresas nascidas
  na janela.

Consequência para os 40 meses: **o custo do log cresce com o tamanho do histórico, não com o tamanho do bloco.** Um
bloco de 6 meses no fim da janela custaria mais que a carga inicial inteira.

Observação: na execução de 4 meses a janela (2023-05..08) **contém o baseline**. As versões que começam no baseline
nunca geram evento (ordem 1 no baseline é carga inicial; ordem > 1 no baseline é impossível porque o baseline é o
menor `valid_since`). Hoje elas passam pelo shuffle mesmo assim — e são a maioria das linhas.

### 2.2 `src/silver_final/ingest/fato_mudanca_estado.sql` (implementado)

- `mapeamento_pk` (34–51): `SELECT DISTINCT entidade_tipo, pk, cnpj_basico` das **quatro SCD2 inteiras** e `UNION`
  (deduplicação = mais um shuffle) — só para descobrir o `cnpj_basico` de cada `pk` do log da janela.
- `versoes_pareadas` (121–130): dois joins com `dim_empresa` por `cnpj_raiz` + intervalo (`d_after`, `d_before`), sem
  restringir a dimensão à janela.
- `primeira_versao_empresa` (95–105): `ROW_NUMBER` sobre a `dim_empresa` inteira.
- `DISTINCT` final (134–149) sobre linhas largas.
- O texto deste arquivo não mudou com a nova semântica; a lentidão (1,7×) vem de **mais eventos** (C-16, D-17,
  eventos de primeira aparição) passando pelos mesmos joins e de a `dim_empresa` ter ficado maior (C-02).
- Observação (não é desempenho, não se corrige aqui): `d_before` aceita qualquer versão com `inicio < mes` e
  `fim >= mes`; se a empresa não mudou de versão no mês do evento, casa **duas** versões e o `DISTINCT` deixa duas
  linhas com `sk_empresa_anterior` diferentes. **A medir** (§5.4); se aparecer, vira item de backlog, não desta fase.

### 2.3 `src/silver_final/ingest/dim_empresa.sql` (implementado)

| Passo | Linhas | Custo |
|---|---|---|
| `est_matriz`: ANTI JOIN da matriz canônica | 172–187 | self-join de **linhas largas** (≈25 colunas: endereço, contatos, CNAEs) por `cnpj_basico` com condição de sobreposição; as duas pontas são todas as versões de matriz |
| `fronteiras`: 6 projeções com `UNION` (dedup) | 213–220 | shuffle estreito |
| `fatias`: `LEAD` por `cnpj_basico` | 224–234 | shuffle estreito |
| `unificado_raw`: 3 joins por `cnpj_basico` + intervalo | 240–269 | três sort-merge joins com o lado largo de `est_matriz` (e de `emp`/`sim`) embaralhado e ordenado; o resultado intermediário carrega todas as colunas largas |

O spill (7,6 GB) é compatível com ordenações de linhas largas em sort-merge join; memória sobra no resto do Job
(B20). **Range join hint não resolve:** a otimização de range join do Databricks só aceita colunas numéricas, `DATE`
ou `TIMESTAMP` na condição de intervalo (os meses aqui são `STRING 'YYYY-MM'`), e para `LEFT OUTER` só vale com o
ponto do lado esquerdo; ANTI JOIN não é coberto ([docs: range join](https://docs.databricks.com/aws/en/optimizations/range-join)).
Além disso já existe chave de igualdade (`cnpj_basico`) com poucas versões por chave — o problema é a largura das
linhas, não a explosão do intervalo.

### 2.4 `src/gold/fato_empresa_mes.sql` (implementado)

O fato **já é incremental na escrita** (`INSERT INTO ... REPLACE WHERE mes_referencia BETWEEN` a janela, 82–84): um
bloco de meses só substitui os seus meses. O que não escala é a leitura/expansão:

| Passo | Linhas | Custo |
|---|---|---|
| `primeira_aparicao`: `GROUP BY cnpj_raiz` na dim inteira | 104–113 | shuffle estreito (2 colunas), proporcional ao histórico — aceitável |
| `versoes_janela`: versões que tocam a janela | 143–171 | inclui **todas as versões correntes**, também as ~30+ mi de empresas baixadas há anos (`fim_vigencia` NULL) |
| `versoes_com_anterior`: `LAG` por `cnpj_raiz` | 172–189 | shuffle + ordenação de **todas** as versões da janela, largas (UF, município, bairro, CNAE...), **antes** do filtro que descarta as mortas (219–220) |
| `versoes_a_expandir`: filtro `ATIVA OR inicio >= mes_ini` | 200–221 | na carga inicial `mes_ini` = baseline: **toda** versão começa em `mes_ini`, então as ~30 mi de mortas também são expandidas |
| `meses_empresa`: `EXPLODE(SEQUENCE(...))` | 222–230 | multiplica cada versão pelos meses; mortas × 4 meses viram ~120 mi de linhas que o `WHERE` final (360–364) joga fora |
| `LEFT JOIN mei_na_baixa` | 355–359 | feito **depois** da expansão e **antes** do `WHERE`: se o Spark escolher sort-merge (o tamanho de `mei_na_baixa` não é conhecido no plano), embaralha as centenas de milhões de linhas expandidas |
| escrita particionada com `optimizeWrite` (conf. do cluster) | — | um shuffle do resultado final (~23 mi linhas/mês) para agrupar arquivos por partição |

Hipótese para os 84 GB (**a medir** no event log): expansão das mortas + join de `mei_na_baixa` depois da expansão +
shuffle do optimized write. Os dois primeiros são desperdício puro; o terceiro é proporcional à saída.

### 2.5 `src/gold/fato_indicadores_setoriais_mes.sql` (painel, implementado)

`CREATE OR REPLACE TABLE ... AS SELECT ... FROM fato_empresa_mes` **inteiro** a cada execução. Com 40 meses são
~900 mi de linhas lidas e agregadas a cada bloco, mesmo que o bloco tenha 6 meses. Este é o "recalcula tudo" de
verdade da gold.

### 2.6 Dashboard (`src/gold/dashboard/gerar_dashboard.py`)

**Limites oficiais do AI/BI (Lakeview)** ([docs: dashboard limits](https://docs.databricks.com/aws/en/dashboards/limits),
[docs: caching](https://docs.databricks.com/aws/en/dashboards/caching), consultados em 2026-11-16):

- "Dataset size has no hard limit, but large datasets are processed differently." **Não há corte de dataset.**
- Dataset **≤ 100.000 linhas e ≤ 100 MB**: o resultado vai para o navegador; filtros e agregações rodam no navegador.
- Dataset **> 100.000 linhas ou > 100 MB**: a consulta do dataset é embrulhada num `WITH` e **cada widget/filtro vira
  uma consulta no SQL warehouse**; aplicar filtro reexecuta a consulta (salvo cache de 24 h com os mesmos filtros).
- Gráficos desenham até 15 mil linhas (tabelas, 100 mil) — vale para o resultado já agregado do widget.
- Filtros dropdown/multiselect listam até **100.000 valores distintos**; os excedentes não aparecem nem na busca.
- Materialização de datasets do próprio dashboard existe, mas é Beta, roda em **serverless** (pipelines Lakeflow) e
  não aceita dataset com parâmetro ([docs: materialization](https://docs.databricks.com/aws/en/dashboards/materialization))
  — descartada pela regra de não usar serverless no processamento.

Portanto o problema real não é "truncar": é que os datasets grandes **recalculam a sua consulta inteira no warehouse
a cada clique**. E o SQL deles hoje é pesado: `fluxos_empresa` varre `fato_empresa_mes` (todos os meses) com um
join; `mercado` agrega as ~23 mi de linhas de `dashboard_mercado_ativo`; `baixas_motivo` junta fato × OBT.

**Quem consome cada dataset grande** (widgets e filtros, com linhas do gerador):

| Dataset (linhas) | Cresce com os meses? | Widgets | Filtros da página | Agregações usadas |
|---|---|---|---|---|
| `fluxos_empresa` (195–215) | **sim, linear** (~500 mil/mês → ~20 mi em 40 meses) | P2: `c2_mei_ab`, `c2_mei_bx`, `b2_porte`; P4: `c4_falencia`, `c4_idade`, `c4_menos_2` | P2: UF, grande área (572–573); P4: UF, porte, grande área (631–633) | `AVG(pct_*)`, `PERCENTILE(idade_baixa_anos, 0.5)`, `SUM(qtd)` por `fluxo` × `porte` |
| `mercado` (293–297) | **não** (só o último snapshot; cresce com o estoque de ativas) | P5: `c5_ativas`, `b5_porte`, `b5_area` | UF, município, bairro, CNAE (675–678) | `SUM(empresas_ativas)` por nada / porte / área |
| `mercado_bairros` (301–320) | não | P5: `b5_bairro` | UF, município, bairro, CNAE | `SUM(empresas_ativas)` por bairro |
| `mercado_empresas` (323–330) | não (LIMIT 1000) | P5: `t5_empresas` | idem | linhas |
| `eventos` (253–265) | sim (grão inclui `data_referencia`) | P4: `b4_tipo` | UF, porte, área | `SUM(qtd_eventos)` por `tipo_evento` |
| `baixas_idade` (217–222) | sim (grão inclui `data_referencia`) | P4: `b4_idade` | UF, porte, área | `SUM(qtd_baixas)` por faixa |
| `baixas_motivo` (228–250) | sim; e faz join fato × OBT a cada consulta | P4: `b4_motivo` | UF, porte, área | `SUM(qtd_empresas)` por categoria |

Nenhum widget das páginas 2, 4 e 5 usa o mês (não há filtro de data); `data_referencia` nos grãos de `eventos`,
`baixas_idade` e `baixas_motivo` só multiplica linhas. Os datasets do painel (`painel`, `estoque_mes`, `resumo_mes`,
taxas, `top_cnaes`) ficam em ~1,3 mil linhas/mês (mês × 28 UF × 6 portes × 8 áreas) → ~54 mil em 40 meses: continuam
no navegador e **ficam como estão**.

Correção de premissa: `mercado` e `mercado_bairros` **não crescem 3–10×** com 40 meses — leem só o snapshot corrente.

---

## 3. Desenho proposto

Regras de compatibilidade (todas as propostas): só Spark 3.5 (`TRY_TO_TIMESTAMP` sim, `TRY_TO_DATE` não);
`OPTIMIZE` **sem** `ZORDER` (tabela com clustering automático recusa); `INSERT INTO ... REPLACE WHERE` é suportado
em SQL desde o DBR 12.2 LTS, também em tabela não particionada ou com liquid clustering, e o predicado pode usar
colunas que não são de partição ([docs: selective overwrite](https://docs.databricks.com/aws/en/delta/selective-overwrite)).
`REPLACE USING`/`REPLACE ON` (DBR 16.3/17.1) **não** existem no 15.4. Parâmetros continuam como literais
substituídos pelo `run_sql.py`.

### 3.1 Log de eventos — uma leitura por SCD2, só o que importa à janela

**Ideia 1 — filtrar antes do `LAG`, sem mudar o resultado.** Para um evento detectado no mês M da janela só
importam: (a) as versões que começam na janela **depois do baseline** e (b) a versão imediatamente anterior de cada
uma. Como nas SCD2 deste projeto `valid_until` = `LEAD(valid_since)` (contiguidade por construção,
`estabelecimentos.sql:183`, `empresas.sql:98`, `dados_simples.sql:109`, `socios.sql:219`), a anterior de uma versão
que começa em M termina exatamente em M — logo tem `valid_until` na janela. Filtro:

```sql
WHERE (valid_since BETWEEN '{{mes_referencia_inicial}}' AND '{{mes_referencia_final}}'
       AND valid_since > <baseline da tabela>)
   OR  valid_until BETWEEN '{{mes_referencia_inicial}}' AND '{{mes_referencia_final}}'
```

Prova de equivalência (curta, para a defesa): sobre o conjunto filtrado, `LAG` por `pk` ordenado por `valid_since`
devolve, para cada versão de (a), a mesma anterior que devolveria sobre a tabela inteira, porque essa anterior está no
conjunto e nenhuma versão da mesma `pk` fica entre as duas. "É a primeira versão da pk" (`ordem_versao = 1`) vira "não
tem anterior no conjunto filtrado": se existisse anterior na tabela, ela teria `valid_until` = `valid_since` da
versão, dentro da janela, e estaria no conjunto. Versões que começam no baseline nunca geram evento (§2.1), então
sair do conjunto não muda nada. **A equivalência depende da contiguidade** — que hoje é só suposição (comentário das
linhas 17–21). Por isso entra uma asserção nova (T-L1) que falha se alguma `pk` tiver buraco entre versões.

Ganho: o shuffle passa de "todo o histórico" para "versões que mudaram no bloco" — inclusive na carga inicial, porque
as versões do baseline que nunca mudaram (a maioria) saem.

**Ideia 2 — uma passada por tabela, todos os eventos da linha de uma vez.** Em vez de 4–6 ramos de `UNION ALL` que
o Spark recalcula, cada SCD2 vira um único `SELECT` que monta um array com os eventos possíveis daquela versão e o
"explode" com `INLINE` (gerador do Spark 3.x). A detecção continua legível: cada `IF(condição, evento, NULL)` é uma
regra do glossário.

```sql
-- Esqueleto (estabelecimentos). Mesmas regras, mesmas colunas, mesmo texto de valor_antigo/valor_novo de hoje.
WITH params AS (
    SELECT '{{mes_referencia_inicial}}' AS mes_ini, '{{mes_referencia_final}}' AS mes_fim,
           (SELECT MIN(valid_since) FROM tcc_receita.silver_intermediate.estabelecimentos) AS baseline_est
           -- idem baseline_simples, baseline_socios (leitura de uma coluna; barato)
),
est_rel AS (   -- (a) versões novas da janela pós-baseline + (b) as anteriores delas. UMA leitura.
    SELECT e.pk, e.cnpj_basico, e.cnpj_ordem, e.identificador_matriz_filial, e.situacao_cadastral_desc,
           e.motivo_situacao_cadastral, e.data_situacao_cadastral, e.cnae_fiscal_principal,
           e.cnae_fiscal_secundaria, e.data_inicio_atividade, e.valid_since, e.valid_until
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    CROSS JOIN params p
    WHERE (e.valid_since BETWEEN p.mes_ini AND p.mes_fim AND e.valid_since > p.baseline_est)
       OR  e.valid_until BETWEEN p.mes_ini AND p.mes_fim
),
est_versoes AS (
    SELECT r.*,
           LAG(valid_since)               OVER w IS NULL AS eh_primeira_versao,
           LAG(situacao_cadastral_desc)   OVER w AS ant_situacao_desc,
           LAG(motivo_situacao_cadastral) OVER w AS ant_motivo,
           LAG(cnae_fiscal_principal)     OVER w AS ant_cnae_principal,
           LAG(cnae_fiscal_secundaria)    OVER w AS ant_cnae_secundaria
    FROM est_rel r
    WINDOW w AS (PARTITION BY pk ORDER BY valid_since)
),
est_janela AS (   -- só as versões novas (a); as anteriores serviram só ao LAG
    SELECT v.* FROM est_versoes v CROSS JOIN params p
    WHERE v.valid_since BETWEEN p.mes_ini AND p.mes_fim AND v.valid_since > p.baseline_est
),
ev_est AS (
    SELECT /*+ BROADCAST(m) */ e.pk, ev.*
    FROM est_janela e
    LEFT JOIN dom_motivos m ON e.motivo_situacao_cadastral = m.cod_motivo
    LATERAL VIEW INLINE(FILTER(ARRAY(
        -- 1a. transição de situação ou de motivo
        IF(NOT e.eh_primeira_versao
           AND (e.ant_situacao_desc IS DISTINCT FROM e.situacao_cadastral_desc
                OR e.ant_motivo IS DISTINCT FROM e.motivo_situacao_cadastral),
           NAMED_STRUCT('entidade_tipo', 'ESTABELECIMENTO', 'evento', 'MUDOU ESTADO CADASTRAL',
                        'valor_antigo', COALESCE(e.ant_situacao_desc, 'NAO INFORMADA'),
                        'valor_novo',   COALESCE(e.situacao_cadastral_desc, 'NAO INFORMADA'),
                        'informacao_adicional', m.desc_motivo,
                        'data_texto', e.data_situacao_cadastral,
                        'codigo_motivo', e.motivo_situacao_cadastral), NULL),
        -- 1b. primeira aparição pós-baseline já fora de ATIVA (C-16)
        IF(e.eh_primeira_versao AND e.situacao_cadastral_desc IS NOT NULL
           AND e.situacao_cadastral_desc <> 'ATIVA',
           NAMED_STRUCT(... 'valor_antigo', CAST(NULL AS STRING), 'valor_novo', e.situacao_cadastral_desc, ...), NULL),
        -- 2. CNAE principal; 3. lista de secundários (data_texto NULL, codigo_motivo NULL)
        IF(NOT e.eh_primeira_versao AND e.ant_cnae_principal IS DISTINCT FROM e.cnae_fiscal_principal, ..., NULL),
        IF(NOT e.eh_primeira_versao AND e.ant_cnae_secundaria IS DISTINCT FROM e.cnae_fiscal_secundaria, ..., NULL)
    ), x -> x IS NOT NULL)) ev
)
-- data_evento = CAST(TRY_TO_TIMESTAMP(ev.data_texto, 'yyyyMMdd') AS DATE) só para os eventos de estado
--   (CNAE: NULL, como hoje); eh_matriz = COALESCE(e.identificador_matriz_filial = '1', FALSE).
```

Notas de implementação: todos os `NAMED_STRUCT` de um array precisam dos mesmos nomes e tipos de campo (use
`CAST(NULL AS STRING)`); `dados_simples` e `socios` seguem o mesmo molde (6 e 4 regras); `empresas` (uma regra) fica
com o filtro da Ideia 1 e um `SELECT` simples. O `UNION ALL` final de 4 blocos (um por tabela) e o
`INSERT ... REPLACE WHERE` não mudam.

**Ideia 3 — nascimento sem self-join do histórico inteiro.** O nascimento é "primeira versão da matriz canônica de um
`cnpj_basico`, depois do baseline, na janela". Só pode nascer na janela um CNPJ que tenha **alguma versão de matriz
começando na janela** — e essas versões já estão em `est_rel`. Então:

```sql
candidatos AS (   -- sai de est_rel: nenhuma leitura nova
    SELECT DISTINCT cnpj_basico FROM est_rel CROSS JOIN params p
    WHERE identificador_matriz_filial = '1' AND valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND valid_since > p.baseline_est AND cnpj_basico IS NOT NULL
),
hist_matriz AS (  -- histórico COMPLETO de matriz, mas só dos candidatos e só colunas estreitas
    SELECT e.pk, e.cnpj_basico, e.cnpj_ordem, e.data_inicio_atividade, e.valid_since, e.valid_until
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    LEFT SEMI JOIN candidatos c ON c.cnpj_basico = e.cnpj_basico
    WHERE e.identificador_matriz_filial = '1'
),
matriz_canonica AS (   -- MESMA regra de hoje (linhas 177–198), aplicada ao conjunto reduzido
    SELECT h.* FROM hist_matriz h
    LEFT ANTI JOIN hist_matriz melhor
        ON melhor.cnpj_basico = h.cnpj_basico AND melhor.cnpj_ordem < h.cnpj_ordem
       AND melhor.valid_since < COALESCE(h.valid_until, '9999-12')
       AND COALESCE(melhor.valid_until, '9999-12') > h.valid_since
    WHERE h.valid_since IS NOT NULL AND h.valid_since < COALESCE(h.valid_until, '9999-12')
),
primeira_versao_matriz AS (   -- igual a hoje (QUALIFY ROW_NUMBER ... ORDER BY valid_since, pk)
    ...
),
pk_empresa AS (   -- só para os nascidos (semi join), não para empresas inteira
    SELECT emp.cnpj_basico, MIN(emp.pk) AS pk
    FROM tcc_receita.silver_intermediate.empresas emp
    LEFT SEMI JOIN primeira_versao_matriz mat ON mat.cnpj_basico = emp.cnpj_basico
    GROUP BY emp.cnpj_basico
)
```

Equivalência: a primeira versão canônica de um CNPJ que nasce na janela é uma versão de matriz que começa na janela,
logo o CNPJ está em `candidatos`; para ele, `hist_matriz` tem **todo** o histórico de matriz (o lado `melhor` do ANTI
JOIN continua sem filtro de validade, como hoje), então a regra da matriz canônica dá o mesmo resultado. O
`ev_empresa_nasceu` mantém os filtros `valid_since > baseline_est` e `BETWEEN mes_ini AND mes_fim` (229–231).

**Ideia 4 (opcional, decisão U1) — `cnpj_basico` no log.** O log já lê as quatro SCD2, que têm `cnpj_basico`. Gravar
`cnpj_basico` como coluna nova **no fim** do log elimina o `mapeamento_pk` do `fato_mudanca_estado` (§3.2), que hoje
relê e deduplica as quatro tabelas inteiras. Não é extrair CNPJ da `pk` (a regra do `CLAUDE.md` continua cumprida):
o valor vem da coluna `cnpj_basico` da própria linha da SCD2.

**Sugestão para a fase 2 (não obrigatória):** se as SCD2 reescritas tiverem clustering por `valid_since` (ou
`valid_until`), o filtro da Ideia 1 também pula arquivos na leitura. Sem isso o ganho vem do shuffle, que é o que pesa.

### 3.2 `fato_mudanca_estado` (efeito colateral do 3.1)

- Com U1 aprovada: `logs_base` usa `l.cnpj_basico AS cnpj_raiz` e o `mapeamento_pk` some. Sem U1: `mapeamento_pk`
  filtra cada SCD2 por `LEFT SEMI JOIN` com as `pk` do log da janela antes do `DISTINCT` (continua lendo as tabelas,
  mas o `UNION`/`DISTINCT` fica do tamanho da janela).
- `d_after`, `d_before` e `primeira_versao_empresa` passam a ler só `dim_empresa` dos CNPJs presentes no log da janela
  (`LEFT SEMI JOIN` por `cnpj_raiz`), com as mesmas condições de intervalo. Mesmas linhas de saída, porque um evento
  só casa versões do próprio CNPJ.
- A observação do `d_before` (§2.2) não é corrigida aqui; a comparação `EXCEPT ALL` garante que o resultado é o mesmo,
  inclusive nos casos duplicados.

### 3.3 `fato_empresa_mes` — expandir só o que vira linha

O resultado não muda; muda o caminho. Três cortes, cada um com a prova de que não altera linha nenhuma:

**Corte A — versão não ATIVA gera no máximo uma linha, no mês em que começa.** Dentro de uma versão a situação não
muda. Numa versão não ATIVA, para todo mês M > início: `situacao_inicio_mes` = a própria situação (não ATIVA), então
`ativa_inicio_mes` e `ativa_fim_mes` são FALSE, não há abertura (`eh_mes_inicio_versao` falso) nem mudança de
situação → o `WHERE` final (360–364) descarta. No mês M = início só há linha se M > baseline e M estiver na janela.
Logo: versões ATIVA expandem como hoje; versões não ATIVA **não expandem** — geram uma linha só, com
`data_referencia` = `data_inicio_vigencia`, quando `inicio_vigencia BETWEEN mes_ini AND mes_fim`, `inicio_vigencia >
mes_baseline` e `inicio_vigencia <= último mês da dim`. Corta as ~30 mi de mortas × meses (na carga inicial, ~120 mi
de linhas inúteis em 4 meses).

**Corte B — o "mês anterior" vem de um join de igualdade, só para versões que começam na janela.** O `LAG` (172–189)
só importa no mês de início da versão (nos outros meses `situacao_inicio_mes` é a própria situação; `porte/mei/
simples_versao_anterior` só são usados com `saiu_estoque_mes`, que no meio da versão é impossível). E o valor do
`LAG` só é usado quando `fim_vigencia_anterior = inicio_vigencia`. Então:

```sql
novas AS (          -- versões que começam na janela, depois do baseline (pequeno: só o que mudou)
    SELECT v.* FROM versoes_janela v CROSS JOIN params p CROSS JOIN limites_dim l
    WHERE v.inicio_vigencia BETWEEN p.mes_ini AND p.mes_fim AND v.inicio_vigencia > l.mes_baseline
),
anteriores AS (     -- candidatas a "versão anterior": terminam dentro da janela (estreito)
    SELECT cnpj_raiz, fim_vigencia, situacao_cadastral AS situacao_versao_anterior,
           enquadramento_porte_analitico AS porte_versao_anterior,
           opcao_pelo_mei AS mei_versao_anterior, opcao_pelo_simples AS simples_versao_anterior
    FROM versoes_janela CROSS JOIN params p
    WHERE fim_vigencia BETWEEN p.mes_ini AND p.mes_fim
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
antigas AS (        -- começaram antes da janela (ou no baseline): nenhuma coluna de "anterior" é usada
    SELECT v.*, CAST(NULL AS STRING) AS fim_vigencia_anterior, CAST(NULL AS STRING) AS situacao_versao_anterior,
           CAST(NULL AS STRING) AS porte_versao_anterior, CAST(NULL AS BOOLEAN) AS mei_versao_anterior,
           CAST(NULL AS BOOLEAN) AS simples_versao_anterior, FALSE AS eh_primeira_versao, FALSE AS eh_mei_na_baixa
    FROM versoes_janela v CROSS JOIN params p CROSS JOIN limites_dim l
    WHERE (v.inicio_vigencia < p.mes_ini OR v.inicio_vigencia = l.mes_baseline)
      AND v.situacao_cadastral = 'ATIVA'                               -- Corte A: mortas antigas não geram linha
)
-- versoes_a_expandir = (novas_com_anterior ∪ antigas) + CNAE (broadcast, como hoje);
-- nas não ATIVA, data_ultimo_mes = data_primeiro_mes = data_inicio_vigencia (Corte A).
```

Por que é igual: no baseline nada termina (todas as versões começam ≥ baseline e terminam depois de começar), então
versão que começa no baseline nunca tem anterior contígua — o `LAG` de hoje dá `situacao_inicio_mes` NULL e o
`eh_baseline` zera os fluxos; `antigas` reproduz isso com NULLs. Sem sobreposição de versões (garantida pelo DQ de
colisão de SK da `dim_empresa`), a versão com `fim = inicio` é exatamente a anterior do `LAG`. O caminho largo
(`antigas`, ~23 mi de ativas) **não passa por shuffle nenhum** antes da expansão.

**Corte C — D-17b antes da expansão.** `mei_na_baixa` (114–142) continua igual (já é estreito e com broadcast do lado
pequeno). O join com ele sai do fim (355–359) e vai para o nível de versão em `novas_com_anterior`. No `SELECT` final,
`WHEN mb.cnpj_raiz IS NOT NULL THEN 'MEI'` vira `WHEN f.eh_mei_na_baixa AND f.teve_abertura_mes AND f.teve_baixa_mes
THEN 'MEI'` (idem `opcao_pelo_mei`) — as mesmas três condições do `ON` de hoje. Nenhum join depois do `EXPLODE`.

**Escrita.** O shuffle do *optimized write* (conf. global do cluster, `gerar_job.py:87`) fica até a medição. Opção
(decisão U3): desligá-lo só nesta tarefa (`SET spark.databricks.delta.optimizeWrite.enabled = false` no início do
arquivo e `true` no fim), aceitando mais arquivos por partição, e compactar com `OPTIMIZE` simples ao fim do bloco.

**Painel incremental (`fato_indicadores_setoriais_mes`).** Troca `CREATE OR REPLACE TABLE AS` por `CREATE TABLE IF
NOT EXISTS` (mesmo schema, `PARTITIONED BY (mes_referencia)`) + `INSERT INTO ... REPLACE WHERE mes_referencia
BETWEEN` a janela, lendo só esses meses do fato. A agregação é por mês, então cada mês do painel só depende do mesmo
mês do fato. Migração: um `DROP TABLE` único em `src/jobs/migracoes/` (D-08 permite em dev). Ressalva: se a fonte
macro (`fato_indicadores_macro_mes`) revisar um mês antigo, o painel só reflete ao reprocessar aquele mês (hoje
refletiria em qualquer execução) — decisão U5.

### 3.4 `dim_empresa` — fatias por pontos de mudança, sem join por intervalo de linhas largas

Continua rebuild total (D-08). Duas mudanças:

**(a) Matriz canônica pelo caminho estreito.** O ANTI JOIN só pode descartar algo em CNPJ com **mais de uma ordem**
marcada como matriz (B19, raro — **a medir**). Então:

```sql
mat_chaves AS (   -- estreito: só chaves
    SELECT pk, cnpj_basico, cnpj_ordem, valid_since, valid_until
    FROM tcc_receita.silver_intermediate.estabelecimentos WHERE identificador_matriz_filial = '1'
),
multi AS (SELECT cnpj_basico FROM mat_chaves GROUP BY cnpj_basico HAVING COUNT(DISTINCT cnpj_ordem) > 1),
descartadas AS (  -- mesma condição de hoje (177–182), só nos CNPJs com mais de uma ordem
    SELECT e.pk, e.valid_since
    FROM mat_chaves e LEFT SEMI JOIN multi m ON m.cnpj_basico = e.cnpj_basico
    LEFT SEMI JOIN mat_chaves melhor
        ON melhor.cnpj_basico = e.cnpj_basico AND melhor.cnpj_ordem < e.cnpj_ordem
       AND melhor.valid_since < COALESCE(e.valid_until, '9999-12')
       AND COALESCE(melhor.valid_until, '9999-12') > e.valid_since
),
est_matriz AS (   -- colunas largas lidas uma vez; descartadas é pequena (broadcast)
    SELECT /*+ BROADCAST(d) */ <mesmas colunas e expressões de hoje, 137–171>
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    LEFT ANTI JOIN descartadas d ON d.pk = e.pk AND d.valid_since = e.valid_since
    WHERE <mesmos filtros de hoje, 183–186>
)
```

Igual a hoje: em CNPJ com uma ordem só, `melhor.cnpj_ordem < e.cnpj_ordem` nunca vale, o ANTI JOIN não remove nada.
O mesmo molde serve à Ideia 3 do log (se o autor preferir, as duas consultas podem compartilhar o texto).

**(b) Fatias por "ponteiros" preenchidos para frente, atributos por join de igualdade.** Em vez de calcular as fatias
e depois procurar, por intervalo, a versão de cada fonte que contém cada fatia, cada fonte entra numa linha do tempo
estreita com um ponteiro `(pk, valid_since, valid_until)` no mês em que a versão começa; a fatia herda o último
ponteiro de cada fonte (`LAST(..., TRUE)`, que ignora nulos) e confere se ele ainda vale.

```sql
pontos AS (   -- uma linha por fronteira (início e fim de versão) de cada fonte; estreito
    SELECT cnpj_basico, valid_since AS mes,
           NAMED_STRUCT('pk', pk, 'vs', valid_since, 'vu', valid_until) AS p_est,
           CAST(NULL AS STRUCT<pk:STRING, vs:STRING, vu:STRING>) AS p_emp,
           CAST(NULL AS STRUCT<pk:STRING, vs:STRING, vu:STRING>) AS p_sim
    FROM est_matriz
    UNION ALL SELECT cnpj_basico, valid_until, NULL, NULL, NULL FROM est_matriz      -- só fronteira
    UNION ALL SELECT cnpj_basico, valid_since, NULL, NAMED_STRUCT(...), NULL FROM emp_limpa
    UNION ALL SELECT cnpj_basico, valid_until, NULL, NULL, NULL FROM emp_limpa
    UNION ALL SELECT cnpj_basico, valid_since, NULL, NULL, NAMED_STRUCT(...) FROM sim_limpa
    UNION ALL SELECT cnpj_basico, valid_until, NULL, NULL, NULL FROM sim_limpa
),
por_mes AS (  -- = UNION das fronteiras de hoje (213–220): uma linha por (cnpj, mês)
    SELECT cnpj_basico, mes, MAX(p_est) AS p_est, MAX(p_emp) AS p_emp, MAX(p_sim) AS p_sim
    FROM pontos GROUP BY cnpj_basico, mes
),
preenchido AS (
    SELECT cnpj_basico, mes AS inicio_fatia,
           LEAD(mes) OVER w AS fim_fatia,
           LAST(p_est, TRUE) OVER wa AS p_est, LAST(p_emp, TRUE) OVER wa AS p_emp, LAST(p_sim, TRUE) OVER wa AS p_sim
    FROM por_mes
    WINDOW w  AS (PARTITION BY cnpj_basico ORDER BY mes),
           wa AS (PARTITION BY cnpj_basico ORDER BY mes ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
fatias AS (   -- versão vigente no início da fatia: valid_since <= inicio < valid_until (mesma regra de 235–268)
    SELECT cnpj_basico, inicio_fatia, fim_fatia,
           IF(p_est.vu > inicio_fatia, p_est, NULL) AS p_est,
           IF(p_emp.vu > inicio_fatia, p_emp, NULL) AS p_emp,
           IF(p_sim.vu > inicio_fatia, p_sim, NULL) AS p_sim
    FROM preenchido WHERE fim_fatia IS NOT NULL
),
unificado_raw AS (   -- joins de IGUALDADE por (pk, valid_since); matriz INNER, as outras LEFT, como hoje
    SELECT est.*, f.inicio_fatia AS valid_since_final, f.fim_fatia AS valid_until_final,
           (emp.pk IS NOT NULL) AS possui_cadastro_empresa, emp.razao_social, ..., sim.opcao_pelo_mei_bool, ...
    FROM fatias f
    JOIN est_matriz est ON est.pk = f.p_est.pk AND est.valid_since = f.p_est.vs
    LEFT JOIN emp_limpa emp ON emp.pk = f.p_emp.pk AND emp.valid_since = f.p_emp.vs
    LEFT JOIN sim_limpa sim ON sim.pk = f.p_sim.pk AND sim.valid_since = f.p_sim.vs
)
-- dim_enriquecida, ghost_row e o INSERT OVERWRITE: sem mudança.
```

(Em `emp_limpa`/`est_matriz`/`sim_limpa` `valid_until` já vem com `COALESCE(..., '9999-12')`, como hoje; o
marcador `'9999-12'` não abre fatia porque é a última fronteira — mesma regra do `LEAD` atual.)

Por que é igual: as fronteiras e as fatias são as mesmas (mesmo conjunto de meses por CNPJ, mesmo `LEAD`). Sem
sobreposição de versões numa fonte, "a versão de maior início ≤ início da fatia, se ainda vigente" é exatamente "a
versão com `valid_since <= início < valid_until`". **Diferença de comportamento a cobrir:** hoje, se uma fonte tiver
versões sobrepostas, a fatia duplica e o DQ de colisão de SK acusa (D-09); com o preenchimento, o `MAX` escolheria uma
em silêncio. Por isso entra antes do `INSERT` uma asserção explícita de não sobreposição por `cnpj_basico` em
`emp_limpa` e `sim_limpa` (a matriz canônica não se sobrepõe por construção) — T-D2.

Ganho: as três junções passam a ser de igualdade sobre chave quase única (AQE trata bem; sem explosão por intervalo) e
o lado largo de cada fonte é embaralhado **uma** vez; a linha do tempo, que é o que vai para janela e ordenação,
carrega só ponteiros. **Variante B** (a comparar só se a A não tirar o spill): carregar os atributos largos dentro dos
structs e eliminar os três joins — um shuffle só, porém largo.

**Opcional (decisão U4):** coluna nova `inicio_vigencia_empresa` (primeira aparição do CNPJ, `MIN(valid_since_final)
OVER (PARTITION BY cnpj_basico)`, barata aqui porque a janela por CNPJ já existe) no fim da dimensão. Serve três
leitores que hoje refazem o `GROUP BY`/`ROW_NUMBER` na dim inteira: `fato_empresa_mes.primeira_aparicao`,
`fato_mudanca_estado.primeira_versao_empresa` e o DQ 2 da gold.

### 3.5 Dashboard — tabelas gold pré-agregadas, mesmos números por construção

Princípio: **o dataset do Lakeview passa a ler uma tabela pequena, já agregada, gravada pelo Job.** Duas famílias:

**(i) Só somas (`SUM`)** — `mercado`, `mercado_bairros`, `eventos`, `baixas_idade`, `baixas_motivo`. Soma é
decomponível: agregar antes e somar de novo dá o mesmo número para qualquer filtro sobre colunas do grão.

| Tabela nova (gold) | Grão | Carga | Dataset passa a ser |
|---|---|---|---|
| `dash_mercado` | uf, municipio, bairro (normalizado com a expressão `BAIRRO`), porte_empresa, grande_area_cnae, nome_cnae | `CREATE OR REPLACE` a partir de `dashboard_mercado_ativo` (snapshot), `CLUSTER BY (uf, municipio)` | `SELECT * FROM gold.dash_mercado` |
| `dash_mercado_bairros` | mesmo SQL de hoje (top 20 bairros por município × CNAE) | idem | `SELECT * FROM ...` |
| `dash_mercado_empresas` | as 1.000 maiores com `exibir_razao_social` (mesmo SQL) | idem | `SELECT * FROM ...` |
| `dash_eventos_mes` | mes_referencia, tipo_evento, uf, porte_empresa, grande_area_cnae | `REPLACE WHERE` da janela, a partir da OBT | `SELECT tipo_evento, uf, porte_empresa, grande_area_cnae, SUM(qtd_eventos) ... GROUP BY ALL` (sem mês) |
| `dash_baixas_idade_mes` | mes_referencia, uf, porte, área, faixa_idade_baixa | `REPLACE WHERE`, a partir do fato | idem, sem mês |
| `dash_baixas_motivo_mes` | mes_referencia, uf, porte, área, categoria_macro_evento | `REPLACE WHERE`, fato × OBT da janela (mesmo SQL de hoje com filtro de janela) | idem, sem mês |

`mercado` continua com ~6,7 mi linhas (o grão é o próprio cruzamento dos quatro filtros com porte e área; `grande_area`
depende do CNAE, então tirá-la não reduz linhas). Continua "grande" para o Lakeview (consulta no warehouse a cada
filtro), mas a consulta vira leitura de tabela agrupada por UF/município em vez de `GROUP BY` de 23 mi de linhas. Não
cresce com os meses. Reduzir mais exige perder o cruzamento bairro × CNAE (decisão U6; recomendação: não perder).

As colunas `data_referencia` saem de `eventos`/`baixas_idade`/`baixas_motivo` porque nenhum widget as usa (§2.6); o
`mes_referencia` fica **na tabela** (para a carga por `REPLACE WHERE` e para o filtro `SEM_CARGA_INICIAL`, quando
aplicável), não no dataset.

**(ii) Médias e mediana** — `fluxos_empresa`. `AVG` de indicador 0/100 e `PERCENTILE` não são decomponíveis: a média
de médias de grupos não é a média geral, e a mediana de uma tabela agregada não é a mediana das empresas. O Lakeview
já mostrou erro silencioso com expressão composta no campo do widget (backlog, execução de 2026-11-14), então a
proposta **não depende** de recurso novo do Lakeview:

- Tabela `dash_fluxos_mes` com grão `(mes_referencia, uf, porte_empresa, grande_area_cnae, fluxo, teve_falencia_mes,
  idade_na_baixa_meses)` e `qtd = COUNT(*)` — exatamente as colunas de que as colunas do dataset atual são função
  (`pct_mei_*` dependem de porte e fluxo; `pct_falencia_baixa` de `teve_falencia_mes`; `idade_baixa_anos` e
  `pct_baixa_menos_2_anos` de `idade_na_baixa_meses`). Carga por `REPLACE WHERE` da janela. Tamanho esperado
  (**a medir**): aberturas ~1,3 mil combinações/mês; baixas × idade em meses × falência, dezenas de milhares/mês.
- O dataset **refaz as linhas** a partir das contagens e devolve exatamente o multiconjunto de linhas de hoje:

```sql
WITH agregado AS (   -- soma os meses (os widgets não usam mês) e respeita a regra do baseline
    SELECT uf, porte_empresa, grande_area_cnae, fluxo, teve_falencia_mes, idade_na_baixa_meses, SUM(qtd) AS qtd
    FROM tcc_receita.gold.dash_fluxos_mes
    WHERE mes_referencia > (SELECT MIN(mes_referencia) FROM tcc_receita.gold.fato_empresa_mes)
    GROUP BY ALL
)
SELECT uf, porte_empresa, grande_area_cnae, fluxo, 1 AS qtd,
       CASE WHEN fluxo = 'Aberturas' THEN IF(porte_empresa = 'MEI', 100, 0) END AS pct_mei_abertura,
       CASE WHEN fluxo = 'Baixas'    THEN IF(porte_empresa = 'MEI', 100, 0) END AS pct_mei_baixa,
       CASE WHEN fluxo = 'Baixas'    THEN IF(teve_falencia_mes, 100, 0) END     AS pct_falencia_baixa,
       CASE WHEN fluxo = 'Baixas'    THEN idade_na_baixa_meses / 12.0 END      AS idade_baixa_anos,
       CASE WHEN fluxo = 'Baixas' AND idade_na_baixa_meses IS NOT NULL
            THEN IF(idade_na_baixa_meses < 24, 100, 0) END                     AS pct_baixa_menos_2_anos
FROM agregado
LATERAL VIEW EXPLODE(SEQUENCE(1L, qtd)) r AS _rep   -- uma linha por empresa, como hoje
```

Como o dataset continua com milhões de linhas, o Lakeview faz filtros e agregações **no warehouse** — mas cada clique
agora lê uma tabela de dezenas de milhares de linhas e expande na memória, em vez de varrer o fato de todos os meses.
O número de cada widget é idêntico por construção (mesmas linhas). `mes_referencia`/`data_referencia` saem do dataset
(não são usados por widget; a lista `COLUNAS` do gerador é atualizada).

**Variante "pequena" (decisão U2, não recomendada agora):** dataset sem `EXPLODE`, com `SUM` de numerador e
denominador e as razões como *custom calculations* do dataset (recurso documentado:
[docs: custom calculations](https://docs.databricks.com/aws/en/dashboards/manage/data-modeling/custom-calculations),
exemplo `try_divide(SUM(a), SUM(b))`, respeita filtros). Ficaria < 100 mil linhas e rodaria no navegador. Mas: (1) a
mediana continuaria precisando do `EXPLODE` (não há mediana ponderada); (2) é o mesmo tipo de expressão que já falhou
calada no widget — exigiria provar na prévia que a *custom calculation* (definida no dataset, não no campo do widget)
calcula certo, e o `validar()` do gerador teria de aprender o formato JSON delas.

**Comum às tabelas novas:** um arquivo `src/gold/dashboard_agregados.sql` (um `CREATE`/`INSERT` por tabela),
registrado em `SQL_INGEST` do `gerar_job.py` (o gerador não detecta `.sql` novo — C-24), depois de `fato_empresa_mes`,
`obt_dashboard_eventos` e `dashboard_mercado_ativo`. Nada de CNPJ, razão social ou endereço além de
município/bairro nas tabelas agregadas (LGPD); `dash_mercado_empresas` mantém o filtro `exibir_razao_social`. Tabelas
com `CLUSTER BY` recebem só `OPTIMIZE` simples.

---

## 4. Invariantes (o que não pode mudar) e como cada uma se mantém

| # | Invariante | Onde é verificada | Como se mantém no desenho novo |
|---|---|---|---|
| I1 | Identidade de estoque: `ativa_fim = ativa_inicio + entrou − saiu`, linha a linha (fora do baseline) | DQ gold 2 (1) | as flags são calculadas pelas mesmas expressões (`estado_mes` → `fluxos_mes` intactos); só muda de onde vêm `situacao_inicio_mes` e o porte anterior (Corte B, provado igual) |
| I2 | Continuidade: `estoque_inicial(M) = estoque_final(M−1)` | DQ gold 2 (2) | `ativa_inicio_mes` de M vem da **dim** (versão que cobre M−1), nunca do fato do bloco anterior → vale entre blocos |
| I3 | Aberturas = primeiras aparições na dim (olha para trás no histórico inteiro) | DQ gold 2 | `primeira_aparicao` continua sobre a dim inteira (ou a coluna U4) |
| I4 | D-17: primeira aparição pós-baseline já BAIXADA/INAPTA/SUSPENSA = abertura **e** evento | DQ gold 2 (`primeira_aparicao_sem_evento`) | versão não ATIVA nova gera a sua linha no mês de início (Corte A) com `eh_primeira_versao` |
| I5 | D-17b: porte MEI pelas datas do Simples, só versões do Simples com `valid_since <= mês da aparição` (não olha o futuro) | DQ gold 2c | `mei_na_baixa` intacto; o join só sobe para o nível de versão (Corte C) |
| I6 | B23: porte/MEI/Simples de M−1 na saída do estoque | — (coberto pela comparação) | vem da versão anterior contígua (Corte B) |
| I7 | Baseline sem fluxo; sem eventos no log | DQ gold 2 (5); DQ log | baseline = `MIN` da própria tabela, como hoje; versões do baseline nunca entram no `LAG` do log |
| I8 | Dois relógios (D-13): `mes_referencia` = detecção ∈ janela; `data_evento` só da Receita | DQ log | o log continua usando `valid_since` como mês e as mesmas colunas de data |
| I9 | Unicidade `(cnpj_raiz, mes_referencia)` no fato; SK sem colisão na dim | DQ gold 2 (3); `dq_dim_empresa` | Corte A produz no máximo uma linha por versão não ATIVA; fatias iguais às de hoje (+ asserção de não sobreposição, T-D2) |
| I10 | Fato × OBT: baixas de matriz fecham (≤ 0,5%/mês) | DQ gold 2b | nem fato nem OBT mudam de resultado |
| I11 | Painel = somas do fato | DQ gold 3 | painel incremental lê os mesmos meses do fato que substitui |
| I12 | Cada widget do dashboard mostra o mesmo número para qualquer combinação de filtros | §5.3 | (i) somas sobre o mesmo grão; (ii) mesmas linhas por `EXPLODE` |
| I13 | Contiguidade das SCD2 (suposição já existente no log) | **nova**, T-L1 | asserção falha o Job se houver buraco |

**Colunas que olham para trás ou para frente no tempo** e o que acontece em blocos:

- `ativa_inicio_mes`, `situacao_inicio_mes`, `teve_reativacao_mes`, porte de saída (B23): olham **um mês para trás**,
  sempre na dim (rebuild total). Bloco novo não depende de linhas do fato de blocos anteriores.
- `eh_primeira_aparicao`, `EMPRESA NASCEU`, `ENTROU NO MEI/SIMPLES/QSA` de primeira versão: olham **o histórico
  inteiro para trás** (primeira versão). Estáveis enquanto a silver anterior a M não mudar.
- `idade_na_baixa_meses`, `idade_meses`: datas da própria versão; nada no tempo.
- `mei_na_baixa`: olha para trás (Simples até o mês da aparição).
- `data_ultimo_mes_dim` (último snapshot): é o único "olhar para frente" — limita a expansão ao último snapshot carregado.
  Um bloco nunca gera mês posterior ao último snapshot; o bloco seguinte gera os seus.
- `fim_vigencia` (fecha quando surge versão nova): muda em blocos futuros, mas o fato não o grava; `sk_empresa` =
  hash(cnpj|início), estável quando a versão é fechada depois.

**Reprocessar um mês do meio (M):**

- Só a gold, com a silver igual: o mês M é função pura da dim e do Simples até M → sai idêntico; os outros meses
  ficam intactos (`REPLACE WHERE` só de M). O mesmo vale para o log, o painel e as tabelas `dash_*_mes`.
- A silver de M mudou (SCD2 reprocessadas a partir de M): as versões que começam em M mudam, e com elas a "versão
  anterior" de M+1, a primeira aparição e os ponteiros de `sk_empresa` dos meses seguintes. **Regra: reprocessar o
  log, a dim, o fato, o painel e os `dash_*_mes` de M até o último mês carregado** — não só M. Isso já é verdade hoje;
  esta spec só torna explícito, e a fase 4 (blocos) tem de obedecer.

---

## 5. Plano de validação

### 5.1 Ambiente e preservação

- Tudo na amostra de 1% (`tcc_receita.amostra_*`), com `scripts/amostra/rodar_no_warehouse.py --arquivos ...`. A base
  real não é tocada até o §5.5.
- Antes de cada comparação, guardar a versão atual da tabela de saída na amostra:
  `CREATE OR REPLACE TABLE tcc_receita.amostra_<camada>.<tabela>__antes DEEP CLONE tcc_receita.amostra_<camada>.<tabela>`
  (amostra é pequena; deep clone é a opção mais simples e independente de time travel). Depois rodar o SQL novo, que
  sobrescreve só a tabela da amostra, e comparar com `__antes`. Ao final, apagar os `__antes`.
- As comparações rodam com **as mesmas SCD2 de entrada** dos dois lados. Se a fase 2 trocar as SCD2 da amostra no
  meio, refazer o `__antes` antes de comparar.

### 5.2 Equivalência linha a linha (tabelas)

Para cada tabela, `EXCEPT ALL` nos dois sentidos, fora as colunas de carimbo de tempo (`gerado_em`,
`data_carga_etl`); resultado esperado: **0 e 0**.

```sql
WITH novo AS (SELECT * EXCEPT (gerado_em) FROM tcc_receita.amostra_silver_intermediate.log_eventos_negocio),
     antes AS (SELECT * EXCEPT (gerado_em) FROM tcc_receita.amostra_silver_intermediate.log_eventos_negocio__antes)
SELECT (SELECT COUNT(*) FROM (SELECT * FROM novo EXCEPT ALL SELECT * FROM antes)) AS so_no_novo,
       (SELECT COUNT(*) FROM (SELECT * FROM antes EXCEPT ALL SELECT * FROM novo)) AS so_no_antes
```

(`SELECT * EXCEPT (col)` existe no Databricks SQL e no DBR 15.4.) Mesmo molde para `fato_mudanca_estado`,
`dim_empresa`, `fato_empresa_mes`, `fato_indicadores_setoriais_mes`, `obt_dashboard_eventos` (que muda só se a
entrada mudar — serve de teste de regressão da cadeia).

Testes específicos:

1. **Log, cenários de janela:** (a) janela inteira 2023-05..08 (inclui o baseline); (b) só 2023-07..07 — comparar
   com o recorte de julho do `__antes`; (c) dois blocos (05..06 e depois 07..08) contra uma execução única.
2. **Fato, blocos:** idem (a)/(b)/(c). (b) é o "reprocessar um mês do meio": julho sozinho tem de sair igual e
   junho/agosto não podem mudar (comparar a tabela inteira).
3. **Dim:** comparação de todas as colunas + `nk_empresa` único + nenhuma fatia a mais/a menos por CNPJ.
4. **Com U1:** além do `EXCEPT` do log (que agora tem uma coluna a mais — comparar sem ela), conferir
   `cnpj_basico` do log = `cnpj_basico` da SCD2 de origem para toda `pk` (0 divergências) e que nenhuma `pk` mapeia
   para dois `cnpj_basico` (se mapear, hoje o fato duplica a linha; registrar e decidir antes de seguir).
5. **DQ:** `--etapas si,sf,gold` completo passando, com as asserções novas (T-L1, T-D2).

### 5.3 Equivalência do dashboard (widget a widget)

Script novo `scripts/amostra/comparar_widgets.py` (ou extensão do `testar_datasets.py` usado em 2026-11-15): para
cada widget das páginas 2, 4 e 5, monta a consulta que o Lakeview faria — `SELECT <agregação> FROM (<SQL do
dataset>) WHERE <filtros> GROUP BY <categoria>` — com o SQL **antigo** e o **novo** do dataset, e compara os
resultados (igualdade exata em contagens e somas; `AVG`/`PERCENTILE` com tolerância 1e-9). Cenários de filtro por
página:

- P2 e P4: nenhum; `uf = 'SP'`; `uf IN ('RJ','MG')`; `porte_empresa = 'MEI'` (P4); `grande_area_cnae = 'COMERCIO'`;
  combinação UF + porte + área.
- P5: nenhum; UF; UF + município (o maior da UF); UF + município + bairro (o maior); + CNAE (o mais frequente do
  bairro); só CNAE.
- Para cada filtro, também a lista de valores do dropdown (`SELECT DISTINCT campo FROM (<dataset>)`) antigo × novo.

Depois, publicar a prévia com `scripts/amostra/publicar_dashboard_amostra.py --id <prévia>` e o autor confere na tela
as páginas 2, 4 e 5 (é a mesma prova, agora visual; o dashboard oficial não é tocado).

### 5.4 Consultas de diagnóstico pendentes (rodar quando o CLI reautenticar; em lote, warehouse 2X-Small)

```sql
-- contiguidade das SCD2 (I13): esperado 0 em cada tabela
SELECT COUNT(*) FROM (SELECT valid_until, LEAD(valid_since) OVER (PARTITION BY pk ORDER BY valid_since) AS prox
                      FROM tcc_receita.silver_intermediate.<scd2>) WHERE prox IS NOT NULL AND valid_until <> prox;
-- versões por valid_since e fração que começa no baseline e nunca fecha (ganho da Ideia 1)
SELECT valid_since, COUNT(*), COUNT_IF(valid_until IS NULL) FROM tcc_receita.silver_intermediate.<scd2> GROUP BY 1;
-- CNPJs com mais de uma ordem marcada matriz (tamanho de `multi`, §3.4a)
SELECT COUNT(*) FROM (SELECT cnpj_basico FROM tcc_receita.silver_intermediate.estabelecimentos
                      WHERE identificador_matriz_filial = '1' GROUP BY 1 HAVING COUNT(DISTINCT cnpj_ordem) > 1);
-- dim: versões correntes não ATIVA (o que o Corte A deixa de expandir)
SELECT inicio_vigencia, COUNT(*), COUNT_IF(fim_vigencia IS NULL AND situacao_cadastral_matriz <> 'ATIVA')
FROM tcc_receita.silver_final.dim_empresa GROUP BY 1;
-- tamanho de dash_fluxos_mes por mês (grão proposto)
SELECT mes_referencia, COUNT(*) FROM (SELECT DISTINCT mes_referencia, uf, porte_empresa, grande_area_cnae,
       teve_abertura_mes, teve_baixa_mes, teve_falencia_mes, idade_na_baixa_meses
       FROM tcc_receita.gold.fato_empresa_mes WHERE teve_abertura_mes OR teve_baixa_mes) GROUP BY 1;
-- bairros distintos no filtro da P5 (limite de 100 mil valores do dropdown)
SELECT COUNT(DISTINCT COALESCE(NULLIF(TRIM(bairro), ''), 'NAO INFORMADO')) FROM tcc_receita.gold.dashboard_mercado_ativo;
-- d_before casando duas versões (observação §2.2)
SELECT COUNT(*) FROM (SELECT pk_entidade_origem, mes_referencia, sk_mudanca_estado, data_evento
                      FROM tcc_receita.silver_final.fato_mudanca_estado
                      GROUP BY ALL HAVING COUNT(DISTINCT sk_empresa_anterior) > 1);
-- DESCRIBE DETAIL das SCD2, da dim e do fato (partição/clustering, nº de arquivos)
```

E no cluster (não no warehouse, que usa Photon e outro otimizador): `EXPLAIN FORMATTED` do log atual e do novo,
contando `Scan` e `Exchange` por tabela — é a prova de "uma leitura por SCD2". Pode ir num notebook junto com a
medição da §5.5, sem subida extra de cluster.

### 5.5 O que medir na base real (uma execução de 4 meses, ao fim das fases 2 e 3)

Por tarefa (event log / Spark UI; duração também por `system.lakeflow.job_task_run_timeline`): duração, bytes lidos,
shuffle read/write, spill, número de stages. Comparar com a linha de base (B20 / fase 0):

| Tarefa | Linha de base | Alvo desta spec |
|---|---|---|
| `si_log_eventos_negocio` | 873 s | ≤ 241 s (o tempo antes da nova semântica) |
| `sf_fato_mudanca_estado` | 1,7× o anterior | ≤ tempo anterior à nova semântica |
| `sf_dim_empresa` | spill 7,6 GB | spill 0 |
| `gold_fato_empresa_mes` | 84 GB shuffle | shuffle ≤ ~2× o tamanho da saída escrita |
| `gold_fato_indicadores_setoriais_mes` | (medir) | proporcional aos meses do bloco, não ao histórico |

Dashboard: no `system.query.history` (ou na tela de histórico do warehouse), tempo das consultas dos datasets das
páginas 2, 4 e 5 com e sem filtro, cache frio (primeira abertura depois da carga). E, já no piloto de 12 meses da fase
4: as tabelas `dash_*` não crescem além do previsto e o dashboard abre sem corte (critério do guarda-chuva).

---

## 6. Ganho esperado e riscos

| Mudança | Ganho (qualitativo) | Por quê | Risco |
|---|---|---|---|
| Log, Ideias 1+2 | **alto** (o maior da fase) | o shuffle passa de "histórico × nº de ramos" para "versões que mudaram no bloco × 1"; na carga inicial também, porque as versões do baseline que nunca mudaram saem | baixo: equivalência depende da contiguidade → asserção T-L1; erro de tipo nos structs aparece na primeira execução |
| Log, Ideia 3 | médio | o self-join de todas as matrizes vira self-join só dos CNPJs com matriz mudando na janela | baixo; candidatos podem ser milhões num bloco de 12 meses (é shuffle estreito) |
| U1 (`cnpj_basico` no log) | médio no `fato_mudanca_estado` | some a releitura+dedup das quatro SCD2 | schema do log muda (coluna no fim; `REPLACE WHERE` não migra sozinho → migração) |
| Fato, Cortes A+B+C | **alto** | some a expansão das mortas (na carga inicial, ~120 mi de linhas) e o join depois da expansão; o `LAG` sobre ~56 mi de linhas largas vira join de dois conjuntos pequenos | médio: três caminhos (`novas`/`antigas`/não ATIVA) é mais código para revisar; coberto pelo `EXCEPT` e pelos DQ da gold |
| Painel incremental | médio agora, **alto** em 40 meses | de ~900 mi linhas por bloco para só os meses do bloco | revisão de dado macro não propaga sozinha (U5) |
| Optimized write off (U3) | médio, condicional | tira um shuffle do tamanho da saída | arquivos pequenos; mitigado por `OPTIMIZE` simples |
| Dim, (a)+(b) | médio; deve zerar o spill | linhas largas embaralhadas uma vez, joins de igualdade | (b) troca "duplica e o DQ acusa" por "escolhe um" → asserção T-D2 obrigatória |
| Dashboard, tabelas `dash_*` | **alto** na experiência; pequeno no Job | cada clique lê tabela pequena em vez de varrer fato/OBT/mercado; tamanho não cresce com os meses (exceto o `dash_*_mes`, que cresce pouco) | `fluxos_empresa` continua "grande" no Lakeview (backend), só que barato; filtro de bairro pode passar de 100 mil valores (a medir) |

Riscos transversais: (1) a fase 2 pode mudar a ordem/tipo físico das colunas SCD2 (não a semântica) — os
esqueletos usam nomes de coluna, não posição; (2) a amostra roda no warehouse (Photon, Spark mais novo) e não pega
incompatibilidade com o 15.4 — por isso a checagem automática da fase 1 (item 2 do guarda-chuva) deve existir antes
da execução real, e os esqueletos aqui evitam qualquer função fora do Spark 3.5; (3) desempenho só é provado na base
real — a amostra prova igualdade, não velocidade.

---

## 7. Tarefas (pequenas, independentes, com aceite)

Legenda de dependência: **F2** = depende da fase 2 estar mesclada (só para rodar a comparação final com as SCD2
novas; o código não depende). Todas as tarefas seguem o fluxo do `CLAUDE.md` (commit atômico próprio, backlog).

**Log (silver_intermediate)**

- [ ] **T-L1 — Asserção de contiguidade.** `ASSERT_TRUE` em `src/silver_intermediate/tests/dq_<scd2>.sql` (ou no
  início do log, decidir pelo custo): nenhuma `pk` com `valid_until` ≠ próximo `valid_since`.
  *Aceite:* passa na amostra; falha num teste com uma linha adulterada numa cópia (não na tabela real).
- [ ] **T-L2 — Ideias 1+2 (estabelecimentos, simples, sócios, empresas).** *Aceite:* `EXCEPT ALL` 0/0 nos cenários
  (a)(b)(c) do §5.2; `EXPLAIN` no cluster com 1 `Scan` por SCD2 nesses ramos. Depende de T-L1.
- [ ] **T-L3 — Ideia 3 (nascimento por candidatos).** *Aceite:* `EXCEPT ALL` 0/0 só nos eventos `EMPRESA NASCEU`
  e no log inteiro.
- [ ] **T-L4 — (se U1) `cnpj_basico` no log.** DDL com coluna no fim + script em `src/jobs/migracoes/` (ou recriação,
  D-08); atualizar `dq_log_eventos_negocio.sql` (não nulo, igual à origem). *Aceite:* teste 4 do §5.2.
- [ ] **T-L5 — `fato_mudanca_estado` lendo só o necessário (§3.2).** *Aceite:* `EXCEPT ALL` 0/0;
  `dq_fato_mudanca_estado` passa. Depende de T-L4 se U1.

**Dimensão (silver_final)**

- [ ] **T-D1 — Matriz canônica estreita (§3.4a).** *Aceite:* `EXCEPT ALL` 0/0 na `dim_empresa`.
- [ ] **T-D2 — Asserção de não sobreposição** em `emp_limpa`/`sim_limpa` por `cnpj_basico`, antes do `INSERT`.
  *Aceite:* passa na amostra.
- [ ] **T-D3 — Fatias por ponteiros (§3.4b, variante A).** *Aceite:* `EXCEPT ALL` 0/0; `dq_dim_empresa` passa.
  Depende de T-D2. Variante B só se a medição real ainda mostrar spill.
- [ ] **T-D4 — (se U4) `inicio_vigencia_empresa`** e uso nos três leitores. *Aceite:* `EXCEPT ALL` 0/0 nas tabelas
  leitoras; a coluna nova = `MIN(inicio_vigencia)` por CNPJ (consulta de conferência = 0 divergências).

**Fato e painel (gold)**

- [ ] **T-F1 — Cortes A+B+C em `fato_empresa_mes`.** *Aceite:* `EXCEPT ALL` 0/0 nos cenários (a)(b)(c); DQ gold
  inteiro passa (identidade, continuidade, D-17, D-17b, 2b).
- [ ] **T-F2 — Painel incremental.** Migração (DROP único) + `REPLACE WHERE`. *Aceite:* `EXCEPT ALL` 0/0 contra o
  painel reconstruído do zero; DQ gold 3 passa; rodar 05..06 e depois 07..08 = rodar 05..08.
- [ ] **T-F3 — (se U3) optimized write desligado no fato** + `OPTIMIZE` ao fim. Só na base real; *aceite:* shuffle da
  tarefa cai e o nº de arquivos por partição fica registrado.

**Dashboard**

- [ ] **T-B1 — `src/gold/dashboard_agregados.sql`** com as tabelas da §3.5 (família i) + registro em `SQL_INGEST` e
  regeneração do YAML (`python -m src.jobs.gerar_job`). *Aceite:* tabelas criadas na amostra; somas por mês das
  `dash_*_mes` = as somas correspondentes no fato/OBT (consulta de reconciliação).
- [ ] **T-B2 — `dash_fluxos_mes` + dataset com `EXPLODE`** (família ii). *Aceite:* o dataset novo devolve o mesmo
  multiconjunto de linhas que o antigo, sem as colunas de mês (`EXCEPT ALL` 0/0 projetando as colunas comuns).
- [ ] **T-B3 — Gerador do dashboard aponta para as tabelas novas** (`DATASETS`, `COLUNAS`, `validar()`), JSON
  regenerado. *Aceite:* `validar()` ok; `comparar_widgets.py` (§5.3) sem diferença em todos os cenários.
- [ ] **T-B4 — Prévia publicada e conferida pelo autor;** só então republicar o oficial. *Aceite:* autor aprova.
- [ ] **T-B5 — DQ das tabelas `dash_*`** em `dq_gold.sql`: soma de `dash_fluxos_mes` por mês = aberturas + baixas
  do fato; `dash_mercado` soma = `COUNT(*)` de `dashboard_mercado_ativo`.

**Fechamento**

- [ ] **T-X1 — Rodada de equivalência completa na amostra** (`--etapas si,sf,gold`) **com as SCD2 da fase 2** (F2).
- [ ] **T-X2 — Execução de 4 meses na base real e medição** (§5.5), junto com a da fase 2 (uma subida de cluster).
- [ ] **T-X3 — Docs:** `docs/specs/<camada>.md` afetadas, `docs/backlog.md` (fecha o P1 do log e do dashboard; abre
  o que a §5.4 encontrar), `docs/decisions.md` (registro das decisões U1–U6 aprovadas), `docs/tcc.md` se o texto do
  TCC descreve a construção do log/fato (sugestão, nunca edição do repo do TCC).

Ordem sugerida: T-L1 → T-L2 → T-L3 → (T-L4 → T-L5) | em paralelo: T-F1 → T-F2 | T-D2 → T-D1 → T-D3 | T-B1 → T-B2 →
T-B3 → T-B4 → T-B5; por fim T-X1 → T-X2 → T-X3. Nenhuma tarefa depende de outra de um grupo diferente, exceto T-L5
(de T-L4) e T-D4 (opcional).

---

## 8. Decisões para o autor

| ID | Pergunta | Opções | Recomendação |
|---|---|---|---|
| U1 | Gravar `cnpj_basico` no log de eventos? | (a) sim, coluna nova no fim; (b) não, filtrar o `mapeamento_pk` por semi-join | **(a)**: elimina a releitura das quatro SCD2 no `fato_mudanca_estado`; não é extrair CNPJ da `pk`; custo = uma migração |
| U2 | Datasets de percentuais/mediana: refazer as linhas por `EXPLODE` ou usar *custom calculations*? | (a) `EXPLODE` (mesmas linhas, números iguais por construção, consultas no warehouse); (b) *custom calculations* (dataset pequeno no navegador, precisa provar que o Lakeview calcula certo) | **(a)** agora; (b) só se a prévia mostrar lentidão com (a) |
| U3 | Desligar o optimized write no fato? | (a) sim, com `OPTIMIZE` ao fim; (b) não | decidir **depois** da medição real; se o shuffle da escrita for > ~30% do total da tarefa, (a) |
| U4 | Coluna `inicio_vigencia_empresa` na `dim_empresa`? | sim / não | **sim**, se T-F1 e T-L5 ainda mostrarem o `GROUP BY` da dim entre os passos caros; senão não mexer |
| U5 | Painel incremental (revisão de Selic/IPCA de mês antigo só entra ao reprocessar aquele mês)? | (a) incremental; (b) rebuild total | **(a)**: a série macro do BCB/IBGE quase não revisa; o custo do (b) cresce com os 40 meses |
| U6 | Trocar algum filtro cruzado da página "Mercado ativo" por tamanho? Ex.: separar bairro e CNAE em datasets diferentes (filtro de CNAE deixaria de valer no gráfico de bairros) | (a) manter os quatro filtros cruzados, materializando; (b) perder o cruzamento | **(a)**: o dataset não cresce com os meses e a materialização já tira o custo; perder o cruzamento empobrece a pergunta da página |
| U7 | Se o filtro de bairro passar de 100 mil valores distintos (limite do dropdown) | (a) aceitar (com UF/município escolhidos a lista é curta); (b) bairro como texto livre/busca | medir primeiro (§5.4); se passar, **(a)** com nota na página |

Nenhuma decisão muda número do dashboard; U2(b) e U6(b) mudam como o número é calculado ou o que o filtro alcança.

---

## 9. Fontes externas

- Limites do AI/BI dashboard: <https://docs.databricks.com/aws/en/dashboards/limits>
- Processamento de datasets pequenos × grandes e cache: <https://docs.databricks.com/aws/en/dashboards/caching>
- Materialização de datasets (Beta, serverless): <https://docs.databricks.com/aws/en/dashboards/materialization>
- *Custom calculations*: <https://docs.databricks.com/aws/en/dashboards/manage/data-modeling/custom-calculations>
- `REPLACE WHERE` e versões de runtime: <https://docs.databricks.com/aws/en/delta/selective-overwrite>
- Range join (tipos suportados): <https://docs.databricks.com/aws/en/optimizations/range-join>

## 10. Notas de execução

- 2026-11-16: spec escrita a partir do código; consultas da §5.4 não rodaram (CLI sem autenticação). Nenhum código de
  produção alterado.
