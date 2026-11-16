# Revisão adversarial das specs de otimização (fases 2 e 3)

Revisão de 2026-11-16, feita sem acesso ao Databricks (só código e specs). Specs revisadas:
[`2026-11-16-otimizacao-scd2.md`](2026-11-16-otimizacao-scd2.md) (chamada aqui **F2**),
[`2026-11-16-otimizacao-log-fato-dashboard.md`](2026-11-16-otimizacao-log-fato-dashboard.md) (**F3**) e a
guarda-chuva [`2026-11-16-otimizacao-pipeline.md`](2026-11-16-otimizacao-pipeline.md) (**GC**).

Severidades: **bloqueia** (resultado errado ou perda de dado que a validação planejada não pega) ·
**corrigir antes de implementar** · **observação**. Cada achado foi conferido no código citado.

Resumo: 3 bloqueiam · 12 corrigir antes de implementar · 16 observações.

---

## 1. Erros de equivalência

### R1 — Socios: o mês de contexto (`B_CTX`) não passa pelo desempate do B16 · **bloqueia**
- **Spec:** F2 §3.3 (d) e §3.6 itens 2–3.
- **Evidência:** o `dedup` de F2 (linhas 188–194 da spec) filtra `WHERE origem <> 'B' OR rn = 1`, ou seja, só deduplica
  `origem = 'B'`. Em socios, F2 §3.6 cria `origem = 'B_CTX'` para `mes_ini−1`, e essas linhas não são deduplicadas.
  Hoje o desempate vale também para o mês anterior: `socios.sql:61-73` (o `ROW_NUMBER` cobre
  `[mes_ini−1, mes_fim]`) e `socios.sql:120-122` (`rn = 1` antes de `estados_inativos`). O backlog registra
  ~6 pks repetidas por mês em socios (`docs/backlog.md:78`, B16).
- **Efeito:** com duas linhas `B_CTX` na mesma `(pk, mes)`, o `LEAD(mes)` da §3.6.3 empata no `ORDER BY mes`. Uma das
  linhas recebe `prox_presenca = mes` e a outra a próxima presença real. Qual delas gera a versão inativa, e com qual
  hash, depende da ordem física. O resultado diverge do atual e não é determinístico. A amostra de 1% (sorteio por
  `cnpj_basico`, `criar_amostra_bronze.sql:24`) quase certamente não tem nenhum caso de B16, então os cenários C1–C10
  passam mesmo com o bug.
- **Correção:** deduplicar com `origem NOT IN ('B','B_CTX') OR rn = 1` (ou rotular depois do `dedup`). Acrescentar à
  validação um cenário sintético de B16 (ver R30).

### R2 — Migração do painel e das `dash_*_mes` preenche só a janela, e o DQ não percebe · **bloqueia**
- **Spec:** F3 §3.3 ("Painel incremental", migração por `DROP TABLE` único) e §3.5 (tabelas `dash_*_mes` carregadas por
  `REPLACE WHERE`).
- **Evidência:** hoje o painel é `CREATE OR REPLACE TABLE ... AS` sobre o fato inteiro
  (`fato_indicadores_setoriais_mes.sql:14-44`). Depois do `DROP` e do primeiro `REPLACE WHERE`, ele só terá os meses da
  janela daquela execução. O DQ 3 só olha a janela (`dq_gold.sql:290-309`, `WHERE s.mes_referencia >= p.mes_ini AND ...
  <= p.mes_fim`), então passa. Os datasets `painel`, `estoque_mes`, `top_cnaes` e as taxas leem o painel inteiro
  (`gerar_dashboard.py:161-187`) e mostrariam a série cortada, sem erro nenhum. Com `dash_fluxos_mes`,
  `dash_eventos_mes`, `dash_baixas_*_mes` acontece o mesmo: hoje `fluxos_empresa`, `eventos` etc. leem todos os meses
  (`gerar_dashboard.py:195-265`).
- **Correção:** o passo de migração precisa recarregar `[MIN, MAX]` de `fato_empresa_mes` logo depois do `DROP`/`CREATE`.
  Também falta um DQ que não dependa da janela: os meses distintos do painel e de cada `dash_*_mes` têm de ser iguais aos
  meses distintos do fato.

### R3 — Falha entre o `MERGE` e o `INSERT`: o DQ não detecta o estado intermediário, e rodar outra janela corrompe o histórico · **bloqueia**
- **Spec:** F2 §3.2 (atomicidade), §7.2 ("o DQ ... acusa duas versões correntes") e D3.
- **Evidência:** depois do passo 5 (`AJUSTA_FIM` fecha a versão corrente; `APAGA` remove a substituída), uma pk que
  mudou fica com **zero** versões correntes, e não com duas. `dq_estabelecimentos.sql:10-16` (e os DQ de empresas e
  dados_simples, que seguem o mesmo molde) só testam "mais de uma corrente". Só `dq_socios.sql:19-25` testa "exatamente
  uma". Se a próxima execução for a janela seguinte, e não a mesma, `silver_estreita` exclui a versão fechada
  (`valid_until < mes_ini`). A pk passa a ter um buraco `[M, mes_ini)`, e a bronze abre uma versão que parece a primeira.
  A Ideia 1 de F3 então gera eventos de "primeira aparição" falsos (`eh_primeira_versao`).
- **Correção:** (a) corrigir o texto da §7.2; (b) incluir nos DQ de empresas, estabelecimentos e dados_simples os testes
  `sem_corrente` e de continuidade com `IS DISTINCT FROM` que já existem em `dq_socios.sql:19-42`; (c) na instrução 1,
  falhar se `tmp_scd2_acoes_<t>` ainda existir. Essa tabela só é apagada no passo 7, então a presença dela indica uma
  execução anterior incompleta, e a mensagem deve mandar repetir a mesma janela.

### R4 — O modelo `acoes` da §3.3 grava `h_origem` e `mes_origem` errados para a inativa de socios · **corrigir antes de implementar**
- **Spec:** F2 §3.3 (`acoes`: `h AS h_origem`, `mes AS mes_origem`, `ts AS ts_origem`) × §3.6 itens 4–6.
- **Evidência:** na §3.6, o ponto inativo tem `h = h_inativo` e `mes = M`, mas a busca precisa do prefixo do hash **de
  origem** e do mês **M−1**. `pontos` já projeta colunas `h_origem`/`mes_origem`/`ts_origem` com esses nomes. Se `acoes`
  for copiada da §3.3, essas colunas são recalculadas a partir de `h`/`mes` e a busca não acha a linha da bronze: a
  versão inativa some, sem erro.
- **Correção:** escrever `acoes` de socios por extenso na spec (usar as colunas `*_origem` de `pontos`) e cobrir o caso
  num cenário com saída do QSA nos dois lados da fronteira de bloco (ver R30).

### R5 — Desvio do comportamento atual com mês faltante descrito como se fosse novo · **observação**
- **Spec:** F2 D5 ("com o desenho novo, um mês sem bronze num reprocessamento apagaria as versões dele").
- **Evidência:** hoje, em `estabelecimentos.sql:193-202`, a versão existente de um mês ausente da bronze, de uma pk que
  aparece em outro mês da janela, já entra em `t` sem par em `s` e é apagada (`WHEN MATCHED AND staging.pk IS NULL THEN
  DELETE`). A diferença do desenho novo vale só para pks **sem nenhuma** linha na janela.
- **Correção:** ajustar o texto. A recomendação (a), falhar, continua certa.

### R6 — A observação sobre o `d_before` está errada · **observação**
- **Spec:** F3 §2.2 (último item) e a consulta correspondente na §5.4.
- **Evidência:** `fato_mudanca_estado.sql:122-126` exige `inicio < M` e `fim >= M`. Se duas versões do mesmo CNPJ
  satisfazem as duas condições, elas se sobrepõem em `[max(inicio), M)`, que não é vazio. As versões da dim não se
  sobrepõem: são fatias de `LEAD` (`dim_empresa.sql:224-234`), e duplicata vira colisão de SK, que o DQ pega. Se a empresa
  não mudou de versão em M, só a versão que cobre M casa, porque a anterior termina antes de M.
- **Correção:** retirar a observação, ou reescrevê-la como "só com sobreposição, que o DQ de SK já acusa". A consulta da
  §5.4 pode ficar como sanidade.

---

## 2. Afirmações sobre o código atual que estão erradas ou imprecisas

### R7 — "Filtra a bronze já na leitura" e "custo proporcional ao que mudou" · **corrigir antes de implementar**
- **Spec:** F2 §0, §3.5 e §5 (fórmula de custo).
- **Evidência:** o `LEFT SEMI JOIN` difundido (instrução 6, linhas 331–341 da spec) é um `BroadcastHashJoin` **depois** do
  *scan*. Ele evita a troca de dados, mas não evita ler as colunas largas de todas as linhas dos meses esparsos: a `pk`
  não está ordenada nos arquivos, então as estatísticas de *row group* não descartam nada. Além disso, sem *deletion
  vectors*, o `MERGE` reescreve "≈ a tabela" (a própria §5 diz isso), o que contradiz o §0 ("custo por mês incremental
  proporcional ao que mudou, não ao tamanho da tabela") e a linha da §7.1 ("escreve ~1 M linhas/mês, não ~60 M").
- **Correção:** a fórmula da §5 precisa somar "meses esparsos × leitura larga (sem troca)" e "reescrita dos arquivos
  tocados pelo `MERGE` (≈ a tabela sem DV)". O §0 deve dizer que o custo proporcional ao que mudou vale para a **troca
  de dados**, e para a escrita só com DV.

### R8 — Referências de linha desatualizadas · **observação**
- F2 §2.1 cita `bronze_lib.py:241` (`partitionBy`) e `:161-162` (`pk` não nula). No código atual são `:268` e
  `:188-189` (o commit `4760da3`, da fase 1, deslocou o arquivo).
- F3 §2.2 cita `primeira_versao_empresa (95–105)`, `versoes_pareadas (121–130)` e `DISTINCT final (134–149)`. No
  `fato_mudanca_estado.sql` atual são `88-98`, `99-129` e `131-147`.

### R9 — "O texto de `fato_mudanca_estado` não mudou com a nova semântica" · **observação**
- **Spec:** F3 §2.2.
- **Evidência:** o commit `8066676` (2026-11-14, mesma leva da semântica de eventos) acrescentou `data_evento`, `eh_matriz`
  e `codigo_motivo_norm` a `logs_base`, mais o join `dme.codigo_motivo <=> ...` (`fato_mudanca_estado.sql:59-61, 84`) e
  duas colunas no `DISTINCT` (`:144-145`).
- **Correção:** atribuir a lentidão de 1,7× também ao `DISTINCT` mais largo, ou deixar a causa como "a medir".

### R10 — QUALIFY "proibido" em F2, mas usado e mantido em F3 · **corrigir antes de implementar**
- **Spec:** F2 §3.9 ("Proibido: ... `QUALIFY`") × F3 §3.1, Ideia 3 ("igual a hoje (`QUALIFY ROW_NUMBER ...`)").
- **Evidência:** `log_eventos_negocio.sql:203` usa `QUALIFY`, e essa task rodou no cluster do Job (873 s no B20). O
  `QUALIFY` existe no Databricks Runtime desde muito antes do 15.4. `scripts/checar_sql_cluster.py` não tem regra para ele.
- **Correção:** retirar `QUALIFY` da lista de proibidos da F2. A regra de compatibilidade deve sair só do
  `checar_sql_cluster.py`, para não haver duas listas divergentes.

### R11 — Critério "1 `Scan` por SCD2" contradiz o próprio desenho · **observação**
- **Spec:** F3 T-L2 (aceite) × §3.1.
- **Evidência:** o esqueleto lê `estabelecimentos` três vezes: `MIN(valid_since)` em `params`, `est_rel` e `hist_matriz`
  (Ideia 3). Lê também `empresas` duas vezes: o baseline novo de empresas e `pk_empresa`.
- **Correção:** o critério deve ser "uma leitura com janela (`LAG`) por SCD2, mais leituras de uma coluna (`MIN`) e o
  `hist_matriz`".

---

## 3. Compatibilidade com DBR 15.4 / Spark 3.5 e com o warehouse da amostra

### R12 — U3 (`SET spark.databricks.delta.optimizeWrite.enabled`) quebra o runner da amostra · **corrigir antes de implementar**
- **Spec:** F3 §3.3 "Escrita" e U3 × F2 §3.9 ("Proibido: ... `SET` de configurações que o warehouse recusa").
- **Evidência:** `rodar_no_warehouse.py:51-56` manda cada instrução ao SQL warehouse, que recusa `SET` de configuração do
  Spark que não esteja na lista permitida. O `fato_empresa_mes.sql` roda na amostra (F3 §5.2), então a validação inteira
  da fase 3 deixaria de rodar.
- **Correção:** usar a propriedade de tabela `delta.autoOptimize.optimizeWrite`, que é a mitigação da própria F2 §7.2.
  Antes disso, confirmar se a conf de sessão definida no cluster (`gerar_job.py:87`) tem precedência sobre a
  propriedade de tabela. Se tiver, a única saída é tirar a conf do cluster e ligá-la por tabela.

### R13 — Onde conferir o `EXPLAIN`: F2 diz warehouse, F3 diz cluster · **corrigir antes de implementar**
- **Spec:** F2 §6.1 ("Os `EXPLAIN` da §3.8 também devem ser conferidos no warehouse") × F3 §5.4 ("no cluster (não no
  warehouse, que usa Photon e outro otimizador)").
- **Evidência:** o warehouse roda Photon e um Spark mais novo. A bronze da amostra também é criada por CTAS **sem
  partição** (`criar_amostra_bronze.sql:12-25`), então poda de partição e o `IN (SELECT mes ...)` (R14) não aparecem
  no plano da amostra. As regras da F2 §3.8 (uma troca só; nenhum `Exchange` depois do `Generate`; *broadcast* sem
  `Exchange`) só têm valor no plano do cluster sobre as tabelas reais.
- **Correção:** um único notebook no cluster, sobre as tabelas reais, com o `EXPLAIN FORMATTED` das instruções 2 e 6 da
  F2 e do log de F3, junto com a medição (uma subida de cluster).

### R14 — Poda dinâmica dos ramos "denso" e "esparso" não verificada · **corrigir antes de implementar**
- **Spec:** F2 §3.5, instrução 6 (`WHERE b.mes_referencia IN (SELECT mes FROM tmp_scd2_meses_<t> WHERE denso)` e o
  equivalente para os esparsos).
- **Evidência:** a poda só acontece se o otimizador transformar o `IN (subquery)` em poda dinâmica de partição. Sem
  isso, **cada um dos dois ramos lê a janela inteira com colunas largas**, e a leitura da bronze larga dobra. A §3.8 não
  tem regra sobre isso.
- **Correção:** acrescentar à §3.8 a regra "`PartitionFilters: dynamicpruningexpression(...)` nos dois *scans* da
  bronze". Se não aparecer, juntar os ramos num único *scan* com o filtro
  `denso OR xxhash64(...) IN broadcast` (ou num `JOIN` com `tmp_scd2_meses` difundida).

### R15 — Mensagens de `ASSERT_TRUE` com número sem `CAST` e a armadilha do `_eh_consulta` · **observação**
- **Spec:** F2 instrução 4 (`CONCAT('...', SUM(...), '...')`) e as instruções de validação novas.
- **Evidência:** `dq_gold.sql:5` registra a convenção "Números nas mensagens vão com `CAST(... AS STRING)` para funcionar
  também com ANSI ligado (SQL warehouse)". Além disso, `run_sql.py:94-97` só faz `collect()` de `SELECT`/`WITH` que **não
  contenham** as palavras `INSERT` ou `MERGE`, inclusive dentro de *strings*. Uma mensagem de asserção como "...antes do
  MERGE" faria a asserção não rodar no Job, sem nenhum aviso.
- **Correção:** `CAST` nas mensagens. Proibir `INSERT`/`MERGE` no texto das mensagens, ou corrigir o `_eh_consulta` para
  ignorar *strings* (o `checar_sql_cluster.mascarar` já faz isso).

### R16 — ANSI: nenhuma dependência encontrada no desenho · **observação**
Nenhuma expressão nova depende do ANSI (`TRY_CAST`, `try_to_timestamp`, `xxhash64`, `SEQUENCE(1L, qtd)`). A ressalva é
a de sempre: a igualdade provada na amostra roda com ANSI ligado, e o cluster roda com ANSI desligado. Os cenários
de equivalência não provam o comportamento do cluster com dado sujo, que não existe na amostra. Basta registrar isso na
§6.1 da F2.

---

## 4. Contradições entre as specs e com a guarda-chuva

### R17 — Plano de medição da F2 não é comparável com a linha de base nem com a F3 · **corrigir antes de implementar**
- **Spec:** F2 §6.2 (execuções `2023-05..06`, depois `07`, depois `08`; aceite "2× mais rápida que o B20") × F3 T-X2 e
  GC §2 ("uma execução de 4 meses ao fim das fases 2 e 3").
- **Evidência:** o B20 é **uma** carga inicial `05..08` (`docs/medicoes/2026-11-16-linha-de-base.md`). Comparar
  "2 meses + 1 + 1" com "4 meses de uma vez" mistura carga inicial com incremental, e não existe um número de linha de
  base para um mês incremental.
- **Correção:** medir `05..08` de uma vez (comparável ao B20) e, na mesma subida de cluster, um mês incremental
  (reexecutar `08` depois de apagar as versões de `08`, ou usar `09`, se a bronze já tiver esse mês). As duas specs
  devem descrever a mesma execução.

### R18 — Preservação da referência real por `SHALLOW CLONE` seguido de `DROP` da origem · **corrigir antes de implementar**
- **Spec:** F2 §6.2, itens 1–2.
- **Evidência:** o *shallow clone* aponta para os arquivos da origem. No Unity Catalog, o `DROP` de uma tabela gerenciada
  apaga os arquivos depois do prazo de recuperação (`UNDROP`). A otimização preditiva, presumida em
  `checar_sql_cluster.py` (regra `zorder`, "clustering automático"), pode rodar `VACUUM` sem ninguém pedir. A
  instrução "não rodar `VACUUM`" não controla nenhum desses dois caminhos.
- **Correção:** `DEEP CLONE` das quatro SCD2. Custa uma cópia (dezenas de GB em S3, alguns minutos de cluster) e deixa a
  referência independente da origem.

### R19 — Contiguidade: o mesmo teste aparece em duas tarefas, e uma variante reintroduz o custo · **corrigir antes de implementar**
- **Spec:** F2 T0 (DQ de continuidade e unicidade nos três DQ) × F3 T-L1 (asserção "em `dq_<scd2>.sql` ou no início do
  log") e a consulta da §5.4 (`WHERE prox IS NOT NULL AND valid_until <> prox`).
- **Evidência:** a consulta de F3 não pega versão fechada sem sucessora (`prox` nulo e `valid_until` não nulo), que é
  justamente o estado de R3. `dq_socios.sql:35-42` já faz o teste certo (`IS DISTINCT FROM`). Pôr a asserção no
  início do log recria a janela sobre o histórico inteiro que a Ideia 1 elimina.
- **Correção:** uma tarefa só (a T0 da F2), com `IS DISTINCT FROM` + `sem_corrente`, nos DQ das SCD2. A F3 só depende
  dela.

### R20 — U1 (`cnpj_basico` no log) com migração por `ADD COLUMN` deixa meses antigos nulos · **corrigir antes de implementar**
- **Spec:** F3 T-L4 ("DDL com coluna no fim + script em `src/jobs/migracoes/` (ou recriação, D-08)").
- **Evidência:** o log é gravado por `REPLACE WHERE` da janela, com inserção posicional (`log_eventos_negocio.sql:433-436`).
  Um `ADD COLUMN` deixa `cnpj_basico` nulo nos meses fora da janela. Com o `mapeamento_pk` removido (§3.2), qualquer
  reprocessamento posterior do `fato_mudanca_estado` nesses meses ficaria com `cnpj_raiz` nulo e todos os SK com `-1`.
  O precedente do repo é recriar o log: `src/jobs/migracoes/2026-11-14_semantica_eventos.sql:13` faz `DROP TABLE`.
- **Correção:** fixar "recriação + reprocessamento do log em `[2023-05, último mês]`" e um DQ de `cnpj_basico` não nulo em
  **todos** os meses, não só na janela.

### R21 — Sugestão de *clustering* por `valid_since` (F3) contra a decisão da F2 · **observação**
F3 §3.1 ("Sugestão para a fase 2: clustering por `valid_since`") × F2 §2.4 (5) e §10 (descarta *clustering* da silver).
Só uma das duas pode valer. Se a F3 quiser *data skipping* no log, a decisão é da F2 e deve ficar registrada lá.

### R22 — Itens da guarda-chuva sem dono na F3 · **observação**
- GC §3.1 diz que os 5 `ALTER COLUMN ... COMMENT` de `fato_empresa_mes.sql:58-69` (5 commits Delta por execução)
  "ficaram como estão (arquivo da fase 3)". A F3 não os menciona. Tarefa barata: só aplicar quando o comentário difere,
  como na bronze, ou mover para o `CREATE`.
- GC §5 tem o aceite "Dashboard abre sem corte de linhas com 12 meses", mas F3 §2.6 conclui que **não há corte**, só
  custo por clique. O critério deve passar a ser tempo de resposta (o que F3 §5.5 já mede).
- GC Fase 4 item 2 (reextração de `empresas` com a máscara da D-16): se ela vier depois da carga inicial da janela, abre
  uma onda de versões falsas em empresas, com o hash mudando de uma vez. Isso exercita o "mês denso" da F2 sem ninguém
  ter planejado. A reextração precisa vir **antes** da reconstrução das SCD2 da janela completa.

### R23 — Tabelas de trabalho da F2 e o gerador do Job · **observação**
`gerar_job.py:103-107` (`RE_ESCRITA`) vai registrar `tmp_scd2_acoes_<t>` e `tmp_scd2_meses_<t>` como tabelas
"produzidas" pela task `si_<t>`. Isso é inofensivo hoje, porque nenhum outro SQL as lê. Vale uma linha na F2 avisando
que `--checar` muda o YAML só pela ordem e que nenhum DQ deve ler `tmp_*`.

---

## 5. Complexidade desnecessária (o que simplificar)

### R24 — `rn_busca` custa uma troca larga a mais no mês denso · **corrigir antes de implementar**
- **Spec:** F2 §3.5, instrução 6 (`ROW_NUMBER() OVER (PARTITION BY n.pk, n.valid_since ...)` sobre o resultado do join).
- **Evidência:** a junção exata é por `(pk, mes, prefixo, ts)`. O particionamento que sai dela,
  `hashpartitioning(pk, mes_referencia, substr(...), bronze_processed_at)`, não satisfaz `PARTITION BY (n.pk,
  n.valid_since)`, então o Spark faz mais uma troca **larga** do mês denso (57 M linhas na carga inicial). Somadas a junção
  e o `optimizeWrite`, são três trocas largas do mês denso, contra a uma que a estimativa da §7.1 (~10 GB) supõe. A
  própria spec diz que a rede de segurança cobre um caso que a bronze já elimina (`dropDuplicates()` em todas as colunas,
  `bronze_lib.py:197`; `source_reference` é constante por mês, `landing_lib.py:611`).
- **Correção:** tirar o `rn_busca` e confiar no DQ de unicidade de `(pk, valid_since)` que a T0 acrescenta. Ele falha
  alto se aparecer duplicata.

### R25 — Prefixo de 16 caracteres (D2): ganho superestimado para a explicação que exige · **observação**
- **Spec:** F2 §3.1 ("a linha cai para ~40% do tamanho") e D2.
- **Evidência:** a `pk` de estabelecimentos é um JSON de ~38 caracteres (`bronze_lib.py:185-187`). A linha estreita tem
  `pk` + mês + origem + ts + hash. Trocar 64 por 16 caracteres reduz a linha para ~60%, não para ~40%. Em troca, o desenho
  precisa defender colisão, o desempate do B16 por prefixo e as consultas de unicidade na T0.
- **Correção:** começar com o hash completo (D2-b). Só adotar o prefixo se a medição mostrar que a troca estreita domina
  o tempo.

### R26 — `dim_empresa` (b) por "ponteiros": difícil de defender e muda o modo de falha · **observação**
- **Spec:** F3 §3.4 (b) e T-D2.
- **Evidência:** hoje uma sobreposição duplica a fatia e o DQ de SK acusa (`dim_empresa.sql:238-239`, D-09). O desenho
  novo precisa de `MAX(struct)`, `LAST(..., TRUE)`, ponteiros com `vu` e uma asserção nova (T-D2) só para não perder esse
  comportamento. O ganho é incerto: o lado largo de `est_matriz` já chega particionado por `cnpj_basico` desde o
  `ANTI JOIN`, e as junções por intervalo seguintes usam a mesma chave (`dim_empresa.sql:177-182, 256-268`). O *spill*
  pode vir do *buffer* do *sort-merge*, e não de trocas repetidas.
- **Correção mais simples, com efeito parecido:** manter `fronteiras`/`fatias` como estão. Fazer as junções por intervalo
  só com as **chaves** de cada fonte (`cnpj_basico, pk, valid_since, valid_until`) e trazer as colunas largas depois, por
  junção de igualdade em `(pk, valid_since)`. O "duplica e o DQ acusa" continua valendo, e a T-D2 deixa de ser
  necessária. Em qualquer caso, olhar o `EXPLAIN` antes de investir.
- Detalhe: `est_matriz` hoje **não projeta `pk`** (`dim_empresa.sql:137-171`). Tanto (a) quanto (b) precisam acrescentá-la.

### R27 — Dataset de fluxos com `EXPLODE(SEQUENCE(1L, qtd))` · **observação**
- **Spec:** F3 §3.5 (ii).
- **Evidência:** o dataset continua com milhões de linhas, e cada clique gera essas linhas de novo no warehouse.
- **Correção mais simples, com o mesmo ganho:** materializar o SQL atual de `fluxos_empresa` (`gerar_dashboard.py:195-215`)
  numa tabela gold estreita, carregada por `REPLACE WHERE`. Os números ficam iguais por construção, sem o truque do
  `EXPLODE` para explicar na banca. O dataset passa a ser `SELECT` dessa tabela. Tamanho estimado: cerca de 20 M linhas
  estreitas em 40 meses.

### R28 — Ordem de implementação no log e no SCD2 · **observação**
- F3: a Ideia 1 (filtrar antes do `LAG`) dá quase todo o ganho, porque as trocas passam a carregar só as versões que
  mudaram. A Ideia 2 (arrays de `NAMED_STRUCT` + `INLINE`) só elimina leituras repetidas de um conjunto já pequeno.
  Fazer só a Ideia 1, medir, e deixar a Ideia 2 condicionada à medição.
- F2: o `descartadas` com `BROADCAST` (F3 §3.4a) supõe que o B19 é raro ("a medir"). Sem esse número, a dica pode
  estourar a memória do driver. Pôr um `ASSERT` de tamanho, como a F2 fez com a instrução 4, ou tirar a dica.
- F3 §3.3: "`antigas` não passa por shuffle nenhum" depende de `ponte_primaria` ser difundida. Hoje ela não tem dica
  (`fato_empresa_mes.sql:190-199, 217`) e o tamanho de `dim_grupo_cnae` não está medido. Acrescentar a dica ou retirar a
  afirmação.

### R29 — D4: "muda o protocolo da tabela, sem volta" · **observação**
*Deletion vectors* podem ser removidos com `ALTER TABLE ... DROP FEATURE deletionVectors` (DBR 14.1+). A afirmação
deixa a decisão mais assustadora do que ela é. Conferir também na T0 se as SCD2 reais **já** têm DV ligado (o padrão da
workspace pode ligá-lo em tabela nova); nesse caso, a D4 já está decidida.

---

## 6. Lacunas no plano de validação

### R30 — Cenários que faltam na F2 §6.1 · **corrigir antes de implementar**
Todos na amostra, sobre **cópias** da bronze da amostra (`amostra_bronze_teste`), nunca sobre a bronze real:
1. **B16 sintético:** duplicar algumas linhas de socios em `2023-06` (contexto de `07`) e de estabelecimentos em `2023-07`,
   com `change_hash` diferente e o mesmo `bronze_processed_at`. Rodar o código atual e o novo e comparar (pega R1).
2. **Mês faltando no meio da janela:** apagar `2023-07` da cópia e rodar `05..08`. Esperado: a instrução 1 falha sem
   gravar nada (D5). Hoje C9 só testa a janela inteira sem bronze.
3. **Falha entre o passo 5 e o passo 6:** rodar só as instruções 0–5 e depois o arquivo inteiro com a mesma janela.
   Esperado: igual à referência (prova a D3). Depois, a variante da R3: rodar a janela **seguinte**. Esperado: a
   instrução 1 falha.
4. **Mês denso que não é o primeiro:** trocar o `change_hash` de todas as linhas de `2023-07` na cópia (simula C-19 ou a
   D-16) e rodar `07` sobre a tabela pronta. Exercita o ramo denso com `S_FORA`, que nenhum cenário atual cobre.
5. **Guarda da instrução 4:** baixar o limite para um valor pequeno numa cópia do arquivo e confirmar que falha sem
   escrever.
6. **Reprocessamento com bronze diferente:** documenta as diferenças intencionais 1 e 2 da §4 com números, em vez de só
   afirmá-las.
7. **C6 não exercita `AJUSTA_FIM`:** depois do `DELETE ... WHERE valid_since = '2023-07'`, a predecessora continua com
   `valid_until = '2023-07'`, então só o caminho `INSERE` roda. Para cobrir `AJUSTA_FIM`, fazer também
   `UPDATE ... SET valid_until = NULL, is_current = TRUE` na predecessora.
8. **Socios na fronteira de bloco:** conferir explicitamente (e não só pelo `EXCEPT`) que existem saídas e retornos em
   `07`, cujo contexto `06` está no bloco anterior (C3). Se a amostra não tiver, o cenário não prova nada.

### R31 — Lacunas na F3 §5 · **observação**
- Testar a **migração** do painel e das `dash_*_mes` a partir da tabela existente (R2), além de "05..06 + 07..08 = 05..08".
- Testar U1 com reprocessamento de um mês **antigo** depois da migração (R20).
- Os DQ das SCD2 e o `dq_gold` também crescem com o histórico (por exemplo, `dq_socios.sql:46-62` faz junção por intervalo
  sobre socios inteira) e não estão na linha de base. Medir o tempo das tasks de DQ na mesma execução.

---

## 7. O que só dá para confirmar com dados (rodar quando o login voltar)

Em lote, numa execução só. As consultas no warehouse 2X-Small estão indicadas; o resto vai num notebook no cluster, junto
com a medição.

```sql
-- D1. B16 por mês e tabela (tamanho do problema da R1): pks repetidas por (pk, mês), incluindo o mesmo bronze_processed_at.
SELECT 'socios' t, mes_referencia, COUNT(*) FROM (
  SELECT pk, mes_referencia FROM tcc_receita.bronze.socios GROUP BY 1,2 HAVING COUNT(*) > 1) GROUP BY 1,2
UNION ALL
SELECT 'estabelecimentos', mes_referencia, COUNT(*) FROM (
  SELECT pk, mes_referencia FROM tcc_receita.bronze.estabelecimentos GROUP BY 1,2 HAVING COUNT(*) > 1) GROUP BY 1,2;
-- (repetir para empresas e dados_simples)

-- D2. Estado de risco da R3 nas SCD2 reais: pks sem versão corrente e versões fechadas sem sucessora.
SELECT COUNT(*) FROM (SELECT pk FROM tcc_receita.silver_intermediate.estabelecimentos
                      GROUP BY pk HAVING COUNT_IF(is_current) <> 1);
SELECT COUNT(*) FROM (SELECT valid_until, LEAD(valid_since) OVER (PARTITION BY pk ORDER BY valid_since) prox
                      FROM tcc_receita.silver_intermediate.estabelecimentos)
WHERE valid_until IS DISTINCT FROM prox;          -- repetir nas 4 SCD2

-- D3. Deletion vectors e otimização preditiva já ligados? (decide D4 e o risco da R18)
SHOW TBLPROPERTIES tcc_receita.silver_intermediate.estabelecimentos;   -- delta.enableDeletionVectors
DESCRIBE DETAIL tcc_receita.silver_intermediate.estabelecimentos;      -- clusteringColumns, numFiles, sizeInBytes

-- D4. Tamanho de dim_grupo_cnae / ponte_primaria (R28: dá para difundir?)
SELECT COUNT(*) FROM tcc_receita.silver_final.dim_grupo_cnae;

-- D5. CNPJs com mais de uma ordem marcada como matriz (tamanho de `descartadas`, R28) — já na F3 §5.4.

-- D6. Meses distintos por tabela gold de série (base da checagem da R2).
SELECT 'fato', COUNT(DISTINCT mes_referencia) FROM tcc_receita.gold.fato_empresa_mes
UNION ALL SELECT 'painel', COUNT(DISTINCT mes_referencia) FROM tcc_receita.gold.fato_indicadores_setoriais_mes;

-- D7. Fração de linhas por mês que abriria versão (calibra os 20% do "mês denso"); igual à Q2 da F2.
SELECT valid_since, COUNT(*) FROM tcc_receita.silver_intermediate.estabelecimentos GROUP BY 1 ORDER BY 1;

-- D8. Precedência do optimizeWrite (R12): depois de ALTER TABLE <cópia> SET TBLPROPERTIES
--     ('delta.autoOptimize.optimizeWrite' = 'false') no cluster do Job (conf global true), um INSERT pequeno
--     e DESCRIBE HISTORY: o plano/métricas mostram a troca do optimized write? (cluster, não warehouse)
```

No cluster (notebook único, junto com a medição da R17):
- `EXPLAIN FORMATTED` das instruções 2 e 6 da F2 (em `estabelecimentos` e `socios`) e do log da F3: contar `Exchange`;
  conferir `dynamicpruningexpression` nos *scans* da bronze (R14), `BroadcastHashJoin LeftSemi` sem `Exchange` e o
  `Exchange` extra do `rn_busca` (R24); conferir se o `Generate` preserva o particionamento por `pk`.
- `EXPLAIN FORMATTED` da `dim_empresa` atual: contar quantas vezes `est_matriz` passa por troca larga (decide R26).
- Event log da execução de 4 meses: *spill* por *stage* da `dim_empresa` (*sort* do *sort-merge* ou troca?), tempo das
  tasks de DQ (R31) e a troca do `optimizeWrite` por task.
