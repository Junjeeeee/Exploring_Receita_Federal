# Feature: otimização da ingestão bronze (fase 1b)

**Status:** P1, P2 e P3 **preparadas, não executadas no Databricks** (§9; roteiro de medida na §10). P4–P6 só desenho.
As linhas de código citadas nas §2–§3 referem-se ao commit `ef09865` (antes de P1–P3).
**Camadas afetadas:** bronze (e o Job em `src/jobs/`)
**Branch:** `worktree-otimizacao`
**Spec guarda-chuva:** [`2026-11-16-otimizacao-pipeline.md`](2026-11-16-otimizacao-pipeline.md) (fase 1b; princípios §2, cluster §3.3)
**Origem:** B17 em `docs/backlog_historico.md:27`; linha de base em `docs/medicoes/2026-11-16-linha-de-base.md`.

## Roteiro de leitura (1 página)

**O problema.** No run completo de 4 meses (2023-05..08), a bronze ocupou 12.065 s de 23.524 s de execução somada
(51%). As 4 tabelas grandes (estabelecimentos, empresas, socios, dados_simples) somam 9.326 s e a DQ da bronze, 1.201 s.
Esse custo cresce em linha reta com o número de meses: para ~40 meses, só a ingestão das 4 grandes passaria de 90 mil s
somados (~26 h), se nada mudar.

**Por que é lenta.** Para cada mês de cada tabela, o código lê o Parquet da landing e o "embaralha" inteiro pela rede
**três vezes**. Embaralhar (*shuffle*) é redistribuir todas as linhas entre as máquinas do cluster, gravando em disco e
mandando pela rede; é a operação mais cara do Spark. As três vezes são:
1. remover linhas repetidas idênticas (`dropDuplicates`) só para **contar** e imprimir um aviso;
2. remover as mesmas repetidas **de novo** para gravar (o Spark não guarda o resultado do passo 1);
3. o *optimizeWrite*, uma opção ligada no cluster todo que reorganiza os dados antes de gravar para fazer arquivos
   de tamanho uniforme. Aqui ele repete o trabalho que o passo 2 já fez.

**O que esta spec propõe, em ordem.**
1. **Tirar a contagem de dentro da ingestão** e fazê-la na DQ lendo só a coluna `pk` (barato). Corta 1 das 3 passadas.
2. **Desligar o optimizeWrite só na escrita da bronze.** Corta a 2ª das 3. Juntas, 1 e 2 devem deixar a ingestão das 4
   grandes entre ~1/3 e ~1/2 do tempo atual (estimativa; **a medir**).
3. **DQ em uma passada por tabela** (todos os meses de uma vez), em vez de duas leituras por mês. Ganho pequeno hoje
   (algumas centenas de segundos), relevante com 40 meses.
4. Depois de medir 1–3: **gravar vários meses numa só escrita** e, se a landing não tiver linhas idênticas
   repetidas, **pular a deduplicação** quando uma checagem barata provar que ela não muda nada.

**O que não muda.** Nenhuma regra de negócio. As linhas gravadas são as mesmas, com o mesmo `pk`, o mesmo
`change_hash` (SHA-256), o mesmo `mes_referencia`; só `bronze_processed_at` (hora da gravação) muda. A tabela
continua particionada por `mes_referencia`. Nada de LGPD é tocado (a máscara é feita na landing).

**O que foi descartado.** Trocar o SHA-256 por um hash mais rápido (mudaria o resultado e o ganho é pequeno);
separar as tabelas grandes em clusters próprios (cada cluster novo leva 2–6 min para subir e o atual não está
saturado); Photon e serverless (custo, decisão do usuário); juntar ingestão e DQ na mesma tarefa (ganho ≤ ~20 s
por tarefa).

**Como provar que deu certo.** Rodar um mês de uma tabela (estabelecimentos 2023-08, a maior) gravando numa tabela de
teste, comparar com a bronze atual por uma "impressão digital" por mês (contagem + soma de hashes de
`pk`/`change_hash`), e ler no *event log* (registro detalhado do Spark) quantos GB foram embaralhados. Uma subida de
cluster de ~30 min.

## 1. Requisito

- **Problema:** a ingestão bronze é linear no nº de meses e domina o tempo do run de 4 meses; precisa caber na janela
  completa (~40 meses, em blocos — fase 4) com custo previsível.
- **Quem consome:** a silver_intermediate, que depende de (a) `(pk, mes_referencia, change_hash)` única na bronze
  (T0, confirmada na linha de base §7) e (b) bronze particionada por `mes_referencia`.
- **Restrições:** resultado idêntico (mesmas linhas, `pk`, `change_hash`, `mes_referencia`; `bronze_processed_at` pode
  mudar); bronze sem regra de negócio; LGPD; cluster clássico spot `r5d.xlarge` 1–4, DBR 15.4 / Spark 3.5, sem Photon,
  sem serverless.
- **Fora de escopo:** landing; troca do tipo de instância (fica na §3.3 da guarda-chuva); C-19 (fixar `colunas_hash`
  muda o hash; é decisão à parte); B16 (regra para pks repetidas).

## 2. Diagnóstico

Legenda: **[C]** fato lido no código; **[M]** medido (linha de base ou B17); **[H]** hipótese, **a medir**.

### 2.1 Caminho de uma tabela mensal, mês a mês

`ingerir_tabela_bronze` (`src/bronze/bronze_lib.py:205-274`) faz um laço por mês (`:225`). Em cada volta:

| Etapa | Onde | O que custa |
|---|---|---|
| Leitura do Parquet | `:228` `spark.read.parquet(<mês>)` | **[C]** um arquivo `dados.parquet` por mês (`landing_lib.py:805`), todas as colunas texto. Lido **2×** por mês, uma por ação (abaixo). |
| Normalização da lista de CNAE | `:237-239`, `:143-154` | **[C]** só estabelecimentos; split/trim/sort por linha. **[H]** CPU pequena. |
| Filtro de componentes da pk | `:241`, `:157-176` | **[C]** filtro linha a linha, sem shuffle. |
| `change_hash` | `:247-251` | **[C]** `sha2(to_json(struct(todas as colunas de negócio)), 256)`. Nenhuma das 4 configs define `colunas_hash` (`configs/*.py`), então entram todas (30 em estabelecimentos). Calculado **2×** por mês. **[H]** custo de CPU, conta na §2.4. |
| `pk` | `:252`, `:179-190` | **[C]** `to_json(struct(chaves))` ou `PK_SOCIOS_EXPR` (`configs/socios.py:4-18`). Barato. |
| Deduplicação | `:253`, `:197` | **[C]** `dropDuplicates()` em **todas** as colunas: shuffle da linha inteira. |
| Contagem de pks repetidas | `:199` | **[C]** `groupBy("pk").count().filter(...).count()` é uma **ação** (comando que obriga o Spark a executar o plano). Sem `cache`, ela executa leitura + hash + `dropDuplicates` só para imprimir um aviso (`:200-201`). **[M]** B17: é o 1º dos 3 shuffles completos. |
| Metadados | `:255-264` | **[C]** `mes_referencia` literal, `bronze_processed_at = current_timestamp()`, descarta `ingested_at`. Barato. |
| Escrita Delta | `:266-269` | **[C]** `overwrite` + `replaceWhere mes_referencia='<m>'` + `partitionBy("mes_referencia")`: a 2ª ação, que **refaz** leitura + hash + `dropDuplicates` (2º shuffle) e grava. Um *commit* Delta (registro atômico de nova versão da tabela) por mês. |
| optimizeWrite | `gerar_job.py:87` | **[C]** ligado no cluster inteiro. **[M]** B17: soma o 3º shuffle completo antes de gravar. |
| Comentários UC | `:272-273`, `:90-115` | **[C]** uma vez por tabela, depois do laço. Desde a fase 1 só aplica o que difere (0 commits a partir da 2ª execução). **Preparado, não executado.** |
| OPTIMIZE / estatísticas | — | **[C]** o código **não** roda `OPTIMIZE`, `ANALYZE` nem `ZORDER`. **[M]** a otimização preditiva do Databricks rodou `OPTIMIZE`/`COMPUTE STATS`/`VACUUM` em estabelecimentos fora do Job (linha de base §7, D3/R18). Não entra no tempo do Job; **[H]** entra na conta (SKU próprio) — a medir. |
| Estatísticas por arquivo | escrita Delta | **[C/H]** o Delta coleta mín/máx/nulos das 32 primeiras colunas em toda escrita. **[H]** CPU modesta. |

**[M] B17 (event log de empresas, 2026-11-08):** por mês, ~1,6 GB de Parquet viram **3 shuffles de ~7 GB**; sem spill
(dado que transborda da memória para o disco), GC (coleta de lixo da JVM) baixo; ocupação dos cores ~40–60%.

**Por que a deduplicação não pode simplesmente sair (B17 propunha):** **[C]** sem ela, uma linha que a landing
repetisse idêntica entraria duas vezes, e `(pk, mês, change_hash)` deixaria de ser única — quebra T0 e a regra
"resultado idêntico". Se a landing tem essas cópias é **desconhecido** (pergunta Q1).

**Por que deduplicar por todas as colunas equivale a deduplicar por `(pk, change_hash)`:** **[C]** fora as colunas de
negócio, a linha só tem `mes_referencia`, `source_reference` e `ingested_at`, constantes no arquivo do mês
(`landing_lib.py:610-612`); `change_hash` cobre todas as colunas de negócio, e `pk` deriva delas. Logo, salvo colisão
de SHA-256, duas linhas com o mesmo `(pk, change_hash)` são iguais em tudo. Isso sustenta as propostas P5 e a
validação da §4. (Não ajuda o shuffle por si: `dropDuplicates(["pk","change_hash"])` ainda move a linha inteira.)

### 2.2 DQ da bronze

`testar_tabela_bronze` (`bronze_lib.py:276-324`), laço por mês (`:286`):
- `validar_schema` (`data_quality.py:10-22`): só metadados, sem ler dados. **[C]**
- `validar_volumetria` (`data_quality.py:24-68`): `df.count()` do mês (`:25`), `CREATE TABLE IF NOT EXISTS` do log (`:32`),
  consulta da média dos 3 meses anteriores (`:38-48`) e um `INSERT` (um commit) por mês (`:68`). **[C]** Os meses do
  próprio run entram na média dos seguintes, porque cada `INSERT` acontece antes do mês seguinte. **[H]** o `count()`
  com filtro de partição pode sair só dos metadados do Delta, sem ler arquivos.
- Nulos em `change_hash`/`pk` (`bronze_lib.py:312-315`): uma agregação por mês, lendo duas colunas. **[C]**
- Total por tabela grande e mês: 2 leituras + 2 consultas pequenas + 1 commit. **[M]** DQ das 4 grandes: 226 + 165 + 114
  + 109 = 614 s; estáticas somam 587 s, com `cnaes_ibge` em 21 s, o que limita o custo fixo de uma tarefa a ≲ 21 s.

### 2.3 Job e cluster

- **[C]** Todas as tarefas usam **um** job cluster (`gerar_job.py:118`, `CLUSTER` em `:71-97`): `r5d.xlarge` 1–4 spot,
  `shuffle.partitions=auto`, `FAIR`. As 11 ingestões bronze não dependem de nada (`:132-136`) e começam juntas; cada DQ
  depende só da sua ingestão.
- **[C/H]** Consequência: as durações das 4 grandes **se sobrepõem**; 9.326 s é soma de tarefas, não tempo de parede nem
  tempo de CPU. Tirar trabalho de uma libera cores para as outras. A métrica certa de ganho é o tempo de CPU dos
  executores (`executorRunTime` no event log) e o tempo de parede do trecho bronze.
- **[C]** Job cluster começa com `min_workers` = 1. **[H]** a bronze, que roda no início, pode passar minutos com menos de 4
  workers enquanto o autoscale reage (§3.3 da guarda-chuva) — parte dos 40–60% de ocupação pode vir daí.
- **[M]** `setup_duration` somou 3.308 s no run de 11-08 (114–354 s por ocorrência). **[H]** a soma conta a mesma espera
  em várias tarefas paralelas e as subidas de cluster dos reparos; não é "um cluster por tarefa".

### 2.4 Custo do SHA-256 (ordem de grandeza)

**[H]** estabelecimentos tem ~57 mi linhas/mês. Supondo ~500 bytes de JSON por linha, são ~28 GB/mês passando pelo
SHA-256. A ~300 MB/s por core (ordem típica de SHA-256 em Java sem instrução dedicada; **a medir**), dá ~95 s de CPU
por mês, ou ~6–24 s de parede em 16–4 cores, contra ~830 s por mês de tarefa (3.317 ÷ 4). Mesmo multiplicando por 3
para o `to_json` e pelo cálculo em dobro de hoje, fica abaixo de ~10%. **Conclusão:** o hash não é o gargalo; o que
pesa é ele ser calculado 2× (resolvido pela P1).

## 3. Propostas (ordem: ganho esperado ÷ risco)

Base das contas: **[M]** 9.326 s somados nas 4 grandes em 4 meses; **[M]** B17: 3 shuffles completos + 2 leituras por mês.
Modelo simplificado **[H]**: o tempo da ingestão é dominado pelos shuffles e cada passada custa parecido. Faixa
conservadora: o shuffle é metade do tempo; otimista: é quase todo.

### P1 — Tirar a contagem de pks repetidas da ingestão

- **Muda:** `_remover_copias_exatas` (`bronze_lib.py:193-202`) deixa de executar a ação da linha 199; a contagem vai para a
  DQ, sobre a tabela já gravada, lendo só `pk` e `mes_referencia` (Delta lê só as colunas pedidas).
- **Por que é seguro:** a contagem só imprime um aviso; não filtra nem altera nada. O plano de escrita fica igual.
- **Validar:** fingerprint da §4 = idêntico; event log com 1 ação de escrita por mês (antes 2) e 2 shuffles (antes 3);
  o aviso aparece no log da DQ com o mesmo número.
- **Ganho:** remove 1 de 3 shuffles e 1 de 2 leituras/hashes. Conservador ~20%, otimista ~33% de 9.326 s →
  **~1.900–3.100 s** em 4 meses; ~19–31 mil s em 40 meses. A DQ ganha um `groupBy(mes, pk)` de uma coluna estreita
  (~40 bytes por linha em estabelecimentos, ~2,3 GB por mês contra ~7+ GB da linha inteira) — **a medir**.
- **Pode dar errado:** quase nada. Se a DQ ficar mais lenta que o ganho (improvável), a contagem pode virar opcional.
- **Alternativa descartada:** `cache()`/`persist()` do resultado deduplicado: guardaria vários GB por mês (≥ ~7 GB em empresas, pelo B17) em memória/NVMe e
  mantém o 2º shuffle do optimizeWrite; P1 é mais simples e mais barata.

### P2 — Desligar o optimizeWrite só na escrita da bronze

- **Muda:** a escrita (`bronze_lib.py:266-269`) passa a gravar sem o optimized write, por opção do próprio `writer`
  (`.option("optimizeWrite", "false")`, se o DBR 15.4 aceitar) ou por `spark.conf.set(...)` na sessão da tarefa bronze.
  A conf global do cluster (`gerar_job.py:87`) continua valendo para silver/gold.
- **Por que é seguro:** o optimized write só decide **como as linhas se distribuem em arquivos**, não quais linhas são
  gravadas. A tabela continua particionada por `mes_referencia` (`partitionBy` não muda).
- **Validar:** fingerprint idêntico; event log sem o shuffle antes da escrita; `DESCRIBE HISTORY` → `numFiles`,
  `numOutputBytes`, `numOutputRows` do commit (mesmo nº de linhas, mais arquivos). Confirmar a precedência: opção do
  writer vs. conf da sessão vs. propriedade de tabela (revisão R12/D8) e se `spark.conf.set` numa tarefa vaza para
  outras tarefas do mesmo cluster (Q3).
- **Ganho:** sem o optimized write, o shuffle do `dropDuplicates` já entrega partições de tamanho controlado pelo AQE
  (*Adaptive Query Execution*, que ajusta o nº de partições em tempo de execução). Remove mais 1 dos 3 shuffles
  originais: conservador ~15%, otimista ~33% → **~1.400–3.100 s**. **P1+P2:** de 3 shuffles + 2 leituras para 1 + 1 →
  9.326 s viram **~3.100–4.700 s** (ganho de ~4.600–6.200 s em 4 meses; ~46–62 mil s em 40 meses). B17 estimou 3–4×
  com todas as propostas; esta spec não promete isso só com P1+P2.
- **Pode dar errado:** arquivos menores e mais numerosos (hoje 128 arquivos em 35 GB, ~275 MB cada; o tamanho alvo do
  AQE padrão é 64 MB de shuffle por partição) → a silver lê mais arquivos. Mitigação: subir
  `spark.sql.adaptive.advisoryPartitionSizeInBytes` (ex.: 256 MB) na sessão bronze. A otimização preditiva pode
  compactar depois, mas isso custa fora do Job (Q8). Se a conf de sessão não for isolada por tarefa, a mudança tem de
  ser por opção do writer.

### P3 — DQ em uma passada por tabela

- **Muda:** `testar_tabela_bronze` lê a faixa de meses uma vez e faz **uma** agregação por `mes_referencia`: `count(*)`,
  nulos de `change_hash`, nulos/vazios de `pk` (e, com P1, pks repetidas num segundo `groupBy` só de `pk`). A
  volumetria roda no driver, mês a mês **em ordem**, usando o histórico do `dq_metrics_log` lido uma vez **mais** os meses
  já aprovados neste run (mesma regra de hoje: até 3 meses anteriores, última linha por mês); grava o log num `INSERT`
  único ao final, só se tudo passar.
- **Por que é seguro:** não toca a tabela bronze. As regras e limites (`limite_queda_volumetria`) são os mesmos; só muda
  quantas vezes os dados são lidos. Diferença de comportamento a registrar: hoje, se o mês 3 falha, os meses 1–2 já
  ficaram no log; com `INSERT` único, nenhum fica. Alternativa que mantém o comportamento: inserir mês a mês até a
  falha (custo de 1 commit por mês, pequeno).
- **Validar:** rodar DQ antiga e nova sobre a mesma bronze e comparar as contagens por mês e as mensagens; forçar uma
  queda artificial (limite 0%) numa cópia do log para ver o erro sair no mesmo mês.
- **Ganho:** de 2 leituras + 4 consultas pequenas por mês para 1 leitura + 2 consultas por tabela. A parte que escala com
  o mês é pequena: **ordem de 200–350 s** dos 614 s das 4 grandes em 4 meses (**a medir**), mais relevante com 40 meses
  (hoje 80 leituras por tabela). A contagem de pks repetidas (P1) devolve parte disso.
- **Pode dar errado:** erro na ordem da volumetria (comparar com meses errados). Coberto pelo teste de queda forçada.

### P4 — Vários meses numa só escrita (lotes de k meses)

- **Muda:** em vez do laço mês a mês, lê os k meses do lote como uma união de leituras por caminho, cada uma com
  `mes_referencia` literal (como hoje), deduplica e grava com `partitionBy("mes_referencia")` + `replaceWhere
  "mes_referencia >= '<ini>' AND mes_referencia <= '<fim>'"` num commit só. k vira parâmetro (ex.: 3; ver Q5).
- **Por que é seguro:** a deduplicação inclui `mes_referencia`, então só junta linhas do mesmo mês (igual a hoje); pk,
  hash e mês por linha não mudam. Não usar a descoberta de partição pelo caminho (`mes_referencia=2023-05` poderia ser
  lido com outro tipo): o mês vem do literal, como hoje. Um mês faltando na landing continua derrubando a tarefa
  (o caminho não existe), então o `replaceWhere` em faixa não apaga partição sem substituta.
- **Validar:** fingerprint por mês idêntico; `DESCRIBE HISTORY` com 1 commit por lote; event log com 1 job de escrita.
- **Ganho:** **a medir**. Remove custos fixos por mês (início/cauda de stage, commit, leitura do rodapé) e deve subir a
  ocupação dos cores. Teto grosseiro: se a ocupação subisse de ~50% para ~80%, o tempo restante cairia ~35%. Mas parte da
  ociosidade pode ser autoscale ou concorrência entre tarefas (§2.3), que P4 não resolve. Só faz sentido **depois** de
  P1+P2 (senão é um shuffle triplo de ~30 GB).
- **Pode dar errado:** shuffle maior por lote ocupa mais NVMe e, se um worker spot cair, perde-se mais trabalho; uma
  falha refaz o lote inteiro. `bronze_processed_at` passa a ser igual em todo o lote (permitido).

### P5 — Deduplicação condicional (só se Q1 mostrar landing sem cópias)

- **Muda:** grava o mês **sem** `dropDuplicates` (escrita sem nenhum shuffle) e, em seguida, confere na tabela gravada se
  existe `xxhash64(pk, change_hash)` repetido (lendo 2 colunas; `xxhash64` é um hash rápido de 8 bytes). Se houver
  qualquer repetição — real ou colisão do `xxhash64` —, regrava o mês pelo caminho atual, com `dropDuplicates`.
- **Por que é seguro:** linhas idênticas têm o mesmo `xxhash64`, então uma cópia nunca escapa (sem falso negativo). Pela
  equivalência da §2.1, sem repetição de `(pk, change_hash)` o `dropDuplicates` não removeria nada. A garantia T0 passa a
  ser **checada** em vez de presumida.
- **Validar:** fingerprint idêntico nos dois caminhos; teste com uma cópia de mês em que se injeta uma linha duplicada
  (o caminho de regravação tem de disparar).
- **Ganho:** **a medir**. Tira o último shuffle de linha inteira; o que sobra é leitura + hash + escrita + um shuffle de 8
  bytes por linha (~460 MB/mês em estabelecimentos, 57 mi × 8 bytes, contra o shuffle da linha inteira, que em empresas — tabela
  mais estreita — já é ~7 GB/mês; o de estabelecimentos é **a medir**). Se Q1 mostrar
  cópias em todo mês, o ganho vira **perda** (grava duas vezes) → não implementar.
- **Pode dar errado:** dois caminhos de código (mais para explicar no TCC); janela curta em que a partição tem cópias,
  antes da regravação, o que é aceitável porque a DQ e a silver só rodam depois da tarefa terminar.

### P6 — Paralelismo e nº de clusters

- **Proposta:** manter **um** job cluster e as 4 grandes em paralelo (já estão). Não separar clusters. Medir a linha do
  tempo de workers (eventos `RESIZING`/`UPSIZE_COMPLETED`) contra as tarefas bronze; se a bronze passar boa parte do
  tempo com 1–2 workers, o teste de `min_workers: 2` da §3.3 da guarda-chuva cobre o caso.
- **Ganho:** 0 sem medida; **a medir**.

### Descartados (e por quê)

| Ideia | Motivo |
|---|---|
| Tirar `dropDuplicates` sem checagem (B17 (1)) | Quebra T0 se a landing tiver cópias; a P5 faz isso com garantia. |
| Trocar SHA-256 por `xxhash64`/MD5 no `change_hash` | Muda o resultado (a silver compara hashes) e o hash pesa < ~10% (§2.4). |
| `dropDuplicates(["pk","change_hash"])` | Ainda embaralha a linha inteira; ganho só na comparação. |
| Cluster separado para as tabelas grandes | +114–354 s de subida e +1 driver on-demand; cluster não saturado (40–60%). |
| Juntar ingestão e DQ numa tarefa | Custo fixo por tarefa ≲ 21 s (`bronze_dq_cnaes_ibge`); muda o DAG que a silver usa. |
| `OPTIMIZE`/`ZORDER`/liquid clustering na bronze | A silver lê meses inteiros; reorganizar custa uma reescrita e não poupa leitura. |
| Reduzir colunas com estatísticas (`delta.dataSkippingNumIndexedCols`) | Ganho provável pequeno; fica como pergunta (Q9), não proposta. |
| Photon / serverless | Fora desta rodada (custo; decisão do usuário). |
| Mexer em `colunas_hash` (C-19) | Muda `change_hash`; decisão separada. |

## 4. Plano de validação

### 4.1 Resultado idêntico

Gravar a versão nova numa tabela de teste (`tcc_receita.bronze_teste.<tabela>`; precisa de um parâmetro de schema de
destino na bronze, tarefa T1) e comparar com `tcc_receita.bronze.<tabela>` no mesmo mês:

```sql
-- Impressão digital por mês: contagem + soma de hashes (independe da ordem, sensível a linha a mais/a menos).
SELECT mes_referencia, COUNT(*) AS linhas,
       SUM(CAST(XXHASH64(pk, change_hash, source_reference) AS DECIMAL(38,0))) AS digital
FROM <tabela> WHERE mes_referencia = '2023-08' GROUP BY mes_referencia;
```
Os dois lados têm de bater. Se não baterem, rodar o detalhamento (mais caro):
```sql
SELECT pk, change_hash FROM <nova> WHERE mes_referencia='2023-08'
EXCEPT ALL SELECT pk, change_hash FROM <atual> WHERE mes_referencia='2023-08';  -- e no sentido inverso
```
`EXCEPT ALL` compara contando repetições (um `EXCEPT` simples esconderia uma cópia a mais). Comparar `pk` +
`change_hash` basta para as colunas de negócio (§2.1); `source_reference` entra na impressão digital. Conferir também o
schema (`DESCRIBE`) e `numOutputRows` do `DESCRIBE HISTORY` das duas escritas.

### 4.2 Medir com o menor custo

1. **Sem cluster (Q1):** contar linhas por mês na landing pelo rodapé do Parquet (DuckDB local, lê só metadados) e comparar
   com o `COUNT(*)` da bronze (warehouse). Diferença = linhas com pk vazia + cópias exatas. Se a diferença for 0, não há
   cópia nem pk vazia; se não for, Q1 fica para a execução do item 2.
2. **Uma subida de cluster, ~30 min:** job avulso com `num_workers: 4` **fixo** (tira o autoscale da conta) e event log
   ligado, rodando em sequência: (a) estabelecimentos 2023-08 código atual → `bronze_teste`; (b) idem com P1+P2;
   (c) empresas 2023-08 com P1+P2 (tabela estreita, compara com o B17); (d) DQ antiga e nova (P3) sobre `bronze_teste`;
   (e) a consulta de cópias por `(pk, change_hash)` antes da deduplicação, para Q1. Pela taxa do run de 11-08
   (12,34 DBU em 4,6 h ≈ 2,7 DBU/h), ~0,5 h custa ~1,3 DBU (~US$ 0,20 de lista, sem EC2).
3. **Ler no event log:** por fase, `executorRunTime` somado, bytes de shuffle escritos, nº de jobs/stages, duração;
   arquivos gravados e tamanho médio. Critério: P1+P2 reduzem o tempo de (a) para (b) em ≥ 40% e o shuffle para ~1/3.
4. **Só depois**, P4/P5 no mesmo formato (lote de 3 meses de estabelecimentos), e o efeito no run de 4 meses completo
   da guarda-chuva (comparação com a linha de base).

## 5. Tarefas

- [ ] T1. Parâmetro de schema de destino na bronze (padrão `bronze`), para gravar em `bronze_teste` sem tocar a real.
- [ ] T2. Script de validação (impressão digital + `EXCEPT ALL` sob demanda) em `scripts/`, uma passada por tabela.
- [ ] T3. Medir Q1 localmente (DuckDB nos rodapés da landing × `COUNT(*)` da bronze no warehouse).
- [x] T4. P1: remover a ação de `bronze_lib.py:199`; contagem de pks repetidas na DQ, lendo só `pk`.
      **Preparado, não executado no Databricks** (§9.1).
- [x] T5. P2: optimized write desligado só na escrita bronze; descobrir a forma que funciona no DBR 15.4 (opção do writer
      ou conf de sessão) e se a conf vaza entre tarefas; `advisoryPartitionSizeInBytes` se os arquivos ficarem pequenos.
      **Preparado (atrás de parâmetro, padrão = comportamento atual), não executado** (§9.2). Documentação consultada;
      qual modo funciona e o vazamento entre tasks ficam para a medida (§10). `advisoryPartitionSizeInBytes` não
      implementado: só se Q6 mostrar arquivos pequenos.
- [x] T6. P3: DQ em uma passada, volumetria em ordem, mesmo texto de erro; teste de queda forçada.
      **Preparado, não executado no Databricks** (§9.3). Queda forçada coberta por teste puro local
      (`tests/test_bronze_otimizacao.py`); a versão no cluster está no roteiro (§10, passo 6).
- [ ] T7. Execução de medida (§4.2 item 2) e registro em `docs/medicoes/` (tempo, shuffle, arquivos, custo).
- [ ] T8. Decidir P4 e P5 com os números de T3/T7; se seguir, spec curta de complemento e nova medida.
- [ ] T9. Medir a linha do tempo de workers na bronze (Q4) junto com o piloto da §3.3 da guarda-chuva.
- [ ] T10. Atualizar `docs/specs/bronze.md` (está desatualizada: diz que pk repetida com hash diferente dá `ValueError`, e as
      linhas citadas mudaram), `docs/backlog.md` (B17) e, se houver decisão, `docs/decisions.md`. Commits separados.
      **Parcial:** `docs/specs/bronze.md` atualizada (P1–P3, `aplicar_comentarios_uc` só aplica diferenças, CLI de DQ
      não está mais quebrada, sem números de linha). `backlog.md`/`decisions.md` ficam para depois da medida.

## 6. Critérios de aceite

- [ ] Impressão digital por mês idêntica entre a bronze nova e a atual em estabelecimentos e empresas 2023-08 (e, depois,
      nos 4 meses das 4 tabelas); `EXCEPT ALL` = 0 nos dois sentidos se a digital divergir e for investigada.
- [ ] T0 continua: `COUNT(*) = COUNT(DISTINCT pk, change_hash)` por mês na bronze nova.
- [ ] Event log: 1 shuffle de linha inteira por mês na ingestão (antes 3); 1 ação de escrita por mês.
- [ ] Ingestão de estabelecimentos 2023-08 em cluster fixo: tempo ≥ 40% menor que o código atual no mesmo cluster.
- [ ] DQ da bronze: mesmas contagens por mês e mesmas falhas (teste de queda forçada) que a versão atual.
- [ ] Arquivos por mês com tamanho médio ≥ ~64 MB (sem explosão de arquivos pequenos).
- [ ] Nenhuma mudança em landing, máscara MEI ou colunas pessoais.

## 7. Perguntas que só dados ou uma execução respondem

- **Q1.** A landing tem linhas idênticas repetidas? Quantas por mês e tabela? (Decide P5.) Quantas linhas são
  descartadas por pk vazia (C-09)?
- **Q2.** Quanto do tempo da ingestão é shuffle e quanto é leitura/hash/escrita? (Define a faixa real de P1+P2.)
- **Q3.** No DBR 15.4, a opção `optimizeWrite` do writer existe? `spark.conf.set` numa tarefa afeta outras tarefas do
  mesmo job cluster? Qual vence: writer, sessão ou propriedade de tabela (R12/D8)?
- **Q4.** A ocupação de 40–60% vem de cauda de stage, de autoscale lento no início do Job ou da disputa entre as 11
  tarefas bronze simultâneas?
- **Q5.** Qual o tamanho de lote k em que P4 para de ganhar (shuffle por lote × NVMe de 150 GB por worker × risco spot)?
- **Q6.** Sem optimized write, quantos arquivos por mês e de que tamanho? A silver lê mais devagar?
- **Q7.** O `count()` da volumetria com filtro de partição usa só metadados do Delta, ou lê arquivos?
- **Q8.** Quanto a otimização preditiva (OPTIMIZE/COMPUTE STATS/VACUUM automáticos) cobra pelas tabelas bronze,
  fora do Job (`system.billing.usage`)?
- **Q9.** Quanto CPU a coleta de estatísticas do Delta (32 colunas) e o SHA-256 consomem de fato (§2.4)?
- **Q10.** A soma de `setup_duration` (3.308 s) conta a mesma espera em tarefas paralelas? Quantos clusters o run
  de 11-08 realmente subiu (reparos incluídos)?

## 8. Notas de execução

(vazio — nada executado)

## 9. Implementação de P1–P3 (preparada, não executada no Databricks)

Commits na branch `worktree-otimizacao`, um por proposta. Verificação local: `py_compile` dos arquivos alterados e
`tests/test_bronze_otimizacao.py` (puro, sem Spark). Nada foi rodado em cluster, Job ou warehouse.

### 9.1 P1 — contagem de pks repetidas fora da ingestão

- `_remover_copias_exatas` (`bronze_lib.py`) passou a ser só `df.dropDuplicates()`: sem a ação
  `groupBy("pk").count().filter(...).count()`, cada mês da ingestão tem **uma** ação (a escrita). O `dropDuplicates` fica.
- A contagem virou aviso (não falha) da DQ: por mês da execução, nº de `pk` com mais de uma linha na tabela gravada,
  impresso como `⚠️ <n> PK(s) repetidas com conteúdo divergente em <tabela> (<mês>) (mantidas; a Silver desempata).`
  (o texto antigo, com o mês acrescentado). O número é o mesmo da ingestão: a tabela gravada é o resultado deduplicado.
  Depois da P3 ela sai da mesma agregação da DQ (§9.3), sem leitura extra.

### 9.2 P2 — optimized write desligável só na escrita mensal

**O que a documentação diz (consultada em 2026-11-16):**

| Mecanismo | Fonte | O que garante | O que não garante |
|---|---|---|---|
| Conf de sessão `spark.databricks.delta.optimizeWrite.enabled=false` | Databricks, *Control data file size* ([docs.databricks.com/aws/en/delta/tune-file-size](https://docs.databricks.com/aws/en/delta/tune-file-size)): "`false` … Can be set at the session level to override optimized writes for all tables modified in the workload." | Vale no Databricks e vence a propriedade de tabela. | Isolamento entre tasks: *Set Spark configuration properties* ([docs.databricks.com/aws/en/spark/conf](https://docs.databricks.com/aws/en/spark/conf)) diz que o que se define num **notebook** vale "only the SparkSession for the current notebook", mas não fala de `spark_python_task` num job cluster compartilhado. |
| Opção do writer `.option("optimizeWrite", "false")` | Delta Lake open source ≥ 3.1 ([docs.delta.io/latest/optimizations-oss.html](https://docs.delta.io/latest/optimizations-oss.html)): precedência, de menor para maior, "table property → SQL configuration → DataFrameWriter option `optimizeWrite`". | A forma mais local (uma escrita). | A documentação do Databricks não a menciona; o DBR usa o Delta próprio. Opção desconhecida é ignorada em silêncio pelo DataFrameWriter: se o DBR 15.4 não a reconhecer, nada quebra, só não há ganho. |
| Propriedade de tabela `delta.autoOptimize.optimizeWrite=false` | Databricks, mesma página | — | Precedência contra a conf de sessão do cluster (`true`) não documentada no Databricks (R12/D8); pela regra do Delta OSS a conf de sessão **vence** a propriedade, então ela provavelmente não serviria. Descartada. |

Também da mesma página do Databricks: "In Databricks Runtime 13.3 LTS and above, all tables registered in Unity Catalog
have optimized writes enabled for CTAS statements and INSERT operations for partitioned tables." — mais um motivo para
desligar explicitamente, e não só tirar a conf do cluster.

**Escolha:** parâmetro `modo_optimize_write` de `ingerir_tabela_bronze` (ou variável de ambiente
`BRONZE_DESLIGAR_OPTIMIZE_WRITE`, lida só pela bronze; `src/bronze/optimize_write.py`), aplicado **só** às escritas
mensais:
- `nao` — **padrão**, comportamento atual (vale a conf do cluster);
- `writer` — opção `optimizeWrite=false` no DataFrameWriter da escrita mensal (preferida: não toca a sessão);
- `sessao` — conf de sessão `false` só durante o `saveAsTable`, restaurando o valor anterior num `finally`.

**Grau de certeza:** nenhuma fonte oficial garante, para DBR 15.4 em `spark_python_task`, que a opção do writer é
respeitada **nem** que a conf de sessão fica isolada da task vizinha. Por isso o padrão é `nao` (preserva o comportamento).
A medida da §10 decide; se `writer` funcionar, ligar por padrão (mudar `MODO_PADRAO` ou definir a variável no
`spark_env_vars` do cluster em `gerar_job.py`). O risco de vazamento no modo `sessao` é só de *layout* de arquivos em
outra task que escreva no mesmo instante (não muda linhas); ainda assim, só usar `sessao` se `writer` não funcionar.
A conf global do cluster (`gerar_job.py`) e o YAML do Job **não** mudaram.

### 9.3 P3 — DQ em uma passada por tabela

`testar_tabela_bronze` faz **uma** agregação sobre os meses da execução (`filter(mes_referencia IN meses)`, lendo
`mes_referencia`, `change_hash` e `pk`): `groupBy(mes, pk)` → linhas e hashes nulos por pk; depois soma por mês. Mais
uma leitura do `dq_metrics_log` (última linha por mês da tabela). A avaliação continua **mês a mês, em ordem**, e dentro
do mês na mesma ordem de antes. `validar_volumetria` foi dividida em `ler_historico_volumetria`,
`media_volumetria_anterior` (pura), `checar_volumetria` (pura) e `registrar_volumetria`.

| # | Asserção | Antes (laço por mês) | Agora | Mensagem |
|---|---|---|---|---|
| 1 | `colunas_esperadas` presentes | por mês, sobre o schema da tabela | uma vez, antes do laço, com o rótulo do 1º mês (onde a antiga falhava; o schema é o mesmo em todos os meses) | igual: `❌ Colunas ausentes em <tabela> (<mês>): <faltantes>` |
| 2 | Contrato `validar_schema` | por mês, `df_mes.select(colunas)` | uma vez, `df_tabela.select(colunas)` (só metadados) | igual; o `✓ Schema validado` sai 1× em vez de N× |
| 3 | Mês vazio | `df_mes.count() == 0` | `linhas` = soma das contagens por (mês, pk); mês ausente da agregação = 0 | igual: `🚨 FALHA: O arquivo de <t> no mês <m> está vazio.` |
| 4 | Queda volumétrica | consulta ao log por mês (`mes < '<m>'`, última linha por mês, top 3 desc, `AVG`) depois do `INSERT` do mês anterior | histórico lido 1×; cada mês aprovado entra nele em memória antes do seguinte; mesma regra de média (testada contra uma reprodução da consulta antiga) | igual: `🚨 ANOMALIA VOLUMÉTRICA em <t>!…` |
| 5 | (efeito) `INSERT` no log | por mês, depois da volumetria | **igual, por mês, no mesmo ponto** (não adotei o `INSERT` único da proposta: mudaria o que fica no log quando um mês falha) | — |
| 6 | `change_hash` nulo | `sum(when(hash nulo))` por mês | soma, por mês, dos nulos contados por (mês, pk) | igual: `❌ <n> registos com change_hash nulo em <t> (<m>)!` |
| 7 | PK nula/vazia (se a config tem pk) | `sum(when(pk nula ou trim(pk) = ''))` por mês | soma das linhas dos grupos cuja pk é nula ou vazia | igual: `❌ <n> registos com PK nula/vazia em <t> (<m>)!` |
| 8 | Aviso de pks repetidas (P1, não falha) | ingestão, `groupBy(pk)` sobre o resultado deduplicado | nº de grupos (mês, pk) com mais de uma linha | texto antigo + `(<mês>)` |

**Asserções que não entraram na passada única:** nenhuma de dados. As de schema (1, 2) não leem dados e passaram a
rodar uma vez. O `INSERT` do log (5) ficou por mês por escolha, para não mudar o efeito colateral.

**Diferenças de comportamento (só desempenho/efeito idempotente):** o `CREATE TABLE IF NOT EXISTS` do log roda uma vez,
antes do 1º mês (antes: depois da checagem de vazio do 1º mês); a agregação lê todos os meses antes de avaliar o 1º,
então um mês 1 com erro não poupa a leitura dos demais. A agregação por (mês, pk) embaralha a coluna `pk` (estreita) —
é o custo da contagem da P1, agora dentro da DQ (§3, P1).

## 10. Roteiro da execução de medida (para o usuário rodar; uma subida de cluster, ~30–45 min)

Substitui a tabela `bronze_teste` (T1, não implementada) por *time travel* do Delta na própria bronze: cada execução
regrava a mesma partição `2023-08` com o mesmo conteúdo (só `bronze_processed_at` muda), e as versões anteriores ficam
consultáveis com `VERSION AS OF`. Aceitável em desenvolvimento (D-08). Não rodar a silver no meio.

**Preparação**
1. Dar `push` da branch `worktree-otimizacao`. No workspace, dois *Git folders* do repo: `receita_antes` no commit
   `ef09865` (código antes de P1–P3) e `receita_depois` na branch `worktree-otimizacao`.
2. Cluster *all-purpose* com a mesma configuração do `CLUSTER` de `gerar_job.py` (DBR 15.4, `r5d.xlarge`, mesma
   `spark_conf`, incluindo `optimizeWrite.enabled=true`, `cluster_log_conf` no Volume), mas **`num_workers: 4` fixo**
   (sem autoscale) e `AWS_BUCKET_NAME` no ambiente. Desligar ao final.
3. Um notebook por pasta (ou um só, com `dbutils.library.restartPython()` ao trocar de pasta). Início de cada notebook:
   ```python
   import sys, time
   RAIZ = "/Workspace/Users/<você>/receita_antes"   # ou receita_depois
   sys.path.insert(0, RAIZ)
   from src.bronze.bronze_lib import ingerir_tabela_bronze, testar_tabela_bronze
   from src.bronze.configs.estabelecimentos import CONFIG as ESTAB
   from src.bronze.configs.empresas import CONFIG as EMPR
   sc = spark.sparkContext

   def versao(t):
       return spark.sql(f"DESCRIBE HISTORY tcc_receita.bronze.{t} LIMIT 1").first()["version"]

   def medir(grupo, fn, *args, **kw):
       sc.setJobGroup(grupo, grupo)
       t0 = time.time(); fn(*args, **kw); dt = time.time() - t0
       jobs = sc.statusTracker().getJobIdsForGroup(grupo)
       print(f"{grupo}: {dt:,.0f} s, {len(jobs)} jobs Spark")
   ```

**Passos**
1. `v0 = versao("estabelecimentos")` (versão atual, gravada pelo código antigo no run de 11-08).
2. **(a) código antigo** (`receita_antes`): `medir("a_estab_antigo", ingerir_tabela_bronze, spark, ESTAB, "2023-08", "2023-08")`;
   `va = versao("estabelecimentos")`. Esperado: 2+ jobs de dados no mês (contagem + escrita).
3. **(b) P1+P2 `writer`** (`receita_depois`):
   `medir("b_estab_writer", ingerir_tabela_bronze, spark, ESTAB, "2023-08", "2023-08", modo_optimize_write="writer")`;
   `vb = versao(...)`. Na Spark UI (aba SQL, consulta do `saveAsTable` do grupo `b_estab_writer`): se ainda houver o
   estágio/`Exchange` do optimized write antes da escrita (compare com o plano do grupo `a_estab_antigo`), a opção foi
   ignorada → rodar **(b')** igual com `modo_optimize_write="sessao"` e, depois,
   `spark.conf.get("spark.databricks.delta.optimizeWrite.enabled")` tem de voltar a `"true"` (restauração).
4. **(c) empresas** com o modo que funcionou: `medir("c_empr", ingerir_tabela_bronze, spark, EMPR, "2023-08", "2023-08", modo_optimize_write=<modo>)`
   (tabela estreita; comparar shuffle com o B17: ~7 GB/mês antes).
5. **(d) DQ antiga × nova** sobre a mesma bronze, 4 meses de estabelecimentos:
   `medir("d_dq_antiga", testar_tabela_bronze, spark, ESTAB, "2023-05", "2023-08")` em `receita_antes` e
   `medir("d_dq_nova", ...)` em `receita_depois`. As linhas `📊 Volumetria atual` e as médias impressas têm de ser
   iguais mês a mês; o aviso de pks repetidas da nova deve bater com o que a ingestão antiga imprimiu em (a) para 2023-08.
   As duas inserem 4 linhas no `dq_metrics_log` (inócuo: vale a mais recente, com a mesma contagem).
6. **Queda forçada** (dos dois lados): `from dataclasses import replace`;
   `testar_tabela_bronze(spark, replace(ESTAB, limite_queda_volumetria=-1.0), "2023-05", "2023-08")` — limite negativo faz
   o mínimo virar 2× a média, então o 1º mês com histórico tem de falhar. A antiga e a nova têm de falhar **no mesmo
   mês com a mesma mensagem**.
7. **Q3 (vazamento de conf), opcional e barato:** num notebook A no cluster, `spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")`;
   num notebook B anexado ao mesmo cluster, `spark.conf.get(...)` deve continuar `"true"`. Isso testa notebooks; para
   `spark_python_task` a resposta só vem do Job (motivo para preferir `writer`).

**Impressão digital por mês** (em SQL no mesmo cluster; os três resultados têm de ser idênticos):
```sql
SELECT 'v0' AS versao, COUNT(*) AS linhas,
       SUM(CAST(XXHASH64(pk, change_hash, source_reference) AS DECIMAL(38,0))) AS digital
FROM tcc_receita.bronze.estabelecimentos VERSION AS OF <v0> WHERE mes_referencia = '2023-08'
UNION ALL
SELECT 'a', COUNT(*), SUM(CAST(XXHASH64(pk, change_hash, source_reference) AS DECIMAL(38,0)))
FROM tcc_receita.bronze.estabelecimentos VERSION AS OF <va> WHERE mes_referencia = '2023-08'
UNION ALL
SELECT 'b', COUNT(*), SUM(CAST(XXHASH64(pk, change_hash, source_reference) AS DECIMAL(38,0)))
FROM tcc_receita.bronze.estabelecimentos VERSION AS OF <vb> WHERE mes_referencia = '2023-08';
-- T0 na versão nova:
SELECT COUNT(*) = COUNT(DISTINCT pk, change_hash) AS t0_ok
FROM tcc_receita.bronze.estabelecimentos VERSION AS OF <vb> WHERE mes_referencia = '2023-08';
```
Se a digital divergir: `EXCEPT ALL` da §4.1 entre `VERSION AS OF <va>` e `<vb>`, nos dois sentidos. Idem para empresas.

**O que olhar e anotar (em `docs/medicoes/`)**
- Tempo de parede impresso por `medir` para a, b (b'), c, d_antiga, d_nova; nº de jobs Spark por grupo (esperado: a
  ingestão nova com **1** job de dados por mês, fora pequenos jobs de metadados).
- Spark UI → Stages, filtrando pelo grupo: soma de *Shuffle Write* (esperado: de ~3 para ~1 shuffle de linha inteira) e
  *Total Time Across All Tasks* (CPU dos executores). O event log no Volume `/Volumes/tcc_receita/ops/cluster_logs/<cluster-id>/`
  traz o mesmo (eventos `SparkListenerTaskEnd`: *Executor Run Time*, *Shuffle Bytes Written*), para guardar.
- `DESCRIBE HISTORY tcc_receita.bronze.estabelecimentos` nas versões `va` e `vb`: `operationMetrics.numFiles`,
  `numOutputBytes`, `numOutputRows` (mesmas linhas; mais arquivos e tamanho médio ≥ ~64 MB — Q6).
- Critérios da §6: tempo de (b) ≥ 40% menor que (a); digital idêntica; T0; DQ igual.

**Depois:** com o modo vencedor, ligar por padrão (§9.2), registrar Q3/Q6 nesta spec e B17 no backlog.
