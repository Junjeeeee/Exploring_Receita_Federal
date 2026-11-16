# Otimização da pipeline antes da janela completa

Status: **em andamento** (aprovado pelo usuário em 2026-11-16). Branch: `otimizacao_pipeline`.

Spec guarda-chuva. O desenho detalhado de cada fase fica em spec própria; a revisão crítica das fases 2 e 3
está em [`2026-11-16-otimizacao-revisao.md`](2026-11-16-otimizacao-revisao.md).

| Fase | Spec | Estado |
|---|---|---|
| 0 — Linha de base | [`../../medicoes/2026-11-16-linha-de-base.md`](../../medicoes/2026-11-16-linha-de-base.md) | ⏸ esqueleto; espera o login do Databricks |
| 1 — Ganhos baratos | esta spec, §3 | 🔧 código pronto; bronze ainda não executada |
| 2 — SCD2 da silver | [`2026-11-16-otimizacao-scd2.md`](2026-11-16-otimizacao-scd2.md) | 📝 desenho revisado; implementação espera a amostra |
| 3 — Log, fato e dashboard | [`2026-11-16-otimizacao-log-fato-dashboard.md`](2026-11-16-otimizacao-log-fato-dashboard.md) | 📝 desenho revisado; implementação espera a amostra |
| 4 — Execução em blocos | esta spec, §4 | ⬜ |

## 1. Requisito

Rodar a janela completa da Receita (~40 meses, 2023-05 em diante) para ter resultados e análises mais
robustos, com custo e tempo previsíveis. Hoje só 4 meses foram processados e o custo é dominado por
volume de shuffle, não por falta de memória (diagnóstico B20 em `docs/backlog_historico.md`):

| Etapa | Medido em 4 meses | Causa apontada |
|---|---|---|
| `si_estabelecimentos` | 38 min, 76 GB lidos, 97 GB de shuffle | bronze lida 2–3× dentro do MERGE |
| `si_dados_simples` | 28 min | mesmo padrão |
| `si_socios` | 25 min, 69 GB | mesmo padrão |
| `si_log_eventos_negocio` | 873 s (era 241 s) | cada SCD2 lida 4–5× |
| `gold_fato_empresa_mes` | 84 GB de shuffle para 0,9 GB de entrada | expansão empresa × mês |
| `sf_dim_empresa` | única com spill (7,6 GB) | joins por intervalo |
| Tabelas estáticas | 3–5 min cada | um commit Delta por comentário de coluna |

## 2. Princípios

- **Resultado idêntico.** Otimização não muda número. Cada mudança é validada na amostra de 1%
  (`scripts/amostra/`) comparando a tabela nova com a atual, linha a linha (`EXCEPT` nos dois sentidos = 0).
  Onde a otimização corrige um bug conhecido (C-05, C-06), a diferença esperada é descrita na spec da fase.
- **Desempenho se mede na base real**, não na amostra: uma execução de 4 meses ao fim das fases 2 e 3,
  comparada com a linha de base da fase 0.
- **Sem serverless no Job** (decisão do usuário, custo). Cluster clássico spot. O SQL warehouse serverless
  2X-Small segue sendo usado só para a amostra e consultas pontuais.
- **Compatibilidade com Spark 3.5 / DBR 15.4**: nada de função que só existe no warehouse.
- Landing fica **fora**: roda no computador do usuário e custa quase nada (decisão do usuário, 2026-11-16).
- Bronze (B17) fica **fora** desta rodada, a menos que a linha de base mostre que ela domina.
- Regras do `CLAUDE.md` continuam valendo (LGPD, bronze sem regra de negócio, NULL ≠ FALSE etc.).

## 3. Fase 1 — ganhos baratos

1. Comentários de coluna aplicados sem um commit Delta por coluna (no `CREATE` ou em um único comando),
   nas tabelas estáticas e onde mais o padrão `ALTER COLUMN ... COMMENT` repetido aparecer.
   - [x] **Preparado, não executado.** O padrão estava na bronze (PySpark), não nos SQL:
     `aplicar_comentarios_uc` (`src/bronze/bronze_lib.py`) reaplicava `COMMENT ON TABLE` + um
     `ALTER COLUMN ... COMMENT` por coluna a cada execução, nas 11 tabelas (estáticas e mensais). Agora compara
     com o que já está gravado (comentário da tabela e `metadata["comment"]` do schema persistido) e só aplica
     o que difere: 1ª execução igual a antes; nas seguintes, nenhum commit. A escrita em overwrite sem
     `overwriteSchema` preserva o schema, comentários incluídos. Não houve como medir localmente (sem PySpark e
     sem acesso ao histórico da bronze real): confirmar no próximo run pelo log da task
     (`Comentários de coluna aplicados: 0`) e pelo `DESCRIBE HISTORY` (sem `SET COLUMN COMMENT` novo).
   - SQL: só `gold/fato_empresa_mes.sql` tem `ALTER COLUMN ... COMMENT` (5 colunas, D-17/D-17b), num task
     dominado por shuffle; ficou como está (arquivo da fase 3). Os demais SQL já comentam no `CREATE`.
2. Checagem automática de compatibilidade dos SQL com o cluster do Job: script em `scripts/` que falha
   ao achar construções que quebraram na base real (`TRY_TO_DATE`, `OPTIMIZE ... ZORDER BY` em tabela com
   clustering automático) e outras só de warehouse; com teste em `tests/`.
   - [x] **Implementado:** `scripts/checar_sql_cluster.py` + `tests/test_checar_sql_cluster.py` (regras e
     fontes no próprio script; uso em `src/jobs/README.md`). O repo atual passa.
3. Revisão do cluster do Job (tipo de instância, partições de shuffle, AQE) — proposta com justificativa
   a partir da linha de base; mudança só depois de medida. Proposta em §3.3.

### 3.3 Proposta de cluster

**Hoje** (`src/jobs/gerar_job.py`, `CLUSTER`): DBR 15.4 LTS sem Photon; workers `r5d.xlarge` spot
(`SPOT_WITH_FALLBACK`, `first_on_demand: 1` → driver on-demand), driver `m5d.xlarge`, autoscale 1–4;
`spark.sql.shuffle.partitions=auto` (auto-optimized shuffle do Databricks, sobre o AQE, que é padrão no
Spark 3.5); `optimizeWrite` ligado no cluster todo; scheduler `FAIR`.

**O que a evidência (B20/B17) diz:** spill ≈ 0 e GC baixo em quase tudo (exceção: `sf_dim_empresa`,
7,6 GB de spill); o custo é volume de shuffle (I/O de disco local + rede + CPU de serialização), e a
ocupação dos cores na bronze ficou em ~40–60%. Ou seja: memória sobrando, CPU nem sempre ocupada.

| Opção (worker) | vCPU / RAM / disco local (AWS) | Leitura |
|---|---|---|
| `r5d.xlarge` (atual) | 4 / 32 GiB / 1×150 GB NVMe | 8 GiB por core; memória ociosa segundo B20. |
| **`m5d.xlarge`** | 4 / 16 GiB / 1×150 GB NVMe | Mesma CPU e mesmo disco de shuffle, metade da RAM; hora mais barata que a `r5d` (EC2 e DBU — conferir na tabela de preços antes de decidir). **Candidata principal.** Risco: mais spill em `sf_dim_empresa` (vai para o NVMe; aceitável se o tempo não piorar). |
| `c5d.xlarge` | 4 / 8 GiB / 1×100 GB NVMe | 2 GiB por core é pouco para MERGE de SCD2 com ~100 GB de shuffle por task; disco menor. **Não recomendada** sem antes cair o volume de shuffle (fases 2–3). |
| `m5d.2xlarge` (2 workers) | 8 / 32 GiB / 1×300 GB NVMe | Mesmos cores totais com metade dos nós: menos shuffle pela rede e menos pontos de interrupção spot. Alternativa a testar se a rede pesar. |

Manter família **"d"** (NVMe local): shuffle e spill vão para o disco de instância, sem EBS. Preços não
citados aqui por falta de fonte conferida; comparar pelo custo real do run (abaixo).

Demais parâmetros:
- **Workers / autoscale:** o autoscale de job cluster reage em minutos e o pipeline alterna fases largas
  (SCD2, fato) e estreitas (dimensões, DQ). Se a linha de base mostrar executores ociosos ou escalonamento
  atrasado nas tasks pesadas, testar `min_workers: 2`; mais workers só ajudam se os cores estiverem cheios
  (com 40–60% de ocupação, o gargalo é a forma do plano, não o tamanho do cluster).
- **Partições de shuffle / AQE:** manter `auto`. Conferir no event log se as partições pós-AQE ficam na
  faixa de ~100–200 MB; se o `auto` errar muito para cima (tasks minúsculas) ou para baixo (spill), fixar
  `spark.sql.shuffle.partitions` em 2–3× o total de cores para o teste. Skew join do AQE já vem ligado.
- **`optimizeWrite` global:** custa um shuffle extra por escrita (B17 mediu na bronze). Avaliar desligar por
  sessão nas escritas que já saem particionadas por mês; mudança separada, medida à parte.
- **Spot:** manter `SPOT_WITH_FALLBACK` com driver on-demand. Perder um worker "d" perde os arquivos de
  shuffle dele (recomputação); contar `executor lost` nos eventos do cluster no piloto de 12 meses.
- **Photon:** fora desta rodada (DBU mais cara; só vale testar depois que o volume de shuffle cair).

**O que medir para decidir** (event log em `/Volumes/tcc_receita/ops/cluster_logs/<cluster_id>/eventlog/`,
eventos do cluster e `system.billing.usage` filtrado pelo `job_run_id`):
1. Pico de memória por executor (`JVMHeapMemory`/`OnHeapExecutionMemory` em `SparkListenerStageExecutorMetrics`)
   e `peakExecutionMemory` por task, nas tasks pesadas. Se o pico ficar < ~50% do heap da `r5d` e o spill
   ≈ 0 → trocar para `m5d.xlarge`.
2. Ocupação dos cores por task: soma de `executorRunTime` ÷ (cores ativos × duração). Abaixo de ~60% →
   não aumentar workers; atacar o plano (fases 2–3).
3. Linha do tempo de workers ativos (eventos `RESIZING`/`UPSIZE_COMPLETED`) contra as tasks pesadas.
4. Shuffle e spill (memória e disco) por task; uso do disco local nas tasks de MERGE.
5. Custo do run (DBU + EC2) e tempo total, mesma janela de 4 meses.

**Como testar:** uma variável por vez, depois das fases 2–3 (para não confundir os ganhos), mesma janela de
4 meses da linha de base: (a) `m5d.xlarge` 1–4; (b) se (a) não piorar o tempo, `min_workers: 2`. O YAML só
muda depois da medida (via `gerar_job.py`).

## 4. Fase 4 — execução em blocos

1. Job parametrizado para rodar blocos de meses (C-21: timeout de 4 h não comporta a janela inteira).
2. Reextração de `empresas` na landing com a máscara ampliada (D-16), **antes** da carga da janela completa
   (senão a cadeia inteira teria de ser reprocessada de novo).
3. Piloto: **12 meses** (2023-05 a 2024-04). Medir custo e tempo por mês; só então liberar o restante.
4. NAT: ligar antes e desligar depois de cada execução (`scripts/aws/`).

## 5. Critérios de aceite

- [ ] Linha de base publicada, com tempo, leitura e shuffle por tarefa.
- [ ] Amostra: tabelas das fases 2 e 3 idênticas às atuais (ou diferença explicada na spec).
- [ ] Base real, 4 meses: tempo total da cadeia silver → gold cai pelo menos pela metade.
- [ ] Custo por mês processado conhecido e estável do 1º ao 12º mês do piloto.
- [ ] Dashboard com 12 meses: cada página responde em tempo aceitável a um clique de filtro (o Lakeview não corta
      dataset; acima de 100 mil linhas ou 100 MB cada interação roda a consulta inteira no warehouse).
