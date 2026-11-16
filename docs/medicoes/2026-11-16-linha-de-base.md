# Linha de base de desempenho (fase 0)

Medido em 2026-11-16, só leitura (CLI `jobs get-run` + SQL warehouse serverless; nenhum cluster nem Job). Legenda:
**[M]** medido agora, **[B20]** herdado do B20 (event log do run de 2026-11-08, não reconferido), **[E]** estimado.
Job `tcc_receita_pipeline` (1078568473913951): cluster clássico DBR 15.4, driver `m5d.xlarge`, workers `r5d.xlarge`
1–4 spot, `optimizeWrite=true`, `shuffle.partitions=auto`.

Fora do escopo por decisão do usuário: landing (tempo por mês e tamanho no S3).

## 1. Runs [M]

| Run | Data | Resultado | Parede (s) | Conteúdo |
|---|---|---|---|---|
| 183708620624854 | 2026-11-08 | SUCCESS (após reparos) | 16.622 | bronze + silver + gold completos, 4 meses (execução de referência do B20) |
| 440118712899237 | 2026-11-14 | FAILED | 2.572 | silver_final + gold; falhou só em `gold_dashboard_mercado_ativo` |
| 1097845690343814 | 2026-11-14 | SUCCESS | 604 | `gold_dashboard_mercado_ativo` + `gold_dq_gold` |
| 297380538835106 | 2026-11-14 | SUCCESS | 975 | `gold_fato_empresa_mes`, `gold_fato_indicadores_setoriais_mes`, `gold_dq_gold` |
| 1044171436918294 | 2026-11-15 | SUCCESS | 438 | `sf_dq_dim_cnae` |

Outros runs curtos: 17768949049037 (FAILED, 237 s), 1015377329218232 (CANCELED, 954 s), 534507222369452 e 548650982706722 (FAILED, 236 s e 413 s).

## 2. Duração por tarefa (s de execução, sem subida de cluster) [M]

Colunas: run completo de 11-08, depois 440118…, 1097845…, 297380…, 1044171… ("–" = a tarefa não teve sucesso nesse run).
Só tentativas com sucesso. A soma das tarefas (23.524 s) é maior que a parede do run (16.622 s) porque as tarefas
rodam em paralelo e em vários clusters.

| Tarefa | 11-08 | % da soma | 440118 | 1097845 | 297380 | 1044171 |
|---|---|---|---|---|---|---|
| `bronze_estabelecimentos` | 3317 | 14.1% | – | – | – | – |
| `bronze_dados_simples` | 2799 | 11.9% | – | – | – | – |
| `si_estabelecimentos` | 2334 | 9.9% | – | – | – | – |
| `si_dados_simples` | 1741 | 7.4% | – | – | – | – |
| `bronze_empresas` | 1677 | 7.1% | – | – | – | – |
| `si_socios` | 1576 | 6.7% | – | – | – | – |
| `bronze_socios` | 1533 | 6.5% | – | – | – | – |
| `sf_dim_empresa` | 780 | 3.3% | 768 | – | – | – |
| `gold_fato_empresa_mes` | 614 | 2.6% | 593 | – | 689 | – |
| `bronze_ceps` | 493 | 2.1% | – | – | – | – |
| `bronze_motivos_situacao` | 482 | 2.0% | – | – | – | – |
| `si_empresas` | 473 | 2.0% | – | – | – | – |
| `bronze_cnaes_ibge` | 418 | 1.8% | – | – | – | – |
| `gold_dashboard_mercado_ativo` | 404 | 1.7% | – | 398 | – | – |
| `bronze_paises` | 386 | 1.6% | – | – | – | – |
| `sf_fato_mudanca_estado` | 322 | 1.4% | 550 | – | – | – |
| `bronze_qualificacoes_socios` | 322 | 1.4% | – | – | – | – |
| `bronze_municipios` | 320 | 1.4% | – | – | – | – |
| `bronze_naturezas_juridicas` | 318 | 1.4% | – | – | – | – |
| `si_log_eventos_negocio` | 241 | 1.0% | 873 | – | – | – |
| `bronze_dq_dados_simples` | 226 | 1.0% | – | – | – | – |
| `sf_dim_grupo_cnae` | 203 | 0.9% | 465 | – | – | – |
| `sf_ponte_cnae` | 202 | 0.9% | 184 | – | – | – |
| `bronze_dq_estabelecimentos` | 165 | 0.7% | – | – | – | – |
| `gold_fato_indicadores_macro_mes` | 164 | 0.7% | 159 | – | – | – |
| `gold_fato_indicadores_setoriais_mes` | 146 | 0.6% | 72 | – | 47 | – |
| `bronze_dq_motivos_situacao` | 116 | 0.5% | – | – | – | – |
| `gold_dq_gold` | 115 | 0.5% | – | 89 | 121 | – |
| `bronze_dq_socios` | 114 | 0.5% | – | – | – | – |
| `bronze_dq_empresas` | 109 | 0.5% | – | – | – | – |
| `bronze_dq_naturezas_juridicas` | 106 | 0.5% | – | – | – | – |
| `sf_dim_tempo` | 103 | 0.4% | 45 | – | – | – |
| `bronze_dq_municipios` | 101 | 0.4% | – | – | – | – |
| `bronze_dq_ceps` | 100 | 0.4% | – | – | – | – |
| `sf_dim_mudanca_estado` | 97 | 0.4% | 69 | – | – | – |
| `bronze_dq_qualificacoes_socios` | 96 | 0.4% | – | – | – | – |
| `gold_obt_dashboard_eventos` | 95 | 0.4% | 192 | – | – | – |
| `sf_dq_dim_empresa` | 89 | 0.4% | 198 | – | – | – |
| `si_dq_log_eventos_negocio` | 86 | 0.4% | 32 | – | – | – |
| `sf_dq_dim_cnae` | 72 | 0.3% | 38 | – | – | 85 |
| `sf_dq_ponte_cnae` | 55 | 0.2% | 55 | – | – | – |
| `bronze_dq_paises` | 47 | 0.2% | – | – | – | – |
| `sf_dq_fato_mudanca_estado` | 47 | 0.2% | 25 | – | – | – |
| `si_dq_socios` | 44 | 0.2% | – | – | – | – |
| `sf_dim_cnae` | 44 | 0.2% | 144 | – | – | – |
| `sf_dq_dim_mucanca_estado` | 42 | 0.2% | 23 | – | – | – |
| `si_dq_dados_simples` | 38 | 0.2% | – | – | – | – |
| `si_dq_empresas` | 37 | 0.2% | – | – | – | – |
| `si_dq_estabelecimentos` | 37 | 0.2% | – | – | – | – |
| `bronze_dq_cnaes_ibge` | 21 | 0.1% | – | – | – | – |
| `gold_d_coorte_sobrevivencia` | 20 | 0.1% | 14 | – | – | – |
| `sf_dq_dim_grupo_cnae` | 19 | 0.1% | 33 | – | – | – |
| `gold_ds_visao_macro` | 18 | 0.1% | 10 | – | – | – |

**Subida de cluster [M]:** `setup_duration` aparece em várias tarefas do run de 11-08 (soma 3.308 s; típico 114–145 s e
320–354 s) e em poucas dos runs de 11-14 (115–325 s). Não está incluída nas durações acima; tarefas que caem em cluster novo pagam essa espera.

## 3. Onde o tempo vai (run de 11-08, soma de 23.524 s de execução) [M]

| Grupo | s | % |
|---|---|---|
| bronze (12 tarefas; as 4 grandes somam 9.326 s, 39,6%) | 12.065 | 51,3% |
| silver_intermediate (`si_*` + log) | 6.365 | 27,1% |
| silver_final (`sf_*`) | 1.751 | 7,4% |
| gold | 1.461 | 6,2% |
| DQ (bronze 1.201 + si 242 + sf 324 + gold 115) | 1.882 | 8,0% |

Observações: (a) a bronze é a maior fatia **neste run**, ao contrário do que a spec guarda-chuva assume (bronze fora
da rodada); é carga inicial dos 4 meses e se repete só por mês novo (linear), mas merece reavaliação no B17. (b) Na cadeia
silver → gold, as 8 mais lentas (último run com sucesso de cada tarefa): `si_estabelecimentos` 2.334, `si_dados_simples` 1.741,
`si_socios` 1.576, `si_log_eventos_negocio` 873 (run 11-14), `sf_dim_empresa` 780, `gold_fato_empresa_mes` 689,
`sf_fato_mudanca_estado` 550, `si_empresas` 473. (c) `si_log_eventos_negocio` foi de 241 s (11-08) para 873 s (11-14) e
`sf_fato_mudanca_estado` de 323 para 550 s, como no B20. (d) DQ pesa 8%; `sf_dq_dim_empresa` subiu de 89 para 198 s entre os dois runs.

## 4. Leitura, shuffle e spill por tarefa

**Não medível sem cluster.** `system.query.history` só cobre SQL warehouse (desde 11-08: 1.040 consultas, 111 GB lidos,
shuffle 0, spill 0; irrelevante para o Job) e `job_task_run_timeline` não traz bytes. Valores herdados **[B20]**:

| Tarefa | Valor |
|---|---|
| `si_estabelecimentos` | 76 GB lidos, 97 GB de shuffle |
| `si_socios` | 69 GB lidos |
| `gold_fato_empresa_mes` | 84 GB de shuffle para 0,9 GB de entrada |
| `sf_dim_empresa` | spill 7,6 GB (única com spill) |
| estáticas | 3–5 min cada, sem trabalho de executor |

## 5. DBU e custo de lista por run [M]

`system.billing.usage` × `list_prices` (SKU PREMIUM_JOBS_COMPUTE), filtrado por `job_run_id`. **Só custo Databricks (DBU);
não inclui as instâncias EC2 (spot) cobradas pela AWS.**

| Run | DBU | USD de lista |
|---|---|---|
| 183708620624854 (completo, 11-08) | 12,34 | 1,85 |
| 440118712899237 | 2,44 | 0,37 |
| 1097845690343814 | 0,42 | 0,06 |
| 297380538835106 | 0,79 | 0,12 |
| 1044171436918294 | 0,12 | 0,02 |
| 4 runs falhos/cancelados (11-08: 0,21 DBU; 11-14: 0,58 DBU) | 0,79 | 0,11 |

## 6. Volumes [M]

Linhas por `COUNT(*)` ou soma por mês; bytes por `DESCRIBE DETAIL`. Bronzes particionadas por `mes_referencia`.

| Tabela | Linhas | GB | Observação |
|---|---|---|---|
| bronze.estabelecimentos | 228,1 mi (56,5 / 56,8 / 57,2 / 57,6 por mês) | 35,2 | 128 arquivos |
| bronze.empresas | 216,6 mi (53,7 / 54,0 / 54,3 / 54,7) | 22,0 | |
| bronze.socios | 91,7 mi (22,8 / 22,9 / 23,0 / 23,1) | 12,1 | |
| bronze.dados_simples | 143,2 mi (35,3 / 35,6 / 35,9 / 36,3) | 11,5 | |
| bronze.ceps | 905 mil | 0,08 | demais estáticas < 6 mil linhas |
| silver_intermediate.estabelecimentos | 59,0 mi | 9,9 | liquid clustering automático |
| silver_intermediate.empresas | 55,0 mi | 5,8 | liquid clustering automático |
| silver_intermediate.socios | 24,1 mi | 3,2 | partição `is_current` |
| silver_intermediate.dados_simples | 36,8 mi | 3,1 | |
| silver_intermediate.log_eventos_negocio | 5,79 mi | 0,09 | |
| silver_final.dim_empresa | 56,4 mi | 7,3 | |
| silver_final.ponte_cnae | 55,6 mi | 2,7 | |
| silver_final.dim_grupo_cnae | 6,96 mi | 0,46 | |
| silver_final.fato_mudanca_estado | 5,79 mi | 0,17 | |
| silver_final.dim_mudanca_estado / dim_cnae / dim_tempo | 350 mil / 1.334 / 96 | < 0,05 | |
| gold.fato_empresa_mes | 90,9 mi (22,29 / 22,65 / 22,83 / 23,13 por mês) | 2,5 | |
| gold.dashboard_mercado_ativo | 22,9 mi | 1,9 | |
| gold.obt_dashboard_eventos | 5,79 mi | 0,25 | |
| gold.fato_indicadores_setoriais_mes | 320 mil | 0,006 | |
| gold.fato_indicadores_macro_mes | 48 | ~0 | |
| gold.ds_coorte_sobrevivencia / ds_visao_macro (views) | 54,7 mi / 124 mil | – | |

## 7. Confirmação das hipóteses das specs [M]

Consultas só de leitura no warehouse, uma passada por bronze (agregação por mês), sem colunas pessoais.
Fora (exigem cluster ou escrita): D8 (precedência do `optimizeWrite`), `EXPLAIN FORMATTED`, `DEEP CLONE`, testes na amostra.

| Item | Resultado | Hipótese |
|---|---|---|
| T0: `(pk, mes, change_hash)` única na bronze | `linhas = DISTINCT(pk, change_hash)` nos 16 pares tabela x mês | **confirmada** |
| D1: pks repetidas por `(pk, mês)` (B16) | estabelecimentos e dados_simples: 0. empresas: 2–3 linhas extras por mês; socios: 6 por mês. Mesmo `bronze_processed_at` nas repetidas, hashes diferentes | **confirmada como pequena**, mas existe em empresas e socios: a R1 precisa de desempate determinístico, senão o MERGE novo pode gerar `(pk, valid_since)` duplicado |
| D2 / T0: pks sem corrente única; versão fechada sem sucessora; `(pk, valid_since)` duplicada | 0 / 0 / 0 nas 4 SCD2 | **confirmada** (I14 vale hoje) |
| Q2 / D7: versões novas por mês (calibra os 20% do mês denso) | estabelecimentos 774 / 771 / 965 mil (1,4 / 1,4 / 1,7% da bronze do mês); empresas 388 / 405 / 515 mil (0,7–0,9%); dados_simples 480 / 459 / 562 mil (1,3–1,5%); socios 396 / 424 / 531 mil (1,7–2,3%) (jun / jul / ago) | **confirmada**: 98–99% das linhas não abrem versão; o mês mais denso tem ≤ 2,3%, longe de 20%. A estimativa da spec para estabelecimentos (836 mil/mês) bate com a média real (836 mil) |
| D3 / Q1: deletion vectors | `delta.enableDeletionVectors = true` nas 4 SCD2 e na bronze de estabelecimentos | **já ligados**: a D4 da F2 está decidida; "ligar depois da T5" perde o objeto |
| D3 / R18: clustering e otimização preditiva | `empresas` e `estabelecimentos` com `clusterByAuto`; histórico de estabelecimentos mostra `OPTIMIZE`, `COMPUTE STATS` (11-08) e `VACUUM` (11-15) | **confirmada**: a otimização automática já age; `SHALLOW CLONE` de referência se perderia, então vale o `DEEP CLONE` da §6.2 |
| Q6: `DESCRIBE HISTORY` | empresas: CREATE, MERGE, CLUSTER BY, SET TBLPROPERTIES, OPTIMIZE; dados_simples e socios: CREATE + 1 MERGE | informativo |
| D4: tamanho de `dim_grupo_cnae` (difundir?) | 6,96 mi linhas, 0,46 GB (`ponte_cnae`: 55,6 mi, 2,7 GB) | **NÃO confirmada** a hipótese de que talvez coubesse em broadcast: é grande (cardinalidade alta, como no C-11); a dica `BROADCAST` não serve ao join de CNAE do fato |
| D5: CNPJs com mais de uma ordem matriz | 256 | **confirmada como pequena** |
| D6: meses distintos no gold | `fato_empresa_mes` 4, `fato_indicadores_setoriais_mes` 4 | **confirmada** |
| F3: versões correntes não ATIVA em `dim_empresa` | 2023-05: 31,2 mi de 53,7 mi versões (58%); jun 189 mil, jul 169 mil, ago 210 mil | **confirmada**: o Corte A poupa mais da metade da expansão na carga inicial |
| F3: linhas de fluxo por mês | 487 / 477 / 598 mil (jun / jul / ago) contra 22,7–23,1 mi de linhas do fato | **confirmada**: ~2% do fato |
| F3: bairros distintos (limite 100 mil do dropdown, U7) | 89.189 | **confirmada, com folga curta** (89% do limite; `dashboard_mercado_ativo` guarda só o corrente, 22,9 mi linhas) |

## 8. Projeção ingênua para 12 e 40 meses [E]

Premissas: os números medidos são de **4 meses** (2023-05..08, carga inicial). Trabalho separado em (a) **linear no
nº de meses** (bronze, fato, gold), (b) que **cresce com o histórico** (log, dim_empresa) e (c) **quase constante**
(dimensões, estáticas, DQ pequeno). Sem otimização, tudo de uma vez, escala simples sobre os 4 meses; a soma de tarefas
ignora paralelismo.

| Componente | 4 meses [M] | 12 meses [E] | 40 meses [E] | Regra |
|---|---|---|---|---|
| bronze das 4 tabelas grandes | 9.326 s | ~28.000 s | ~93.000 s | linear no nº de meses |
| `si_*` (4 SCD2, carga inicial) | 6.124 s | ~18.000 s | ~61.000 s | linear; cada mês novo abre ~1,5% de versões |
| `si_log_eventos_negocio` | 873 s | ≥ 2.600 s | ≥ 8.700 s | cota inferior: o LAG percorre a SCD2 toda, tende a ser pior que linear |
| `gold_fato_empresa_mes` | 614–689 s; 90,9 mi linhas; 84 GB de shuffle [B20] | ~2.000 s; ~270 mi linhas; ~250 GB | ~6.800 s; ~910 mi linhas; ~840 GB | linear (empresa x mês); o painel recalcula o fato inteiro |
| `sf_dim_empresa` | 768 s; spill 7,6 GB [B20] | ≥ 2.300 s | ≥ 7.700 s | cota inferior: expande versões por mês; spill deve piorar |
| DQ, estáticas, dimensões pequenas, demais gold | ~2.600 s | ~3.000 s | ~4.000 s | quase constante |

Soma ingênua de execução (não é parede): 4 meses ~23.500 s (6,5 h); 12 meses ~56.000 s (15,5 h); 40 meses ~180.000 s (50 h).
Mesmo com paralelismo, passa do timeout de 4 h (C-21). Custo de lista só de DBU, linear a partir de 12,34 DBU / US$ 1,85: ~US$ 5,5 (12 meses)
e ~US$ 18 (40 meses), sem EC2. Armazenamento bronze (4 grandes): 81 GB, ~243 GB e ~810 GB (4, 12, 40 meses); `fato_empresa_mes`: 2,5, ~7,4 e ~25 GB.

## 9. Não medido e por quê

- **Landing (tempo por mês e tamanho no S3):** fora do escopo, por decisão do usuário.
- **Leitura, shuffle e spill por tarefa do Job:** exigem event log/Spark UI do cluster; só os valores herdados do B20 (§4).
- **Tarefas `bronze_*` e `si_*` em mais de um run:** só têm sucesso no run de 11-08, então não há repetição para medir variação.
- **Custo EC2:** não está em `system.billing.usage`; precisa de Cost Explorer.
- **D8, `EXPLAIN`, `DEEP CLONE`, comparação na amostra:** exigem cluster ou escrita; ficam para as fases 2 e 3.
- **Projeções (§8):** estimativas lineares; o crescimento real de log e `dim_empresa` pode ser maior.
