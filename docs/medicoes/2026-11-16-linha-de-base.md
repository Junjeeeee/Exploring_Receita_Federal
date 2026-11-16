# Linha de base de desempenho (fase 0)

Status: **INCOMPLETA — bloqueada por autenticação** (2026-11-16). O perfil `tcc` do Databricks CLI está com o
refresh token inválido (`databricks auth login --profile tcc` exige navegador) e o usuário IAM `tcc-rede-claude`
não tem `s3:ListAllMyBuckets`. Nenhuma medição nova foi feita; abaixo só o que já estava registrado.

## Herdado do B20 (event log do run de 2026-11-08, 4 meses)

| Tarefa | Valor herdado | Origem |
|---|---|---|
| `si_estabelecimentos` | 38 min, 76 GB lidos, 97 GB shuffle | B20 |
| `si_dados_simples` | 28 min | B20 |
| `si_socios` | 25 min, 69 GB lidos | B20 |
| `si_log_eventos_negocio` | 873 s (antes 241 s) | B20 |
| `gold_fato_empresa_mes` | 84 GB shuffle, 0,9 GB de entrada | B20 |
| `sf_dim_empresa` | spill 7,6 GB (única com spill) | B20 |
| Estáticas | 3–5 min cada | B20 |

## Pendente (a medir após reautenticar)

Consultas prontas em `/home/Junj/.claude/jobs/a6f336ea/tmp/base/consultas.sql` (duração por tarefa via
`system.lakeflow.job_task_run_timeline`; DBU e custo de lista via `system.billing.usage` × `list_prices`).

- Duração por tarefa e por run (440118712899237, 1097845690343814, 297380538835106, run de 2026-11-08): **não medido** (CLI sem auth).
- Leitura/shuffle/spill por tarefa: **não medido**; só os números herdados acima.
- DBU e custo por run: **não medido**.
- Volumes por tabela e linhas por `mes_referencia`: **não medido**.
- Tamanho da landing no S3: **não medido** (sem permissão de listar buckets e sem auth Databricks para ler external locations).
- Tempo da landing por mês: **não medido** (não verificado nos logs do repo).

## Seções a preencher

"Onde o tempo vai" e "Projeção ingênua para 12 e 40 meses" ficam para depois das medições; nada foi estimado
sem base.
