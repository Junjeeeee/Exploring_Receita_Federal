# Job Databricks — `tcc_receita_pipeline`

Pipeline completo (bronze → silver_intermediate → silver_final → gold) num **único job cluster**
(sem SQL warehouse, sem serverless). Status: **preparado, não executado**.

## Como criar (manual, pela UI)

1. *Workflows → Create job → ⋮ → Edit as YAML* e colar `tcc_receita_pipeline.job.yml`.
2. Ajustar os campos marcados abaixo.
3. Rodar com os parâmetros padrão (`2023-05` a `2023-08`).

| Campo | O que ajustar |
|---|---|
| `git_source` | Repo privado → configurar credencial Git em *User settings → Linked accounts*. Branch atual: `ceps_ibge_and_dashboard_v2`. |
| `aws_attributes.instance_profile_arn` | Ausente por padrão (acesso ao S3 via **external location** do Unity Catalog). Se usar *instance profile*, adicionar `instance_profile_arn: arn:aws:iam::<conta>:instance-profile/<nome>` em `aws_attributes`. |
| `spark_env_vars.AWS_BUCKET_NAME` | Nome do bucket da landing. |
| `node_type_id` / `autoscale` | Ponto de partida: workers `r5d.xlarge` (memória p/ SCD2 de estabelecimentos) spot, 1–4. Driver on-demand (`first_on_demand: 1`) para o spot não derrubar o run. |
| `spark_version` | 15.4 LTS (Spark 3.5). Em 17.x (Spark 4) o ANSI vem ligado — o código já usa `try_to_date`, mas é a versão menos testada. |

## Controle de custo
- `timeout_seconds: 14400` (4 h) corta runs travados; `max_concurrent_runs: 1`.
- Job cluster nasce e morre com o run (não há cluster ocioso). Spot com fallback.
- Para depurar uma etapa: *Repair run* só da task que falhou (o cluster sobe uma vez para o repair).
- Sugestão: criar um **budget/alerta** na conta AWS e em *Account console → Budgets* do Databricks.

## Tasks (53, geradas)

O YAML é **gerado** por `python -m src.jobs.gerar_job` (não editar à mão). Uma task de ingestão e uma de DQ
por tabela; as dependências saem dos próprios SQL (tabelas `tcc_receita.*` lidas/escritas). Design e
decisões: `docs/specs/features/job-databricks.md`.

| Prefixo | Executa |
|---|---|
| `bronze_<t>` → `bronze_dq_<t>` | `run_bronze.py --modo ingestao|dq` (11 tabelas, em paralelo) |
| `si_<t>` → `si_dq_<t>` | SCD2 e `log_eventos_negocio` |
| `sf_<t>` → `sf_dq_<t>` | dimensões, ponte e `fato_mudanca_estado` |
| `gold_<t>`, `gold_dq_gold` | fatos, OBT, mercado ativo, coorte, views `ds_*` |

Ao mudar um SQL (nova tabela lida, novo arquivo), rode o gerador e cole o YAML de novo na UI.

Fora do Job: `ds_micro_*.sql` são consultas do dashboard (parâmetros `:param_*`); `migracao_socios.sql`
(legado) e `vw_dashboard_eventos_cnae.sql` (sem consumidor) não rodam.

## Pré-requisitos no workspace
- Catálogo `tcc_receita` criado **uma vez, antes do Job**, com *managed location* no S3 (os schemas o Job cria):
  *Catalog → + → Create catalog* → tipo *Standard* → desmarcar *Use default storage* → escolher a external
  location e um caminho, ex. `s3://<bucket>/unity/tcc_receita/`. Equivalente em SQL:
  `CREATE CATALOG tcc_receita MANAGED LOCATION 's3://<bucket>/unity/tcc_receita/'`.
  **Não usar Default Storage**: ele só é acessível por compute serverless, e o Job roda em cluster clássico.
- Leitura de `s3://<bucket>/landing/receita_federal/` pelo cluster.
- Volume de logs `tcc_receita.ops.cluster_logs` (usado por `cluster_log_conf`): *Catalog → tcc_receita → Create schema*
  `ops` → *Create volume* `cluster_logs` (managed). Sem ele o cluster não sobe. Logs ficam em
  `/Volumes/tcc_receita/ops/cluster_logs/<cluster_id>/` (`driver/`, `eventlog/` = métricas do Spark UI).
  Leitura: `databricks fs ls dbfs:/Volumes/tcc_receita/ops/cluster_logs/`.
- `bronze.cnaes` (CNAE da Receita) **não** é ingerido: não existe na landing; usa-se `cnaes_ibge`.
- 2023-05 é o primeiro mês publicado: a janela começando nele é **carga inicial** (sem eventos no mês).
