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
| `aws_attributes.instance_profile_arn` | Se o S3 é acessado por *instance profile*, preencher; se for **external location / storage credential** do Unity Catalog, **remover a linha**. |
| `spark_env_vars.AWS_BUCKET_NAME` | Nome do bucket da landing. |
| `node_type_id` / `autoscale` | Ponto de partida: workers `r5d.xlarge` (memória p/ SCD2 de estabelecimentos) spot, 1–4. Driver on-demand (`first_on_demand: 1`) para o spot não derrubar o run. |
| `spark_version` | 15.4 LTS (Spark 3.5). Em 17.x (Spark 4) o ANSI vem ligado — o código já usa `try_to_date`, mas é a versão menos testada. |

## Controle de custo
- `timeout_seconds: 14400` (4 h) corta runs travados; `max_concurrent_runs: 1`.
- Job cluster nasce e morre com o run (não há cluster ocioso). Spot com fallback.
- Para depurar uma etapa: *Repair run* só da task que falhou (o cluster sobe uma vez para o repair).
- Sugestão: criar um **budget/alerta** na conta AWS e em *Account console → Budgets* do Databricks.

## Tasks

| Task | Executa |
|---|---|
| `bronze_estaticas` ∥ `bronze_mensais` | `run_bronze.py` (ingestão + DQ) |
| `silver_int_scd2` | 4 SCD2 + DQ |
| `silver_int_eventos` | `log_eventos_negocio` + DQ |
| `silver_final_dims` | `dim_cnae`, `dim_grupo_cnae`, `ponte_cnae`, `dim_tempo`, `dim_mudanca_estado`, `dim_empresa` + DQ |
| `silver_final_fato` | `fato_mudanca_estado` + DQ |
| `gold` | fatos, OBT, `dashboard_mercado_ativo`, coorte, views `ds_*`, `dq_gold` |

Fora do Job: `ds_micro_*.sql` são consultas do dashboard (parâmetros `:param_*`); `migracao_socios.sql`
(legado) e `vw_dashboard_eventos_cnae.sql` (sem consumidor) não rodam.

## Pré-requisitos no workspace
- Unity Catalog com permissão para `CREATE CATALOG tcc_receita` (ou criar antes, com *managed location*,
  e os schemas `bronze`, `silver_intermediate`, `silver_final`, `gold`).
- Leitura de `s3://<bucket>/landing/receita_federal/` pelo cluster.
- `bronze.cnaes` (CNAE da Receita) **não** é ingerido: não existe na landing; usa-se `cnaes_ibge`.
- 2023-05 é o primeiro mês publicado: a janela começando nele é **carga inicial** (sem eventos no mês).
