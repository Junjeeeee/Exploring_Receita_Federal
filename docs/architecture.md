# Arquitetura

> Estado em 2026-11-08, branch `ceps_ibge_and_dashboard_v2`. Fonte da verdade: o código.

## Stack

| Camada | Execução | Tecnologia | Armazenamento |
|---|---|---|---|
| Landing | Local (Python ≥3.11, Poetry) | pandas, pyarrow, requests, tenacity, boto3, basedosdados | S3 Parquet (snappy) |
| Bronze | Databricks (job Python) | PySpark | Delta, `tcc_receita.bronze` |
| Silver intermediate | Databricks (SQL) | Spark SQL (`MERGE`, `REPLACE WHERE`) | Delta, `tcc_receita.silver_intermediate` |
| Silver final | Databricks (SQL) | Spark SQL | Delta, `tcc_receita.silver_final` |
| Gold | Databricks (SQL + dashboards) | Spark SQL | Delta/views, `tcc_receita.gold` |

- `duckdb` (grupo `notebooks`) é usado apenas para exploração local dos Parquet da landing.
- **Não existe orquestração end-to-end no repo**: a landing tem orquestrador Python; bronze tem CLIs por
  tabela; os SQLs são rodados no Databricks por definição externa ao repo. CI/CD de deploy é stub (`echo`).

## Fontes

| Origem | Mecanismo | Tabelas |
|---|---|---|
| Receita Federal | WebDAV `arquivos.receitafederal.gov.br/public.php/dav/files/{TOKEN_SHARE}/{YYYY-MM}/*.zip` | empresas, estabelecimentos, socios, dados_simples (mensais); municipios, paises, naturezas_juridicas, qualificacoes_socios, motivos_situacao (estáticas) |
| IBGE | API `servicodados.ibge.gov.br/api/v2/cnae/subclasses` | cnaes_ibge |
| Base dos Dados (BigQuery) | `br_bd_diretorios_brasil.cep` | ceps |
| BCB (Selic/IPCA) | **seed hardcoded** em `gold/fato_indicadores_macro_mes.sql` (até 2025-12) | fato_indicadores_macro_mes |

## DAG

```
LANDING (S3)                    s3://$AWS_BUCKET_NAME/landing/receita_federal/<pasta>/[mes_referencia=YYYY-MM/]dados.parquet
  │
BRONZE (snapshot por mês, replaceWhere mes_referencia)
  empresas, estabelecimentos, socios, dados_simples        (mensais, particionadas)
  cnaes, cnaes_ibge, paises, municipios, naturezas_juridicas, qualificacoes_socios, motivos_situacao, ceps (estáticas)
  │
SILVER_INTERMEDIATE
  bronze.empresas          → empresas            (SCD2)
  bronze.estabelecimentos  → estabelecimentos    (SCD2)
  bronze.dados_simples     → dados_simples       (SCD2)
  bronze.socios (+mês ant.)→ socios              (SCD2 + vinculo_ativo)
  4 SCD2 + bronze.motivos_situacao → log_eventos_negocio   (roda por último)
  │
SILVER_FINAL
  bronze.cnaes_ibge                         → dim_cnae
  si.estabelecimentos (matriz)              → dim_grupo_cnae
  dim_grupo_cnae + dim_cnae                 → ponte_cnae
  si.log_eventos_negocio                    → dim_tempo, dim_mudanca_estado
  si.{estab,empresas,simples} + bronze domínios + ceps + dim_grupo_cnae → dim_empresa (SCD2)
  log + mapa pk→cnpj + dim_mudanca_estado + dim_tempo + dim_empresa     → fato_mudanca_estado
  │
GOLD
  silver_final.*                            → obt_dashboard_eventos
  silver_final.* (ponte completa)           → vw_dashboard_eventos_cnae   (view; possivelmente legado)
  dim_tempo, dim_empresa, ponte, dim_cnae, obt_dashboard_eventos → fato_empresa_mes
  seed BCB                                  → fato_indicadores_macro_mes
  fato_empresa_mes + macro                  → fato_indicadores_setoriais_mes
  dim_empresa, ponte, dim_cnae              → dashboard_mercado_ativo, ds_coorte_sobrevivencia
  obt + macro                               → ds_visao_macro (view)
  obt                                       → ds_micro_comparativo_geografico / _setorial (queries com :param_*)
  dashboard_mercado_ativo                   → ds_micro_top5_empresas (query com :param_*)
```

## Ordem de execução (por janela de meses)

1. Landing: `orquestrador` (mês a mês: todas as mensais → DQ; depois estáticas uma vez).
2. Bronze: `ingest/<t>.py` e `tests/<t>.py` (inclui **mês anterior** à janela para socios).
3. Silver intermediate: `empresas`, `estabelecimentos`, `dados_simples`, `socios` (paralelizáveis) → `log_eventos_negocio` → `tests/dq_*`.
4. Silver final: `dim_cnae` → `dim_grupo_cnae` → `ponte_cnae`; `dim_tempo`, `dim_mudanca_estado`; `dim_empresa` → `fato_mudanca_estado` → `tests/dq_*`.
5. Gold: `fato_indicadores_macro_mes` → `obt_dashboard_eventos` → `fato_empresa_mes` → `fato_indicadores_setoriais_mes`; `dashboard_mercado_ativo`; views `ds_*`; `dq/dq_gold.sql`.

## Estado de validação

- Landing: executada; backfill 2023-05..2023-08 em andamento (estabelecimentos 2023-08 falhou com `BadZipFile` → correção no WIP).
- Silver intermediate / final: revisados **estaticamente**, segundo o readme e o contexto histórico; **sem confirmação de execução em cluster**.
- Gold: existe no repo, mas há SQL quebrado (ver `backlog.md`).

## Layout do repositório

```
src/landing/{landing_lib.py, orquestrador.py, configs/, extract/, dq/}
src/bronze/{bronze_lib.py, data_quality.py, configs/, ingest/, tests/}
src/silver_intermediate/{ingest/, tests/, readme.md}
src/silver_final/{ingest/, tests/}
src/gold/{*.sql, dq/dq_gold.sql}
tests/            pytest (quebrado — ver backlog)
notebooks/        exploração (DuckDB) e scratch Databricks (desatualizados)
config/*.yaml     vazios
.github/workflows/ci-cd.yml   pytest + deploy stub
```
