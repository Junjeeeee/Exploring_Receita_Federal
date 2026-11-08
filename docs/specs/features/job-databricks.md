# Feature: Job Databricks granular (`tcc_receita_pipeline`)

Status: **implementado (gerador + YAML), não executado** · 2026-11-08 · decisões do autor no chat.
Spec parcialmente retroativa (D-12): runners e 1ª versão do Job foram feitos em modo protótipo.

## Requisito
- Rodar bronze → silver_intermediate → silver_final → gold num workspace **clássico**, com custo baixo
  (sem serverless, sem SQL warehouse) e criação **manual** do Job pela UI (sem CLI por enquanto).
- **Uma task por tarefa**: ingestão e DQ em tasks separadas, para repair granular, tempo por tabela e
  paralelismo real. Bronze mensal: **uma task por tabela**, com `mes_referencia_inicial/final` como parâmetro.
- Definição em **YAML** (formato da UI / Asset Bundles), **gerada** por script — não editada à mão.

## Design
- `src/jobs/run_bronze.py --modo ingestao|dq|ambos --tabelas <t>`: chama `bronze_lib`. Põe a raiz do repo
  no `sys.path` (scripts de `git_source` não a incluem).
- `src/jobs/run_sql.py --arquivos <f.sql>`: substitui `{{mes_referencia_*}}` (valida `YYYY-MM`), divide por
  `;` respeitando strings/comentários, roda `spark.sql`; consultas puras (DQ) recebem `.collect()`.
- `src/jobs/gerar_job.py`: lista declarativa de arquivos → YAML. **Dependências extraídas dos SQL**
  (mesma ideia do `ref()` do dbt, mas por regex sobre `tcc_receita.<schema>.<tabela>`):
  - cada SQL de ingest declara o que escreve (`CREATE`/`MERGE INTO`/`INSERT`) e o que lê;
  - uma task depende do **DQ** da tabela que lê (ou da produtora, se a tabela não tem DQ);
  - falha se um SQL lê tabela sem produtora ou se duas tasks escrevem a mesma tabela;
  - `--checar` compara com o YAML commitado (para CI).
- Nomes: `bronze_<t>`, `bronze_dq_<t>`, `si_<t>`, `si_dq_<t>`, `sf_<t>`, `sf_dq_<t>`, `gold_<t>`, `gold_dq_gold`.
- Cluster único (`cluster_pipeline`) compartilhado por todas as tasks: DBR 15.4 LTS, driver `m5d.xlarge`
  on-demand, workers `r5d.xlarge` spot com fallback, autoscale 1–4, `spark.scheduler.mode=FAIR`,
  timeout 4 h, `max_concurrent_runs: 1`. Acesso ao S3 via external location do UC (sem instance profile).
- Fora do Job: `ds_micro_*` (consultas do dashboard com `:param_*`), `migracao_socios.sql` (legado),
  `vw_dashboard_eventos_cnae.sql` (sem consumidor), bronze `cnaes` (sem fonte na landing).

## Tarefas
- [x] Runners `run_bronze.py` (com `--modo`) e `run_sql.py`
- [x] Gerador `gerar_job.py` com dependências inferidas dos SQL (53 tasks)
- [x] YAML gerado e README de criação manual
- [ ] `gerar_job --checar` no CI (junto com o CI útil do backlog)
- [ ] 1ª execução 2023-05..2023-08 e ajuste de cluster com base nos tempos por task

## Critérios de aceite
- Job criado pela UI colando o YAML, sem edição além de bucket/credenciais.
- Run completo verde na janela de teste; cada tabela com sua task de ingestão e de DQ visível no DAG.
- Repair de uma task isolada funciona sem rodar as anteriores.

## Riscos / pendências
- Overhead de ~segundos por task (processo Python novo) — irrelevante frente ao volume.
- Bronze `socios` só ingere a janela; em execuções incrementais o mês anterior já deve estar na bronze.
- B13: eventos por data perdem empresas publicadas com atraso em execuções mês a mês.

## Impacto no TCC
O DAG do Job (UI) pode virar figura do capítulo de Implementação; a inferência de dependências a partir do
SQL é um detalhe de engenharia citável.
