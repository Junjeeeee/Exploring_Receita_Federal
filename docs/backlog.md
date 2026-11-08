# Backlog — problemas conhecidos e pendências

Legenda: ✅ confirmado no código · 🔎 provável (análise estática, não executado)

## Bugs

| # | Camada | Problema | Evidência |
|---|---|---|---|
| ~~B1~~ ✔ corrigido 2026-11-08 | bronze | Todos os `src/bronze/tests/*.py` importam `executar_ingestao_cli` e chamam `executar_ingestao_dq_cli` (inexistente) → NameError. Correto: `executar_dq_cli`. | `bronze_lib.py:311` |
| ~~B2~~ ✔ corrigido 2026-11-08 | bronze | `testar_tabela_bronze` filtra `mes_referencia` também em tabelas estáticas (coluna não existe). | `bronze_lib.py:270` |
| ~~B3~~ ✔ corrigido 2026-11-08 | gold | `ds_micro_comparativo_setorial.sql` usa colunas inexistentes (`class_name`, `cnae_name`, `great_area_of_activity_name`) → reais: `nome_classe`, `descricao_cnae`, `grande_setor_economico`. | linhas 5, 10 |
| ~~B4~~ ✔ corrigido 2026-11-08 | silver_final | `dq_dim_cnae` conta `codigo_cnae IS NULL` como chave nula, mas sentinelas -1/-2 têm `codigo_cnae` NULL → DQ falharia. | `tests/dq_dim_cnae.sql:5` |
| B5 🔎 | silver_final | `fato_mudanca_estado`: join anterior usa `fim_vigencia >= mes` e posterior `<` (inconsistente); pode duplicar eventos; fallback para primeira versão pode atribuir estado errado. | `ingest/fato_mudanca_estado.sql` |
| B6 ✅ | tests | `tests/test_extractor.py` importa `cnpj_pipeline.extract.extractor` (removido) → pytest/CI falham na coleta. `test_cdc_logic.py`/`test_dimensions.py` vazios. | `src/cnpj_pipeline/` só `__pycache__` |
| B7 🔎 | silver_int | `log_eventos_negocio.sql` exige parâmetros literais (sem fallback); `dq_log_eventos_negocio` com default diferente das SCD2. | readme |
| ~~B9~~ ✔ corrigido 2026-11-08 | silver_int | **Primeiro mês disponível é 2023-05** (não existe 2023-04 na RFB). `socios.sql` exige o mês anterior a `mes_ini` (ASSERT `snapshots_validos`) → 1ª execução com 2023-05 falha. Falta um modo **carga inicial (baseline)**: 2023-05 abre as versões SCD2 sem gerar eventos de entrada/saída/criação (senão o log teria todo o estoque como "evento" em 2023-05). Eventos reais começam em 2023-06. Verificar o mesmo nas outras SCD2 e em `log_eventos_negocio`. | `socios.sql:63-68` |
| ~~B10~~ ✔ corrigido 2026-11-08 | bronze | `dim_empresa` lê `bronze.ceps`, mas não havia config/ingest bronze de ceps. | `dim_empresa.sql:96` |
| ~~B11~~ ✔ corrigido 2026-11-08 | gold | `INSERT OVERWRITE ... REPLACE WHERE` (inválido) e predicado com `CURRENT_DATE()` em `fato_empresa_mes`/`obt_dashboard_eventos`. | — |
| ~~B12~~ ✔ corrigido 2026-11-08 | silver | `to_date(x,'yyyyMMdd')` quebra com ANSI ligado ('00000000') → `try_to_date`. | — | Revisado: `try_to_date` não existe no DBR 15.4 (Spark 3.5) → `CAST(try_to_timestamp(x, fmt) AS DATE)`.
| B13 🔎 | silver_int | Eventos por data (`EMPRESA NASCEU`, `SOCIO ENTROU NO QSA`) só entram se a data cair **dentro da janela** processada. Com publicação atrasada (empresa de 2023-08 aparece só no snapshot 2023-09), rodar mês a mês perde o evento. Run único da janela completa não sofre. | `log_eventos_negocio.sql` |
| ~~B14~~ ✔ corrigido 2026-11-08 | bronze | `CREATE CATALOG IF NOT EXISTS` sem *managed location* falha no metastore sem storage root. Catálogo virou pré-requisito manual (README do Job). | `bronze_lib.py` |
| ~~B15~~ ✔ corrigido 2026-11-08 | landing | Estáticas `cnaes_ibge`, `paises`, `municipios`, `naturezas_juridicas`, `qualificacoes_socios` só existiam no layout antigo (`<pasta>/mes_referencia=*/`); a bronze lê `<pasta>/dados.parquet`. Reextraídas com a landing atual (snapshot 2023-08). Sobram pastas antigas órfãs (`cnaes/`, `motivos/`, `naturezas/`, `qualificacoes/` e as partições mensais das estáticas): apagar depois de confirmar que nada as lê. | S3 |
| B16 🔎 contornado 2026-11-08 | bronze/silver_int | A Receita publica PKs repetidas com conteúdo divergente (empresas: mesmo `cnpj_basico`; socios: mesma pessoa no mesmo CNPJ, provavelmente com duas qualificações). A bronze falhava. Agora a **bronze mantém todas** (só remove cópias idênticas e loga a contagem) e a **silver desempata** no `ROW_NUMBER` por `pk, mes_referencia` com `change_hash DESC` (determinístico). **Pendente:** medir volume (`SELECT pk, COUNT(*) ... HAVING COUNT(*) > 1` na bronze), entender a causa e decidir a regra (ex.: socios com várias qualificações). | `bronze_lib.py::_remover_copias_exatas`, `silver_intermediate/ingest/*.sql` |
| B17 🔎 | bronze (perf) | Ingestão bronze lenta (empresas 27 min, estabelecimentos 55 min, dados_simples 46 min p/ 4 meses). Event log (2026-11-08, empresas): por mês ~1,6 GB de Parquet viram **3 shuffles completos de ~7 GB**: `dropDuplicates()` em todas as colunas na contagem de PKs repetidas, de novo na escrita (sem cache) e o shuffle do `optimizeWrite`. Sem spill, GC baixo. Proposta: (1) tirar o `dropDuplicates()` da bronze — cópias idênticas a silver já elimina no `ROW_NUMBER` por pk/mês; (2) mover a contagem de PKs repetidas para a DQ (`groupBy(pk)` só da coluna pk); (3) `optimizeWrite=false` na escrita da bronze (1 mês por escrita já dá arquivos de bom tamanho). Estimativa: 3–4× mais rápido. Importante antes da janela completa (~40 meses). | `bronze_lib.py` |
| B8 🔎 (parcial: média só de meses anteriores) | bronze | Volumetria: erros de leitura do histórico engolidos; média inclui meses futuros; `forcar_aprovacao` não ligado. | `data_quality.py:375` |

## Dívidas / limpeza

- `fato_indicadores_macro_mes` é seed hardcoded até 2025-12 → ingerir BCB (`bronze.indicadores_bcb`).
- `d_coorte_sobrevivencia.sql` cria `ds_coorte_sobrevivencia` (nome divergente); usa `FIRST()` não determinístico e `CURRENT_DATE`.
- Typo: `src/silver_final/tests/dq_dim_mucanca_estado.sql`.
- Sem DQ para `dim_tempo`, `fato_indicadores_*`, `ds_*`.
- CTE `ponte_primaria` e normalização de CNAE duplicadas em vários SQL.
- `fato_empresa_mes` acoplado às strings de `categoria_macro_evento` da OBT.
- `vw_dashboard_eventos_cnae` sem consumidor — avaliar remoção.
- Dashboard v0 (`src/gold/dashboard/`, spec `features/dashboard-v0.md`): importar e validar formato; `ds_micro_*` ficaram sem uso (avaliar remoção junto com `vw_dashboard_eventos_cnae`).
- Bronze descarta linhas com PK nula sem contar/registrar.
- `CONTRATOS_SCHEMA` (5 tabelas) duplica `colunas_esperadas` das configs.
- Landing: tabelas estáticas nunca atualizam sem `--force`; DQ só schema+contagem; `min_linhas` hardcoded; `print` em vez de logging.
- Landing: código morto (`descarregar_e_extrair`, `_require_bucket`, `TOKEN_SHARE`/`URL_DOWNLOAD_BASE` no import).
- `python-dateutil` usado mas não declarado em `pyproject.toml`.
- `pyproject` aponta para pacote `cnpj_pipeline` (vazio).
- `tmp_receita/` não está no `.gitignore` (diagnósticos de ZIP podem ser commitados).
- Vazios: `README.md`, `Makefile`, `config/dev.yaml`, `config/prod.yaml`.
- Legado na raiz: `checkpoint_estado.json`, `last_processed_month.txt`.
- Notebooks `backfill_motivos.ipynb` e `New Notebook ...` importam módulos inexistentes.
- CI: sem `ruff`; deploy Databricks é `echo`. Não há runner/Job definition (DABs) para os SQL.

## Ideias e pendências novas (2026-11-08)

- **CI útil** (o atual quebra): ruff + pytest só de testes locais sem rede (landing) + validação de sintaxe dos
  SQL (ex.: `sqlglot` dialeto Spark, trocando `{{param}}` por literais). Remover o deploy `echo`.
- **Etapa exploratória local**: baixar recorte pequeno (ex.: Niterói, 2023-05..08) e explorar com DuckDB para
  achar padrões/problemas e revisar glossário/eventos antes da revisão profunda. Custo zero de cluster.
- **Dicionário de dados** (`docs/dicionario_dados.md`): schema + transformação por coluna, pedido da orientadora.
- **🔎 Investigar PKs repetidas da Receita (B16) — prioridade após o 1º run.** Hoje a silver desempata de forma
  arbitrária (`change_hash DESC`), o que pode esconder informação. Investigar, por tabela e por mês:
  1. volume (`GROUP BY pk HAVING COUNT(*) > 1` na bronze) e se as mesmas PKs se repetem em todos os meses;
  2. quais colunas divergem entre as versões (empresas: capital/porte? socios: qualificação/data de entrada?);
  3. se é erro de publicação da Receita ou dado legítimo (ex.: sócio com dois papéis no mesmo CNPJ);
  4. decidir a regra na silver (ex.: incluir qualificação na chave de socios, agregar papéis, ou manter o desempate)
     e registrar em `decisions.md`; avaliar uma DQ informativa com a contagem por mês.
- **Identidade de sócio PF**: medir colisões/mudanças (faixa etária, nome). Pré-requisito de grafo/grupos.
- **Pseudonimização de nome palavra a palavra** (HMAC com chave secreta) — avaliar quando houver grafo.
- **Grande setor** (D-10): achar fonte oficial ou trocar por seção CNAE.
- **Backlog como pasta** (`docs/backlog/README.md` + um arquivo por item) quando crescer.

## Roadmap (do contexto histórico, ainda válido)

1. ~~Concluir backfill landing 2023-05..2023-08~~ — feito pelo usuário (todas as tabelas no S3, 2026-11). Não existe 2023-04 (1º mês da RFB = 2023-05) → ver B9.
1b. **Revisão/refatoração/caça a bugs** de bronze, silver e gold antes de rodar (via specs em `docs/specs/features/`).
1c. **Job Databricks** em workspace clássico com job cluster barato (spot, autoterminação) — serverless saiu caro.
    Por ora o usuário cria o Job **manualmente**; entregar código + definição (JSON/YAML do Job) para copiar. CLI só depois de definir limites de custo.
    Janela completa: 2023-05 até o mês mais recente disponível. Dashboard: primeiro no Databricks (AI/BI); publicação externa a avaliar.
2. Reingerir bronze → 4 SCD2 → `log_eventos_negocio` → silver final (dims → ponte → fato) → validações.
3. Primeira execução real em cluster das camadas silver (nunca executadas).
4. Gold / dashboard v2: corrigir B3, revisar datasets.
5. Futuro: ingestão BCB, grafo de QSA (conceito), modelo de falência (experimento fora do repo: CatBoost, Niterói, PR-AUC ~0,42).
