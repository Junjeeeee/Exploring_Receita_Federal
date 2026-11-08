# CLAUDE.md — tcc_receita

Plataforma de dados (TCC) sobre os **dados abertos de CNPJ da Receita Federal** + domínios IBGE.
O objetivo é transformar **snapshots mensais** em **histórico de entidades (SCD2)** e **eventos de negócio**,
servindo dashboards, estudos de sobrevivência e ML.

Idioma: **português** (código, SQL, docs e conversa).

## Arquitetura em 1 minuto

```
Receita (WebDAV ZIP) / IBGE API / BigQuery (CEPs)
  └─► LANDING   Python local → Parquet no S3          src/landing/
       └─► BRONZE   PySpark/Delta (Databricks UC)    src/bronze/
            └─► SILVER_INTERMEDIATE  SCD2 + log eventos (Spark SQL)   src/silver_intermediate/
                 └─► SILVER_FINAL   modelo dimensional (Spark SQL)    src/silver_final/
                      └─► GOLD      fatos/OBT/datasets dashboard      src/gold/
```
Catálogo Unity: `tcc_receita.{bronze,silver_intermediate,silver_final,gold}`.
Detalhes e DAG: [docs/architecture.md](docs/architecture.md).

## Onde está cada coisa

| Preciso de... | Leia |
|---|---|
| Visão geral, DAG, ordem de execução | `docs/architecture.md` |
| Termos de negócio (empresa, ativa, eventos, porte, sentinelas) | `docs/glossario.md` |
| Spec de uma camada | `docs/specs/<camada>.md` |
| Por que algo é assim | `docs/decisions.md` |
| Bugs conhecidos / pendências | `docs/backlog.md` |
| Começar uma feature nova | copie `docs/specs/_template.md` → `docs/specs/features/<nome>.md` |
| Texto do TCC (LaTeX em `/home/Junj/Documents/TCC`), status e divergências texto×código | `docs/tcc.md` |
| Prazos, escopo e prioridades do TCC | `docs/tcc.md` § Escopo e prazos |
| Repo anterior (grafo + ML, Niterói) e o que reaproveitar | `docs/mineracao_receita.md` |

## Fluxo de trabalho (Spec Driven Development)

1. **Spec antes do código**: toda mudança não trivial começa com um arquivo em `docs/specs/features/`
   (requisito → design → tarefas → critérios de aceite). Aprovar com o usuário antes de implementar.
2. Implementar seguindo as tarefas da spec; marcar progresso no próprio arquivo.
3. Ao terminar: atualizar a spec da camada afetada (`docs/specs/<camada>.md`), `docs/backlog.md`
   e, se houve decisão de design, `docs/decisions.md`.
4. Distinguir sempre **implementado / preparado (não executado) / planejado**.

## Regras que não podem ser quebradas

- **LGPD**: nunca CPF em claro; não desmascarar documentos; `informacao_adicional` sem documentos pessoais.
- **Regex de mascaramento MEI** (`limpar_chunk_empresas_mei` em `src/landing/landing_lib.py`) foi validada
  em dados reais — não alterar sem evidência concreta de falha.
- **Não redesenhar DQ existente**; corrigir apenas problemas demonstrados. Preferir correções mínimas e pontuais.
- **Bronze não tem regra de negócio.** Regras vivem em silver/gold.
- **Chave técnica ≠ chave de negócio**: nunca extrair CNPJ de `pk` via regex; juntar por `cnpj_basico`.
- **NULL ≠ FALSE** em dados tributários (ausência de info não é "não optante").
- **Reprocessamento** de janela preserva versões passadas e futuras — nada de delete em massa.
- Socios precisa do **snapshot completo do mês anterior** à janela (senão gera saídas falsas do QSA).
  Exceção: **2023-05 é o primeiro mês publicado** — é carga inicial (baseline), sem eventos; ver backlog B9.
- **Nunca editar o repositório do TCC** (`/home/Junj/Documents/TCC`) nem redigir o texto da monografia
  (regra de autoria da UFRJ). Sugestões de mudança vão para `docs/tcc.md` (documento vivo).
- Código gerado com IA precisa ser legível e explicável: o autor revisa tudo antes da entrega final.

## Git

- Commits atômicos, mensagens em português no formato `<camada|docs|chore>: descrição`.
- Nada é considerado "pronto" antes da primeira execução no Databricks; refatorar/apagar código é aceitável
  quando justificado em spec.

## Comandos

```bash
poetry install                                   # deps (Python >=3.11)
# Landing (rodar da raiz do repo, sempre com -m)
poetry run python -m src.landing.orquestrador --tabela empresas,estabelecimentos \
    --mes_referencia_inicial 2023-05 --mes_referencia_final 2023-08 [--force]
poetry run python -m src.landing.extract.<tabela> --mes_referencia_inicial YYYY-MM
poetry run python -m src.landing.dq.<tabela> ...
poetry run pytest tests/                         # ATENÇÃO: suíte quebrada, ver backlog
```
- Bronze: `src/bronze/ingest/<tabela>.py` / `src/bronze/tests/<tabela>.py` rodam **no Databricks** (Spark).
- SQL (silver/gold): **não há runner no repo**; são executados no Databricks (Jobs/SQL tasks) com
  parâmetros `{{mes_referencia_inicial}}` / `{{mes_referencia_final}}` (vazio = mês anterior, exceto onde
  indicado). Datasets `ds_micro_*` usam parâmetros de dashboard `:param_*`.
- Env vars (ver `.env.example`): `AWS_BUCKET_NAME`, `AWS_*`, `TOKEN_SHARE`, `GCP_PROJECT_ID`. Nunca imprimir `.env`.

## Convenções de código

- Landing: uma fonte = um `src/landing/configs/<t>.py` com `CONFIG_LANDING = TabelaLandingConfig(...)`,
  registrado em `TODAS_CONFIGS` (`orquestrador.py`). `extract/` e `dq/` são stubs de 4 linhas.
- Bronze: um `src/bronze/configs/<t>.py` com `TabelaBronzeConfig`; metadados `pk`, `change_hash` (SHA-256),
  `mes_referencia`, `bronze_processed_at`.
- SQL: um arquivo por tabela em `<camada>/ingest/`; DQ em `<camada>/tests/dq_<tabela>.sql` usando
  `SELECT ASSERT_TRUE(cond, 'ERRO DQ [tabela]: msg')` (passa = sem erro).
- SCD2: intervalo meio-aberto `[valid_since, valid_until)` em `'YYYY-MM'`; `valid_until` NULL = atual.
- Surrogate keys: `XXHASH64(nk) & Long.MAX`; `-1` = não aplicável/ghost, `-2` = CNAE não encontrado.

## Contexto histórico

`.claude/context_from_other_llm.md` é contexto de outro agente — útil para *rationale*, mas pode estar
desatualizado. **O código é a fonte da verdade.** O que foi validado de lá já está em `docs/`.
