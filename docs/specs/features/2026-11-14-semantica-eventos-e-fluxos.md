# Feature: semântica de eventos e fluxos (dashboard de 2026-11-16)

**Status:** implementado e validado na amostra de 1%; **falta executar na base real (T8)** · 2026-11-14
**Camadas afetadas:** silver_intermediate (log), silver_final, gold, dashboard
**Branch:** `ceps_ibge_and_dashboard_v2`
**Origem:** `docs/critica.md` (C-01..C-08, C-12, C-13, C-16, C-17) e backlog B21–B24.
**Aprovação:** o autor aprovou em 2026-11-14 a separação *mês de detecção × data do evento* e delegou as
demais definições (marcadas **[padrão]**: valem até ele rever).

## 1. Requisito

O dashboard precisa mostrar números coerentes: o estoque de ativas tem de fechar com os fluxos, "falência"
não pode ser toda baixa, e nenhum gráfico pode misturar dois relógios.

Medição em 2026-11-14 (SQL warehouse, 4 meses):

| Fato medido | Valor |
|---|---|
| Matrizes novas no snapshot (jun/jul/ago) | 303 mil / 314 mil / 397 mil |
| … com `teve_abertura_mes` hoje | 75 mil / 67 mil / 137 mil |
| Empresas com início de atividade em M que só aparecem no snapshot M+1 | ~70% (extração da Receita ocorre por volta do dia 10) |
| `MUDOU ESTADO CADASTRAL` para `BAIXADA - 01` (baixa voluntária), classificado como "liquidação/falência" | 527 mil |
| Baixas com motivo 05 (encerramento da falência) | ~430 |
| Reativações (INAPTA/SUSPENSA/BAIXADA → ATIVA), sem categoria | ~111 mil |
| `ENTROU NO MEI` no log × CNPJs que aparecem já como MEI | 443 × ~745 mil |
| CNPJs com vigência truncada na `dim_empresa` | 92.613 |
| Baixas no fato fora da população em risco | 13–16% por mês |
| Eventos de estado cadastral de filial | 22 mil de 662 mil (3%) |
| Linhas da dim com UF inválida (`'RJ,SP'` etc., 3 CEPs ruins) | 5.887 |

Fora de escopo: reprocessar bronze e SCD2 (nada muda nelas, exceto `socios.sql`, que não precisa ser
reexecutado agora); performance (B17/B20); sobrevivência além de corrigir a view.

## 2. Design

### 2.1 Dois relógios (D-13)

- **`mes_referencia` de todo evento = mês de detecção**: o primeiro snapshot em que o fato aparece. É o
  relógio dos fluxos do dashboard, porque é o mesmo relógio do estoque.
- **`data_evento` (DATE, pode ser NULL) = data real declarada pela Receita**, quando a fonte tem. Serve a
  tempo-até-evento (sobrevivência) e nunca é usada para somar fluxo mensal.
- 2023-05 (primeiro snapshot) é baseline: não gera evento nenhum.
- Limitação a registrar no texto: o snapshot `YYYY-MM` reflete a extração (~dia 10); o "mês de detecção M"
  cobre aproximadamente de meados de M−1 a meados de M.

### 2.2 Unidade de análise (D-14) **[padrão]**

Empresa = CNPJ raiz, representada pela **matriz canônica** (regra já existente em `dim_empresa`). Estoque e
fluxos de empresa vêm **só das transições da matriz**. Eventos de filial continuam no log, marcados com
`eh_matriz = FALSE`, e não entram em nenhum indicador de empresa.

### 2.3 Categorias por código de motivo (D-15) **[padrão]**

`categoria_evento` é atributo de `dim_mudanca_estado` (regra de negócio na silver, um lugar só). Para
`MUDOU ESTADO CADASTRAL`, por `(valor_anterior, valor_posterior, codigo_motivo)`:

| Condição (primeira que casar) | `categoria_evento` |
|---|---|
| situação igual, só o motivo mudou | `MUDANCA DE MOTIVO` |
| posterior = `ATIVA` | `REATIVACAO` |
| posterior = `BAIXADA`, motivo ∈ {05, 06, 10} | `FALENCIA OU LIQUIDACAO JUDICIAL` |
| posterior = `BAIXADA`, motivo = 75 | `EXTINCAO POR OBITO (MEI)` |
| posterior = `BAIXADA`, motivo ∈ {02, 03, 04} | `REORGANIZACAO SOCIETARIA` |
| posterior = `BAIXADA`, motivo ∈ {01, 37, 54} | `BAIXA VOLUNTARIA` |
| posterior = `BAIXADA`, motivo ∈ {13, 14, 15, 63, 64, 66, 71, 73} | `BAIXA DE OFICIO` |
| posterior = `BAIXADA` (demais) | `BAIXA - OUTROS MOTIVOS` |
| posterior = `INAPTA` | `INAPTIDAO` |
| posterior = `SUSPENSA` | `SUSPENSAO` |
| posterior = `NULA` | `ANULACAO` |
| qualquer outro caso | `OUTRA MUDANCA CADASTRAL` |

Demais eventos: `EMPRESA NASCEU` → `ABERTURA`; os outros → o próprio nome do evento.
Códigos: tabela de motivos da Receita (`bronze.motivos_situacao`). 05 = encerramento da falência,
06 = encerramento da liquidação, 10 = extinção pelo encerramento da liquidação judicial.

### 2.4 Contrato de colunas (o que cada arquivo entrega)

**`silver_intermediate.log_eventos_negocio`** — colunas novas no fim: `data_evento DATE`,
`codigo_motivo STRING`, `eh_matriz BOOLEAN`.

| Evento | `mes_referencia` | `valor_antigo` → `valor_novo` | `data_evento` | Outras |
|---|---|---|---|---|
| EMPRESA NASCEU | primeiro `valid_since` da matriz canônica, se > baseline | NULL → `ATIVIDADE INICIADA` | `data_inicio_atividade` | uma linha por empresa; `informacao_adicional` NULL; `eh_matriz` TRUE |
| MUDOU ESTADO CADASTRAL | `novo.valid_since` | situação → situação (**sem** o código do motivo) | `data_situacao_cadastral` nova | `codigo_motivo` = motivo novo; `informacao_adicional` = descrição do motivo; `eh_matriz` |
| idem, primeira aparição pós-baseline já não ATIVA | primeiro `valid_since` | NULL → situação | idem | idem (C-16) |
| MUDOU CNAE PRINCIPAL / LISTA SECUNDARIO | `novo.valid_since` | como hoje | NULL | `eh_matriz` |
| ENTROU/SAIU DO MEI, DO SIMPLES | `novo.valid_since` | como hoje | data de opção / exclusão correspondente | — |
| ENTROU NO MEI / SIMPLES, primeira aparição pós-baseline já `TRUE` | primeiro `valid_since` | NULL → `true` | data de opção | (C-16) |
| MUDOU NATUREZA JURIDICA | `novo.valid_since` | como hoje | NULL | — |
| SOCIO ENTROU NO QSA | primeiro `valid_since` da `pk` com vínculo ativo, se > baseline | NULL → `ATIVO` | `data_entrada_sociedade` | `informacao_adicional` NULL |
| SOCIO SAIU / RETORNOU / MUDOU QUALIFICACAO | `novo.valid_since` | como hoje | RETORNOU: `data_entrada_sociedade` | `informacao_adicional`: só qualificação em SAIU; NULL nos outros (sem CNPJ) |

Baseline de cada SCD2 = menor `valid_since` da própria tabela.

**`silver_final.dim_mudanca_estado`** — colunas novas: `codigo_motivo STRING`, `categoria_evento STRING`.
`codigo_motivo` entra na chave natural.

**`silver_final.fato_mudanca_estado`** — colunas novas (degeneradas): `data_evento DATE`, `eh_matriz BOOLEAN`.
O join com a dimensão passa a incluir `codigo_motivo` (null-safe).

**`silver_final.dim_empresa`** — sem truncar vigência (C-02). Colunas novas:
`codigo_motivo_situacao_cadastral STRING`, `data_situacao_cadastral DATE`, `codigo_natureza_juridica STRING`.
`uf` vinda do CEP só é aceita se casar `^[A-Z]{2}$` (B24).

**`gold.obt_dashboard_eventos`** — `categoria_macro_evento` = `dim_mudanca_estado.categoria_evento` (sem
regex). Colunas novas: `data_evento DATE`, `mes_evento STRING`, `eh_matriz BOOLEAN`, `codigo_motivo STRING`,
`entidade_tipo STRING`. `porte_empresa` = porte **anterior** quando o evento tira a empresa de ATIVA, senão
o posterior (B23). `opcao_*` ficam NULL quando desconhecidos (sem `COALESCE(..., FALSE)`).

**`gold.fato_empresa_mes`** — flags derivadas só da própria linha do tempo da matriz na `dim_empresa`
(não lê mais a OBT):

| Coluna | Regra |
|---|---|
| `situacao_inicio_mes` | situação no mês anterior; NULL se a empresa não existia |
| `ativa_inicio_mes` / `ativa_fim_mes` | como hoje |
| `teve_abertura_mes` | primeira aparição da empresa, em mês > baseline |
| `teve_reativacao_mes` | existia, não estava ATIVA, terminou ATIVA |
| `entrou_estoque_mes` | `NOT ativa_inicio AND ativa_fim` |
| `saiu_estoque_mes` | `ativa_inicio AND NOT ativa_fim` |
| `teve_baixa_mes` | existia, não estava BAIXADA, terminou BAIXADA |
| `teve_falencia_mes` | `teve_baixa_mes` e motivo ∈ {05, 06, 10} |
| `teve_inaptidao_mes` | existia, não estava INAPTA, terminou INAPTA |
| `teve_suspensao_mes` | idem para SUSPENSA |
| `porte_empresa`, `opcao_pelo_mei`, `opcao_pelo_simples` | valor do mês anterior quando `saiu_estoque_mes` (B23); senão o do mês |

Identidade obrigatória (exceto no baseline, onde não há fluxo por construção):
`estoque_final(M) = estoque_inicial(M) + entradas(M) − saídas(M)` e
`estoque_inicial(M) = estoque_final(M−1)`.
A tabela guarda só linhas em que a empresa está ativa no início ou no fim do mês, ou em que algo mudou
(abertura ou mudança de situação) — empresas baixadas há anos não geram linha todo mês.

**`gold.fato_indicadores_setoriais_mes`** — colunas novas: `qtd_reativacoes`, `qtd_entradas_estoque`,
`qtd_saidas_estoque`, `qtd_baixas_de_ativas`, `qtd_suspensoes`. `crescimento_liquido` =
`qtd_entradas_estoque − qtd_saidas_estoque`. Taxas por mil usam numeradores restritos a quem estava ativo
no início (`qtd_baixas_de_ativas`, …).

**`gold.ds_coorte_sobrevivencia`** — morte = `data_situacao_cadastral` da primeira versão BAIXADA
(só BAIXADA é terminal); censura = último mês com dados na `dim_empresa`, não `CURRENT_DATE`; sem `FIRST()`;
covariáveis da primeira versão observada; coluna `coorte_pos_baseline`.

**`gold.dashboard_mercado_ativo`** — coluna nova `codigo_natureza_juridica` e
`exibir_razao_social BOOLEAN` (FALSE para MEI, para natureza 2135 — empresário individual — e para razão
social com 11 dígitos seguidos).

### 2.5 Migração

As quatro tabelas incrementais mudam de schema (`log_eventos_negocio`, `fato_mudanca_estado`,
`obt_dashboard_eventos`, `fato_empresa_mes`). Em desenvolvimento (D-08): `DROP TABLE` e reconstruir a janela
— script único em `src/jobs/migracoes/2026-11-14_semantica_eventos.sql`, rodado uma vez antes do Job.

### 2.6 Impacto LGPD

Nenhum dado novo. O dashboard deixa de listar razão social de empresário individual (C-12). O achado de
11 dígitos em razão social fora da condição da máscara (2.816 casos, natureza 2135 com porte 03/05) fica no
backlog para decisão do autor — a landing não é alterada aqui.

### 2.7 Impacto no TCC

Metodologia: os dois relógios e a data de extração do snapshot são limitações/decisões a descrever; a
classificação por código de motivo substitui a por texto; "falência" passa a ser um número pequeno e correto.

## 3. Tarefas

- [x] T1 `log_eventos_negocio.sql` + DQ (contrato 2.4)
- [x] T2 `dim_empresa.sql` + DQ (C-02, colunas novas, B24)
- [x] T3 `dim_mudanca_estado.sql`, `fato_mudanca_estado.sql`, `obt_dashboard_eventos.sql` + DQs
- [x] T4 `fato_empresa_mes.sql`, `fato_indicadores_setoriais_mes.sql`, `d_coorte_sobrevivencia.sql`,
      `dashboard_mercado_ativo.sql`, `dq_gold.sql`
- [x] T5 `socios.sql` idempotente e com validação fora do `GROUP BY` (C-05, C-06)
- [x] T6 `gerar_dashboard.py` + JSON
- [x] T7 ambiente de amostra no SQL warehouse para validar tudo ponta a ponta sem reprocessar a base
- [ ] T8 reexecução real (log → silver_final → gold) — **só com autorização do autor**

## 4. Critérios de aceite

- [x] Na amostra: todos os DQ passam; identidade de estoque fecha com diferença zero em todos os meses.
- [x] `SUM(teve_abertura_mes)` por mês = matrizes cuja primeira aparição é naquele mês.
- [x] Nenhuma `categoria_macro_evento` fora da lista da seção 2.3 + nomes de evento.
- [x] Todos os datasets do dashboard executam sem erro contra as tabelas novas (amostra).
- [x] Dashboard: todos os widgets atuais continuam existindo (19 mantidos, 2 novos).
- [ ] Dashboard conferido visualmente no workspace.

## 5. Notas de execução

**2026-11-14 — amostra de 1% no SQL warehouse serverless 2X-Small** (`scripts/amostra/`, schemas `tcc_receita.amostra_*`):

- Pipeline completo (4 SCD2 → log → silver_final → gold) e os 12 arquivos de DQ passaram. O pipeline todo roda
  em ~3 min na amostra.
- `EMPRESA NASCEU` por mês de detecção: 2.988 / 3.195 / 4.060 = matrizes novas no snapshot (antes: ~25% disso).
- `ENTROU NO MEI`: ~2,3 mil por mês na amostra (antes: ~1). Falências: 4. Reativações: 1.108.
- Baixas de matriz por porte: MEI 3.738, ME 1.266, DEMAIS 169, EPP 80 (antes o MEI quase não aparecia).
- Identidade e continuidade de estoque fecham em todos os meses. O DQ de continuidade achou um defeito na regra
  da matriz canônica (transferência de matriz entre estabelecimentos) — corrigido: vale a matriz de cada período.
- As 11 consultas do dashboard rodaram contra a amostra. A prévia do dashboard foi publicada no workspace, mas
  **a renderização dos widgets não foi conferida visualmente**.
- Achado de spec: "situação igual, só o motivo mudou" precisa ser a primeira condição das categorias.

**Para executar na base real (T8), nesta ordem:**

1. Rodar uma vez `src/jobs/migracoes/2026-11-14_semantica_eventos.sql` (apaga as 5 tabelas que mudaram de schema).
2. Colar o YAML novo no Job (o DAG mudou: `gold_fato_empresa_mes` não depende mais da OBT).
3. Rodar o Job com `2023-05` a `2023-08` **só a partir de `si_log_eventos_negocio`** (bronze e as 4 SCD2 não
   mudaram de resultado; `si_socios` só precisa rodar de novo quando houver mês novo).
4. Reimportar `src/gold/dashboard/tcc_receita.lvdash.json` no dashboard oficial.

Risco conhecido: `dim_empresa` ficou mais cara (estimativa do autor da mudança: 1,5–2× o tempo anterior);
`fato_empresa_mes` deve ficar mais barata (guarda ~23 mi linhas/mês em vez de 54 mi).
