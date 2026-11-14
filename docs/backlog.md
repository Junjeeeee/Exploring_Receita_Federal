# Backlog

> Reescrito em 2026-11-14. Só o que está **aberto**, por prioridade. Um item = uma linha curta; o detalhe
> (evidência, cenário, correção) fica no documento apontado em **Onde**.
> Histórico e narrativa dos itens antigos: [`backlog_historico.md`](backlog_historico.md).
> Origem dos IDs: `B` = backlog antigo · `C` = [`critica.md`](critica.md) (2026-11-14) · `D` = decisão pendente.

**Estado:** ⬜ aberto · 🔧 código pronto, falta executar · 🧪 validado na amostra, falta rodar na base · ✅ feito

## Alvo atual — dashboard funcionando na segunda, 2026-11-16

Spec: [`specs/features/2026-11-14-semantica-eventos-e-fluxos.md`](specs/features/2026-11-14-semantica-eventos-e-fluxos.md).
Validação: ambiente de amostra (1% da base) no SQL warehouse — `scripts/amostra/`. Em 2026-11-14 o pipeline inteiro
(SCD2 → log → silver_final → gold → todos os DQ) passou na amostra e as 11 consultas do dashboard rodaram contra ela.
Prévia publicada no workspace: *TCC Receita — Dinâmica empresarial (PRÉVIA, amostra 1%)*.

| ID | O que | Onde | Estado |
|---|---|---|---|
| B21 / C-04 | Aberturas subcontadas: evento datado pela data real, empresa só entra no snapshot seguinte. Todo evento passa a ter `mes_referencia` = mês de detecção e `data_evento` = data real | spec §2.1, T1 | 🧪 |
| C-02 | `dim_empresa` trunca a vigência de 92.613 CNPJs | spec §2.4, T2 | 🧪 |
| C-03 | `teve_baixa_mes` repetia e contava filial; flags passam a vir da transição da matriz | spec §2.4, T4 | 🧪 |
| B22 / C-07 | "Falência" = toda baixa; quatro categorias inalcançáveis. Classificação por código de motivo | spec §2.3, T3 | 🧪 |
| B23 | Porte da baixa usava o estado posterior (MEI virava ME) | spec §2.4, T3/T4 | 🧪 |
| B24 | UF `'RJ,SP'` etc. (3 CEPs ruins → 5.887 linhas) | T2 | 🧪 |
| C-16 | Eventos sem versão anterior perdidos (`ENTROU NO MEI`: 443 de ~745 mil) | T1 | 🧪 |
| C-12 | Dashboard listava razão social de empresário individual | T4, T6 | 🧪 |
| C-13 | Mesma métrica por dois caminhos; gráfico de situação misturava eventos de sócio/CNAE | T6 | 🧪 |
| C-08 | DQ que não consegue falhar; falta identidade de estoque e reconciliação | T2–T4 | 🧪 |
| — | Dashboard v2: 6 páginas com pergunta por página, big numbers do último mês com Δ, idade na baixa (`fato_empresa_mes`). Validado na amostra; prévia atualizada; **renderização não conferida** (risco: contadores com `try_divide`) | [`specs/features/2026-11-14-dashboard-v2-narrativa.md`](specs/features/2026-11-14-dashboard-v2-narrativa.md) | 🧪 |
| — | Reexecutar log → silver_final → gold na base real (4 meses) e republicar o dashboard | spec T8 | ⬜ **depende de autorização** — passo a passo na spec §5 |

## Decisões pendentes do autor

| ID | Pergunta | Padrão adotado enquanto isso |
|---|---|---|
| D-b | Baixa de filial conta para a empresa? | não (D-14): empresa = matriz |
| D-c | Lista de "falência" por código | {05, 06, 10} (D-15) |
| D-d | Reativação (INAPTA → ATIVA, ~37 mil/mês) entra como fluxo próprio no dashboard? | sim |
| D-e | Dados de contato de MEI (telefone, e-mail, logradouro) em silver_final/gold | mantidos; não aparecem no dashboard |
| D-f | Plano B do TCC (dashboard + análises; grafo e ML em trabalhos futuros) — decidir com a orientadora | — |

## P1 — antes da janela completa (~40 meses)

| ID | O que | Onde | Estado |
|---|---|---|---|
| D-a | **LGPD:** máscara de CPF da landing ampliada para toda natureza 2135 (antes só porte 01; 2.816 razões de porte 03/05 em claro). **Código pronto, falta reextrair `empresas` na landing** e reprocessar a cadeia; até lá os dados já extraídos seguem com as 2.816 razões em claro e a gold continua escondendo via `exibir_razao_social = FALSE` | [`specs/features/2026-11-14-mascara-cpf-empresario-individual.md`](specs/features/2026-11-14-mascara-cpf-empresario-individual.md), D-16 | 🔧 |
| D-a′ | ~370 razões com 11 dígitos seguidos em **outras** naturezas: não mascaradas (podem ser números legítimos de PJ). Medir após a reextração (por natureza + dígito verificador de CPF) e decidir | mesma spec | ⬜ medir |
| C-05 | `socios.sql` não idempotente (versão inativa fantasma ao reexecutar). Tabela atual **não** está contaminada (medido: 0) | critica §4.2, T5 | 🧪 |
| C-06 | `socios.sql`: bronze vazia fecha todos os vínculos (assert dentro de `GROUP BY`) | critica §4.2, T5 | 🧪 |
| C-01 | Sobrevivência: morte pela data real, censura no último snapshot | spec §2.4, T4 | 🧪 |
| C-09 | Bronze descarta sócio sem documento/nome em silêncio; falta reconciliação landing = bronze | critica §4.3 | ⬜ |
| B17 | Bronze lenta: 3 shuffles completos por mês | backlog_historico | ⬜ |
| B20 | SCD2 leem a bronze 2×; log lê cada SCD2 4–5× | backlog_historico, critica §6 | ⬜ (log: T1) |
| C-21 | Timeout de 4 h e cluster 1–4 não comportam a janela completa; rodar em blocos | critica §4.3 | ⬜ |
| C-19 | `change_hash` depende da ordem das colunas da landing; fixar `colunas_hash` | critica §4.2 | ⬜ |
| C-18 | Sócio com nome corrigido = `pk` nova → saída sem entrada (com T1 a entrada passa a ser detectada) | critica §4.2 | ⬜ medir |
| B16 | PKs repetidas na Receita (6/mês em sócios, 2–3 em empresas): entender e decidir a regra | backlog_historico | ⬜ |
| C-11 | `dim_mudanca_estado` com listas de CNAE na chave (cardinalidade alta) | critica §4.1 | ⬜ (parcial em T1/T3) |
| C-24 | `gerar_job.py` não detecta `.sql` novo; branch e bucket fixos no gerador | critica §4.3 | ⬜ |
| — | Matriz canônica passou a ser a matriz vigente em cada período (transferência de matriz fazia a empresa sumir da dimensão; achado pelo DQ de continuidade). Com mais de uma matriz no mesmo período e sobreposição parcial, a versão de ordem maior é descartada inteira | `dim_empresa.sql` | 🧪 |
| — | Município com grafias de fontes diferentes (com acento via CEP, sem acento via domínio RFB) entre municípios distintos; padronizar | `dim_empresa.sql` | ⬜ |
| — | Coorte: morte no mesmo mês do nascimento vira tempo NULL (`tempo_invalido`); medir em dias | `d_coorte_sobrevivencia.sql` | ⬜ |
| C-10 | DQ: entidade corrente ausente do último snapshot (hoje: 0) | critica §3.5 | ⬜ |

## P2 — qualidade e limpeza

| ID | O que | Estado |
|---|---|---|
| C-14 | Features de ML com estado de fim de mês (vazamento); criar colunas `*_inicio_mes` | ⬜ (parcial em T4) |
| C-17 | `EMPRESA NASCEU` por join de todas as versões | ⬜ (T1) |
| C-15 | Rótulos de "desconhecido" divergentes entre dim e gold | ⬜ |
| C-20 | DQ da bronze concorrendo em `dq_metrics_log` | ✅ 2026-11-14 (append-only) |
| C-22 | Argumento digitado errado virava "mês anterior" | ✅ 2026-11-14 |
| C-23 / B6 | Máscara MEI sem teste vivo; pytest quebrado | ✅ 2026-11-14 (27 testes; `poetry lock` rodado) |
| B8 | Volumetria engolia erro de leitura | ✅ 2026-11-14 |
| — | `ruff check` acusa 105 erros (79 automáticos); ligar no CI depois de limpar | ⬜ |
| — | Docs desatualizados em relação ao run de 2026-11-08 (tabela em critica §2.1) | ⬜ |
| — | Seed BCB (Selic/IPCA) fixo até 2025-12 → ingerir da API | ⬜ |
| — | Apagar sem uso: `vw_dashboard_eventos_cnae.sql`, `ds_visao_macro.sql`, `ds_micro_*.sql`, `bronze/configs/cnaes.py`, `config/*.yaml`, `.claude/context_from_other_llm.md`, `checkpoint_estado.json`, `last_processed_month.txt`, notebooks quebrados | ⬜ |
| — | `README.md` da raiz vazio | ⬜ |
| — | Landing: mês publicado pela metade é aceito e congelado; guardar data de extração dos ZIPs | ⬜ |
| — | Dicionário de dados (pedido da orientadora): gerar a partir dos `COMMENT` das DDLs | ⬜ |

## Ideias (sem compromisso)

- Identidade de sócio PF: medir colisões e mudanças de nome antes de grafo/grupos.
- Pseudonimização de nome palavra a palavra (HMAC com chave fora do repo).
- "Grande setor" (D-10): fonte citável ou trocar por seção CNAE.
- Grafo de QSA e modelo de falência: depois da primeira entrega (ver `mineracao_receita.md`).

## Medições de referência (2026-11-14, base de 4 meses)

| O que | Valor |
|---|---|
| Matrizes ativas (mai → ago/2023) | 22,29 mi → 22,93 mi |
| Matrizes novas por mês (jun/jul/ago) | 303 mil / 314 mil / 397 mil |
| Parcela das empresas iniciadas em M que só aparece no snapshot M+1 | ~70% |
| ATIVA → BAIXADA por mês | 155 mil / 136 mil / 165 mil |
| Reativações em 3 meses | ~111 mil |
| Baixas com motivo de falência (05) em 3 meses | ~430 |
| Eventos de estado cadastral: matriz × filial | 639.841 × 22.108 |
| Sócios com primeira versão inativa · estabelecimentos correntes fora do último snapshot | 0 · 0 |
| Linhas do fato de eventos sem empresa / sem CNPJ | 0 / 0 |
| Municípios com mais de um nome por código IBGE | 0 |
