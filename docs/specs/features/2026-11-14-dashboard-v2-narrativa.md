# Feature: dashboard v2 — narrativa em 6 páginas

**Status:** implementado e validado na amostra de 1%; **renderização não conferida** · 2026-11-14 (plano aprovado pelo autor em 2026-11-14; detalhes de widget delegados)
**Camadas afetadas:** gold (`fato_empresa_mes`, `dq_gold.sql`), dashboard (`src/gold/dashboard/`)
**Branch:** `worktree-dashboard-segunda`
**Substitui:** o layout de [`dashboard-v0.md`](dashboard-v0.md) (datasets e regras de LGPD continuam valendo).
**Contrato de colunas da gold:** [`2026-11-14-semantica-eventos-e-fluxos.md`](2026-11-14-semantica-eventos-e-fluxos.md) § 2.4.

## 1. Requisito

- Problema: lendo o v0, "não se pega nenhuma informação relevante": contadores com totais do período sem
  comparação, uma página com 14 gráficos de mesmo peso, títulos que descrevem o gráfico e não o achado.
- Quem consome: banca e orientadora do TCC. A página "Mercado ativo" (aplicação prática) fica no fim.
- Pergunta por página; big numbers no topo; títulos em forma de pergunta ou achado estável (sem número no
  título: o número muda com filtro e com a base); 1–2 linhas de texto no topo dizendo a pergunta e como ler.
- Fora de escopo: mapa (não há confirmação de que o Lakeview desenhe UFs brasileiras de forma confiável),
  taxas que reajam a filtro de porte/área nos rankings (ver 2.3), período maior que a janela atual.

## 2. Design

### 2.1 Restrições do Lakeview que guiam o desenho

| Restrição | Consequência |
|---|---|
| Caixa de texto não mostra número calculado | a história vai em contadores e títulos; texto fixo só explica como ler |
| Comparação nativa do contador (Value × Comparison) não tem sintaxe JSON documentada | **não usada**: a variação vem como contador próprio "Δ vs. mês anterior" = `SUM` de uma coluna `valor(M) − valor(M−1)` (linear, então respeita qualquer filtro) |
| Filtro de campo filtra linhas do dataset antes da agregação do widget | contador de "último mês" = `SUM` de coluna que só vale no último mês (0 nos outros) |
| Expressão composta no campo do widget (`SUM(a)/SUM(b)`, `try_divide`) é avaliada errado ou não desenha (testado na prévia) | percentuais = `AVG` de um indicador 0/100 em dataset **uma linha por empresa** (AVG respeita filtros); "aberturas por baixa" vem pronta do SQL (Brasil, não reage a filtro) |
| Mediana | `PERCENTILE(col, 0.5)` (visto em dashboard exportado pela própria Databricks) sobre dataset uma linha por baixa |
| Taxa por 1.000 ativas por grupo | precisa de razão de somas → vem pronta do dataset (Brasil por grupo); por isso só reage aos filtros que existem no dataset |

### 2.2 Gold: idade na baixa (`fato_empresa_mes`)

Colunas novas no fim de `gold.fato_empresa_mes` (incremental; já está no `DROP` de
`src/jobs/migracoes/2026-11-14_semantica_eventos.sql`, que ainda não foi rodado na base real → sem migração extra):

| Coluna | Regra |
|---|---|
| `idade_na_baixa_meses INT` | só quando `teve_baixa_mes`: meses completos entre `data_inicio_atividade` e `data_situacao_cadastral` (data real da baixa, versão BAIXADA da matriz). NULL se alguma data falta ou se a baixa é anterior ao início |
| `faixa_idade_baixa STRING` | NULL se não é baixa; `00-01 ano`, `01-02 anos`, `02-05 anos`, `05-10 anos`, `10+ anos`; `sem data` quando a idade é NULL (não some em silêncio) |

Por que no fato e não na consulta do dashboard nem em tabela `CREATE OR REPLACE`: as duas datas já são lidas
da `dim_empresa` dentro de `fato_empresa_mes`; calcular no dataset exigiria juntar a `dim_empresa` inteira a
cada refresh, e uma tabela nova exigiria arquivo, task e DQ novos. Rótulos com dois dígitos ordenam certo em
ordem alfabética (o gráfico não depende de ordenação customizada).

DQ (`dq_gold.sql`, bloco 2): `faixa_idade_baixa` não nula ⇔ `teve_baixa_mes`; `idade_na_baixa_meses` ≥ 0;
`sem data` ≤ 5% das baixas da janela (limiar **[padrão]**; o número observado vai para as notas).

### 2.3 Datasets do dashboard

Mantidos do v0: `painel`, `taxas_setor` (+ `taxa_saldo_por_mil`), `taxas_uf` (+ abertura/fechamento e piso),
`eventos`, `top_cnaes`, `macro`, `mercado`, `mercado_bairros`, `mercado_empresas`.
Removidos: `estoque` (substituído por `resumo_mes`) e `situacao` (ver 2.5).

| Dataset novo | Fonte | Grão | Para quê |
|---|---|---|---|
| `resumo_mes` | `fato_indicadores_setoriais_mes` | UF × porte × área | contadores do último mês e Δ vs. mês anterior (colunas `*_ultimo_mes`, `*_delta`) |
| `ultimo_mes` | idem | 1 linha | tabela com o mês de referência dos contadores e o anterior |
| `estoque_mes` | idem | mês × UF × porte × área | série do estoque de ativas (inclui o baseline) |
| `fluxos_empresa` | `fato_empresa_mes` | 1 linha por empresa × mês × tipo (abertura, baixa) (sem baseline); empresa que abre já BAIXADA (D-17) gera duas linhas, colunas de baixa só na linha `Baixas` | % MEI, % falência, idade mediana, % < 2 anos, composição por porte |
| `baixas_idade` | `fato_empresa_mes` | mês × UF × porte × área × faixa | baixas por faixa de idade |
| `baixas_motivo` | `fato_empresa_mes` (`teve_baixa_mes`, UF/porte/área) + `obt_dashboard_eventos` (só a categoria) | mês × UF × porte × área × categoria | baixas de matriz por categoria de motivo: categoria do evento posterior BAIXADA, anterior ≠ BAIXADA (inclui `INEXISTENTE`, a primeira aparição — D-17); sem evento → `NAO IDENTIFICADO`; total e porte = os do fato (D-17b) |
| `extremos_setor` / `extremos_uf` | `fato_indicadores_setoriais_mes` | 2 linhas | "mais cresce" e "mais encolhe" (taxa de saldo por 1.000 ativas, com piso de volume) |

Piso de volume dos rankings **[padrão]**: média mensal de ativas no início do mês ≥ 100 (na base real todas
as UFs e áreas passam; na amostra de 1% o piso tira ruído de UFs pequenas). Rótulos de "não sei" ficam fora dos
rankings: UF `NI` e `EX` (exterior); áreas `OUTROS / NAO ESPECIFICADO` e `CODIGO NAO LOCALIZADO`.

### 2.4 Páginas

Nomes de widget abaixo sem o prefixo de página; no código levam o número da página (`c1_ativas`, `b4_idade`…).

Grade de 12 colunas. "Filtros" = filtros de campo da página e os datasets que eles alcançam.

**1. Resumo — O estoque de empresas está crescendo?** Filtros: UF, porte, grande área → `resumo_mes`, `estoque_mes`, `painel`.

| Widget | Tipo | Dataset · campo | Título |
|---|---|---|---|
| `t_mes` | tabela | `ultimo_mes` | Mês dos contadores |
| `c_ativas` / `c_ativas_d` | contador | `resumo_mes` · `SUM(ativas_ultimo_mes)` / `SUM(ativas_delta)` | Empresas ativas no fim do último mês / Δ vs. mês anterior |
| `c_aberturas` / `_d` | contador | `aberturas_ultimo_mes` / `aberturas_delta` | Aberturas no último mês / Δ |
| `c_baixas` / `_d` | contador | `baixas_ultimo_mes` / `baixas_delta` | Baixas no último mês / Δ |
| `c_saldo` / `_d` | contador | `saldo_ultimo_mes` / `saldo_delta` | Saldo do estoque no último mês / Δ |
| `c_ab_por_baixa` / `_ant` | contador | `aberturas_por_baixa` · razão calculada em SQL (Brasil) | Aberturas por baixa no último mês / no anterior (Brasil, não muda com os filtros) |
| `l_estoque` | linha | `estoque_mes` · `SUM(estoque_final_ativas)` | O estoque de empresas ativas cresce mês a mês? |
| `l_entradas_saidas` | linha | `painel` · entradas, saídas, aberturas, baixas | Entram mais empresas no estoque do que saem? |

**2. Quem — Quem abre e quem fecha?** Filtros: UF, grande área → `fluxos_empresa`, `painel` (porte é o assunto da página).

| Widget | Tipo | Dataset · campo | Título |
|---|---|---|---|
| `c_mei_ab` / `c_mei_bx` | contador | `fluxos_empresa` · `AVG(pct_mei_abertura)` / `AVG(pct_mei_baixa)` | % das aberturas que são MEI / % das baixas que são MEI |
| `t_setor_extremos` | tabela | `extremos_setor` | Setor que mais cresce e que mais encolhe (Brasil) |
| `b_porte` | barras empilhadas | `fluxos_empresa` · x = fluxo, y = `SUM(qtd)`, cor = porte | O MEI domina tanto as aberturas quanto as baixas? |
| `b_saldo_area` | barras | `taxas_setor` · `taxa_saldo_por_mil` | Quais setores crescem mais, em proporção ao seu tamanho? |
| `b_taxa_area` | barras | `taxas_setor` · `taxa_fechamento_por_mil` | Em quais setores as empresas fecham mais? |
| `t_cnaes` | tabela | `top_cnaes` | Quais atividades concentram as aberturas? |

**3. Onde — Onde cresce mais?** Sem filtros (taxas prontas por UF, Brasil).

| Widget | Tipo | Dataset · campo | Título |
|---|---|---|---|
| `t_uf_extremos` | tabela | `extremos_uf` | UF com maior e com menor crescimento líquido |
| `b_saldo_uf` | barras | `taxas_uf` · `taxa_crescimento_por_mil` | Onde o estoque cresce mais, em proporção ao seu tamanho? |
| `t_uf` | tabela | `taxas_uf` | Taxas por UF: abertura, fechamento e saldo por 1.000 ativas |

**4. Como fecha — Por que e com que idade as empresas fecham?** Filtros: UF, porte, grande área →
`fluxos_empresa`, `baixas_idade`, `baixas_motivo`, `painel`, `eventos`.

| Widget | Tipo | Dataset · campo | Título |
|---|---|---|---|
| `c_falencia` | contador | `fluxos_empresa` · `AVG(pct_falencia_baixa)` | % das baixas por falência ou liquidação judicial |
| `c_idade_mediana` | contador | `PERCENTILE(idade_baixa_anos, 0.5)` | Idade mediana na baixa (anos) |
| `c_menos_2` | contador | `AVG(pct_baixa_menos_2_anos)` | % das baixas com menos de 2 anos de vida |
| `c_reativacoes` | contador | `painel` · `SUM(qtd_reativacoes)` | Reativações no período |
| `b_motivo` | barras | `baixas_motivo` · `SUM(qtd_empresas)` por categoria | Falência é rara: por que as empresas são baixadas? |
| `b_idade` | barras | `baixas_idade` · `SUM(qtd_baixas)` por faixa | Com que idade as empresas fecham? |
| `l_saidas` | linha | `painel` · baixas, inaptidões, suspensões, reativações | Além da baixa: inaptidão, suspensão e reativação mês a mês |
| `b_tipo` | barras | `eventos` · `SUM(qtd_eventos)` por tipo | O que mais muda no cadastro além da situação? |

**5. Mercado ativo — Quem são meus concorrentes locais?** Igual ao v0 (só texto de abertura em forma de pergunta).

**6. Sobre os dados — Como ler estes números?** Texto fixo: mês de detecção × data real do evento; empresa =
matriz; categorias por código de motivo; limites (3 meses de fluxo, jun–ago/2023; 2023-05 é baseline); taxas e
piso; o que reage a filtro. `l_macro` (Selic e IPCA) mora aqui como contexto: com 3 meses de fluxo não sustenta
leitura de causa.

### 2.5 Redistribuição dos widgets do v0

| v0 | v2 |
|---|---|
| `c_ativas`, `c_aberturas`, `c_baixas`, `c_saldo` (período) | p1, último mês + Δ |
| `c_reativacoes` | p4 |
| `l_fluxos` | p1 (`l_entradas_saidas`) e p4 (`l_saidas`) |
| `l_macro` | p6 |
| `b_saldo_uf` | p3 (com piso) |
| `b_taxa_area` | p2 |
| `b_tipo` | p4 (último widget) |
| `b_situacao` | **removido**: suas categorias aparecem em `b_motivo` (baixas) e `l_saidas` (inaptidão, suspensão, reativação); mantê-lo repetiria os mesmos números com outro corte |
| `t_cnaes` | p2 |
| página Mercado ativo | p5 sem mudança |

### 2.6 LGPD e TCC

Nenhuma coluna pessoal nova; `exibir_razao_social` continua controlando a lista de empresas; `fluxos_empresa`
não expõe CNPJ nem nome (só UF, porte, área e indicadores). Impacto no TCC: os prints do capítulo de
Resultados passam a seguir as 6 perguntas; "idade na baixa" é definição nova a descrever na Metodologia
(ver `docs/tcc.md`).

### 2.7 Ajustes pós-prévia (autor, 2026-11-15)

1. **Linguagem**: nada de "estoque", "saldo", "fluxo", "snapshot", "baseline", "detecção" nem "Δ" no texto exibido
   (verificado em `validar()`). "Crescimento líquido" = passaram a estar ativas − deixaram de estar ativas,
   definido na descrição de cada widget que o usa. Grande área aparece como "Setor"; CNAE como "Atividade".
2. **Ordem das barras**: série de cor por porte em ordem fixa (MEI > ME > DEMAIS > EPP > não informado; estável
   na amostra nas aberturas e nas baixas), maior na base e primeiro na legenda (`sort: custom-order` no encoding de
   cor, sintaxe dos dashboards de exemplo da Databricks). Faixas de idade em ordem natural. Barras de uma série:
   por valor, maior no topo (horizontais) ou à esquerda (verticais).
3. **Bairros reagem a todos os filtros** do Mercado ativo: `mercado_bairros` passa a grão bairro × CNAE, ainda
   restrito aos 20 bairros com mais ativas de cada município (contando todas as atividades). Com filtro de CNAE, as
   barras mostram as empresas daquele CNAE nesses 20 bairros (não os 20 maiores bairros daquele CNAE).
4. **Filtro de bairro** no Mercado ativo, em todos os widgets; bairro nulo/vazio vira `NAO INFORMADO` (mesma
   expressão `BAIRRO` nos três datasets). Gold inalterada.

## 3. Tarefas

- [x] T1 `fato_empresa_mes.sql`: `idade_na_baixa_meses`, `faixa_idade_baixa`; DQ em `dq_gold.sql`
- [x] T2 `gerar_dashboard.py`: 6 páginas, datasets novos, `validar()` ampliado (todas as páginas com texto de abertura; contadores só com agregação); JSON regenerado; README
- [x] T3 Amostra: `fato_empresa_mes` + painel + DQ gold; cada dataset do dashboard executado (sem erro, linhas > 0); filtros simulados nos contadores; identidade de estoque
- [x] T4 Atualizar a prévia (`01f1e03111521e8fa0a9cf1aa4f96e4e`) e ler de volta pela API
- [x] T5 Backlog (Alvo atual), `dashboard-v0.md` apontando para esta spec

## 4. Critérios de aceite

- [x] DQ gold passa na amostra (incluindo os novos de idade na baixa).
- [x] Todos os datasets executam na amostra com linhas > 0.
- [x] Contadores do resumo: `ativas_fim(M) = ativas_fim(M−1) + saldo(M)`; % MEI das baixas perto de 70% (amostra).
- [x] Filtro de UF e de porte nos datasets dos contadores devolve subconjunto coerente (soma das UFs = Brasil).
- [x] Prévia atualizada com 6 páginas; `validar()` sem erro.
- [ ] Renderização conferida no workspace (pelo autor: não há login de navegador no ambiente do agente).

## 5. Notas de execução

**2026-11-14 — amostra de 1%, SQL warehouse serverless** (`scripts/amostra/`):

- `fato_empresa_mes` recriada na amostra (`DROP` em `amostra_gold` por causa das colunas novas) + painel + `dq_gold.sql`:
  tudo passou. Baixas por faixa: 00-01 ano 1.308 · 01-02 926 · 02-05 1.442 · 05-10 770 · 10+ 658 · **sem data 0**.
- Os 17 datasets executam (linhas > 0) e as 37 consultas de widget (fora filtros e textos) foram simuladas em SQL (mesmas expressões dos
  campos) sem erro, sem filtro, com `uf = 'SP'` e com `porte_empresa = 'MEI'`.
- Big numbers (Brasil, amostra): último mês 2023-08 × 2023-07. Ativas 228.024 (Δ +2.740 = saldo: identidade
  fecha); aberturas 4.060 (Δ +865); baixas 1.826 (Δ +252); saldo 2.740 (Δ +719); 2,22 aberturas por baixa (2,03 no
  mês anterior). MEI = 72,8% das aberturas e 67,4% das baixas. Falência = 0,08% das baixas (4 de 5.104). Idade
  mediana na baixa 2,4 anos; 43,8% fecham com menos de 2 anos. 1.108 reativações. Setor: Serviços (+11,3‰) e
  Agropecuária (+5,8‰). UF: RR (+22,4‰) e AP (+4,7‰) — na amostra, ruído de UF pequena.
- Com filtro, Δ ativas ≠ saldo (SP: 779 × 783; MEI: 1.699 × 1.928) porque empresas trocam de UF/porte/setor; o
  texto do contador explica. Soma dos grupos = Brasil por construção (o widget só soma linhas).
- Achado: nenhuma transição ATIVA → INAPTA na amostra em jun–ago/2023 (inaptidões = 0 nos três meses); as
  transições INAPTA → ATIVA/BAIXADA existem. Provável calendário de lotes da Receita; conferir na base real.
- Prévia `01f1e03111521e8fa0a9cf1aa4f96e4e` atualizada e lida de volta pela API: 6 páginas, 54 widgets,
  17 datasets apontando para `amostra_*`. **Renderização não conferida** (sem navegador no ambiente).
  Maior risco: `c1_ab_por_baixa`, `c1_ab_por_baixa_ant` (`try_divide(SUM, SUM)`), depois `c4_idade` (`PERCENTILE`).

**2026-11-15 — ajustes pós-prévia (§ 2.7)** na amostra: 17 datasets com linhas > 0; widgets simulados sem erro
com filtros `municipio = 'RIO DE JANEIRO' AND nome_cnae = 'CABELEIREIROS…'` (contador 418, 19 bairros),
`… AND bairro = 'COPACABANA'` (contador 231 = barra do bairro 231) e `bairro = 'NAO INFORMADO'` (138 empresas).
Prévia atualizada e lida de volta (6 páginas, 55 widgets, `sort` de cor presente). Renderização não conferida.

**2026-11-15 — conferência na tela pelo autor:** (A) `c1_ab_por_baixa`/`_ant` com `try_divide(SUM, SUM)` e nome
`razao(a,b)` mostravam "Select fields to visualize"; trocados por `SUM(a) / SUM(b)` com nome simples, e `validar()`
passa a recusar `try_divide` e vírgula em nome de campo. Página temporária "Teste (apagar)" (só na prévia, via
`gerar_dashboard --com-pagina-teste --saida <arq>`) com quatro variantes: A `SUM/SUM`, B `try_divide` com nome
simples, C razão em SQL + `SUM` (Brasil), D razão em SQL sem agregação (Brasil). (B) a pilha por porte saiu
invertida: no Lakeview o primeiro valor de `custom-order` vai para o topo; `ORDEM_PORTE` agora vai do menor para o
maior (MEI na base).

**2026-11-15 — resultado da página de teste e fechamento:** A (`SUM(a) / SUM(b)` no widget) mostrou 4,06 mil =
só `SUM(aberturas)`: **o Lakeview avalia errado, em silêncio, expressão composta no campo do widget**; B
(`try_divide`) não desenha; C e D (razão calculada em SQL) mostram 2,22. Decisão: `c1_ab_por_baixa` e
`c1_ab_por_baixa_ant` leem o dataset `aberturas_por_baixa` (uma linha, Brasil, razão em SQL; título avisa que não
muda com os filtros). `validar()` só aceita coluna pura, `DATE_TRUNC("MONTH", col)` ou UM agregado simples
(`SUM`/`AVG`/`PERCENTILE(col, 0.5)`) sobre UMA coluna. Página de teste e flag removidas. Pilha por porte com MEI na
base aprovada; dashboard v2 aprovado pelo autor. Prévia republicada com 6 páginas.

**Para a base real:** nenhuma migração além da já prevista (`fato_empresa_mes` está no `DROP` de
`src/jobs/migracoes/2026-11-14_semantica_eventos.sql`); nenhum arquivo SQL novo, Job inalterado.
