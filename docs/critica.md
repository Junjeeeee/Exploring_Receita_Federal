# Crítica do projeto — revisão de 2026-11-14

> Revisão crítica de **documentação + código** no commit `bf85244` (branch `ceps_ibge_and_dashboard_v2`).
> Escrita por um assistente de IA no papel de revisor; **nada foi executado** (nem cluster, nem DuckDB, nem
> pytest). Tudo aqui é leitura estática.
>
> Como ler o grau de certeza:
> - **[código]** — a lógica foi confirmada lendo o arquivo citado. O defeito existe no texto do SQL/Python.
> - **[medir]** — o mecanismo existe no código, mas o tamanho do estrago depende dos dados. Há uma query de
>   medição na seção 8.
> - **[suspeita]** — depende de comportamento de runtime ou de premissa sobre a fonte que não foi verificada.
>
> Itens já registrados no `backlog.md` (B1–B24) não são repetidos, exceto quando o backlog está errado ou
> quando há causa concreta nova. Os IDs `C-nn` são desta crítica; ao aceitar um item, mova-o para o backlog.

## 1. Veredito

A arquitetura (snapshots → SCD2 → log de eventos → dimensional → gold) é defensável e a engenharia das três
SCD2 "simples" (empresas, estabelecimentos, dados_simples) está correta: idempotente, sem lacuna nem
sobreposição por construção, preserva o futuro no reprocessamento. A documentação é melhor que a média
(glossário, ADRs, backlog com evidência).

O problema está na **camada analítica**: os números que o TCC pretende mostrar (aberturas, baixas, saldo,
falências, sobrevivência) ainda não são confiáveis, e a causa não é um bug isolado — é que **a semântica
temporal e a de "empresa" mudam de arquivo para arquivo**. Os dez pontos que mais importam:

| # | Problema | Onde | Certeza |
|---|---|---|---|
| 1 | Sobrevivência: morte datada pelo mês em que o snapshot a viu (empresa baixada em 2005 "morre" em 2023-05) e censura em `CURRENT_DATE` em vez do último snapshot | C-01 | código |
| 2 | `dim_empresa` perde os meses em que `empresas`/`dados_simples` ainda não cobrem a matriz → empresa entra no estoque sem abertura (causa candidata do B21) | C-02 | código + medir |
| 3 | `teve_baixa_mes` não é transição: repete em todo mês com qualquer evento de empresa já baixada, e conta baixa de **filial** como baixa da empresa | C-03 | código + medir |
| 4 | Log mistura dois relógios: nascimento/entrada de sócio pela **data real**, todo o resto pelo **mês de detecção** | C-04 | código |
| 5 | `socios.sql` não é idempotente: reexecutar a janela cria versões inativas fantasmas e eventos `SOCIO RETORNOU` falsos | C-05 | código |
| 6 | `socios.sql`: janela sem dados na bronze fecha **todos** os vínculos ativos (o `ASSERT` não roda) | C-06 | código |
| 7 | Quatro categorias da OBT são inalcançáveis (`valor_posterior = 'BAIXADA'` nunca casa com `'BAIXADA - 01'`) | C-07 | código |
| 8 | DQ da silver_final/gold passa com o fato 100% errado: vários asserts são tautológicos e não há reconciliação | C-08 | código |
| 9 | Bronze descarta sócios sem documento/nome em silêncio; não há reconciliação landing→bronze | C-09 | código + medir |
| 10 | Docs se contradizem: specs dizem "nunca executado"/"quebrado" para o que o backlog registra como executado/corrigido | §2 | código |

Recomendação de ordem: **parar de adicionar tabelas na gold** e fixar primeiro três definições (§3) —
*relógio do evento*, *unidade de análise* (empresa × estabelecimento) e *o que é encerramento*. Quase todos
os B21–B24 e os C-01..C-07 são consequência de essas três coisas não estarem decididas.

## 2. Crítica da documentação

### 2.1 Contradições internas (o backlog anda, as specs ficam)

O backlog foi atualizado em 2026-11-08 com um run verde de 4 meses; as specs e a arquitetura não.

| Documento | Diz | Realidade |
|---|---|---|
| `CLAUDE.md` § Comandos | "SQL (silver/gold): **não há runner no repo**"; "pytest … suíte quebrada" | existe `src/jobs/run_sql.py`, `run_bronze.py`, `gerar_job.py` e o YAML de 53 tasks |
| `architecture.md:16-17` | "Não existe orquestração end-to-end no repo… SQLs rodados por definição externa" | idem |
| `architecture.md` § Estado de validação | landing "backfill em andamento"; silver "sem confirmação de execução em cluster" | backlog B18–B20: run verde 2026-11-08, com volumes e event log |
| `specs/silver_intermediate.md:3` | "**Nunca executado em cluster**" | executado |
| `specs/silver_final.md:3` | "não há evidência de execução" | executado (B19 nasceu do DQ rodando) |
| `specs/features/job-databricks.md`, `src/jobs/README.md` | "implementado, **não executado**" | executado |
| `specs/bronze.md:3,30-31` | CLIs de DQ quebradas (`executar_ingestao_dq_cli`) | B1 corrigido |
| `specs/bronze.md:14` | "mesma `pk` com `change_hash` diferente ⇒ `ValueError`" | `bronze_lib.py:166-175` **mantém** e só loga (B16) |
| `specs/bronze.md:19` | lista de tabelas sem `ceps` | `configs/ceps.py` existe (B10) |
| `specs/gold.md` | `ds_micro_comparativo_setorial` "**quebrado**" | B3 corrigido |
| `specs/landing.md:3,51-55` | seção "WIP (não commitado)" | árvore limpa; o WIP já entrou |
| `specs/silver_intermediate.md:10` | desempate `ORDER BY bronze_processed_at DESC` | código tem `, change_hash DESC`; como `bronze_processed_at` é constante por escrita, o desempate real é **sempre** o hash |
| `silver_intermediate/readme.md` | "não foram executados em cluster"; não cita a carga inicial (B9) | ambos desatualizados |
| `backlog.md:33` (B8) | evidência em `data_quality.py:375` | o arquivo tem 70 linhas |
| `backlog.md` dívidas | `tmp_receita/` fora do `.gitignore`; `Makefile` vazio; `_require_bucket` morto; "não há runner/Job definition" | `.gitignore:26` já tem; `Makefile` tem `docs-html`; `_require_bucket` é usado; runner existe |
| `backlog.md` B19 | "✔ corrigido" sem riscar | `gerar_docs_html.py:77-84` só conta riscado (`~~`) como fechado → painel mostra B19 aberto |
| `backlog.md` B12 | linha com coluna extra | quebra a tabela Markdown |
| `glossario.md` "Encerramento" | BAIXADA/INAPTA/NULA em `fato_empresa_mes` **e** na coorte | `fato_empresa_mes` usa só BAIXADA (INAPTA à parte); só a coorte usa as três |
| `glossario.md` "Coorte" | "sobrevivência com censura à direita" | a censura implementada está errada (C-01) |
| `glossario.md:27` | "mês = `novo.valid_since`" para as transições | não vale para `EMPRESA NASCEU` e `SOCIO ENTROU` (C-04) |
| `glossario.md:19` | "troca de faixa etária pode separar duas pessoas" | faixa etária não está na `pk`; ela cria **versão**, não pessoa nova |
| `decisions.md` D-03 | "`pk` é técnica (JSON)" | sócios usa concatenação com hífen (`configs/socios.py:4-19`) |

**Causa:** o "estado" de cada coisa está escrito em 6 lugares (CLAUDE.md, architecture, 5 specs, 2 READMEs,
backlog). **Sugestão:** estado de execução vive em **um** lugar (uma tabela "camada × implementado ×
executado em × janela" no `architecture.md`); specs descrevem comportamento, não status. E um teste barato
no CI: falhar se algum `.md` citar `arquivo.py:NNN` com `NNN` maior que o tamanho do arquivo.

### 2.2 Problemas estruturais

- **O backlog virou log.** Mistura bugs abertos, bugs fechados com narrativa, medições, dívidas, ideias e
  roadmap; B8 está fora de ordem; a linha "— medição" é uma célula de 60 palavras. Células de tabela com
  200+ palavras (B17, B20) são ilegíveis no diff. A ideia de "backlog como pasta" já está anotada — está na
  hora: fechados saem da tabela, B17/B20 viram spec de performance.
- **O SDD não está sendo seguido, e tudo bem — mas diga.** `specs/features/` tem 2 arquivos, ambos
  retroativos e resumidos; `carga-inicial` (B9), prometida em D-12, não existe; o cronograma pedia specs de
  revisão aprovadas até 2026-11-16. Ou escreva as specs de C-01..C-04 (são mudanças de regra de negócio,
  que D-12 diz exigir spec), ou rebaixe a regra no CLAUDE.md para o que é praticado.
- **Decisões que faltam em `decisions.md`** (são as que um avaliador vai perguntar):
  1. Qual é o relógio de um evento (data real × mês de detecção)? — C-04.
  2. Unidade de análise: empresa (CNPJ raiz) = matriz? Filial conta em quê? — C-03.
  3. O que é "encerramento" e o que é "falência" (por código de motivo) — B22 + §3.3.
  4. A que data se refere o snapshot `YYYY-MM` (data de extração da Receita ≠ fim do mês)?
  5. Dados de contato (telefone, e-mail, logradouro) de MEI em silver_final/gold — D-07 só fala de CPF.
- **`README.md` vazio** num repositório que vai para banca; `config/*.yaml` vazios; `tests/test_cdc_logic.py`
  e `test_dimensions.py` vazios. Apagar ou preencher.
- **`.claude/context_from_other_llm.md` (953 linhas)** continua versionado e é o maior documento do repo.
  Se "o que foi validado já está em `docs/`", apague — hoje ele é uma segunda fonte de verdade desatualizada.
- **`docs/tcc.md`** contém nome completo do autor e da orientadora num repo; ok se o repo for privado,
  rever antes de publicar.

### 2.3 O que falta para o TCC

- **Dicionário de dados** (pedido da orientadora) não existe; os `COMMENT` das DDLs já contêm 80% dele —
  gere a partir do `DESCRIBE`/das DDLs em vez de escrever à mão.
- **Nenhuma seção de limitações consolidada.** Estão espalhadas (B13, B16, B19, identidade de sócio, 2023-05
  baseline). O capítulo de limitações do texto vai precisar de uma lista única com *medida* de cada uma.
- **Cronograma × estado real.** Em 2026-11-14 os marcos de 11-16 (specs aprovadas, texto reescrito) e 11-23
  ("tudo consistente") competem com B21–B24 + C-01..C-07, que são bloqueantes para qualquer número de
  Resultados. Com entrega em janeiro, o Plano B (dashboard + análises, grafo/ML em trabalhos futuros) já
  parece o plano realista; vale decidir isso com a orientadora agora, não em dezembro.

## 3. Crítica metodológica (o que a banca vai atacar)

### 3.1 Dois relógios no mesmo log — C-04 **[código]**

`log_eventos_negocio.sql` data `EMPRESA NASCEU` (linha 44) e `SOCIO ENTROU NO QSA` (linha 215) pela **data
declarada** na Receita, e todos os outros eventos por `novo.valid_since` (mês em que o snapshot mudou).
`data_situacao_cadastral`, `data_exclusao_simples`, `data_opcao_mei` existem na silver e não são usados.

Consequências: (a) `aberturas − baixas` compara fluxos medidos em relógios diferentes; (b) uma empresa
aberta em 20/06 que só aparece no snapshot de julho tem evento em 2023-06 mas vigência a partir de 2023-07
— `fato_empresa_mes` não tem linha em 06 para ancorar a abertura (é o B13/B21 visto de outro ângulo);
(c) o último mês da janela sempre subconta aberturas.

**Proposta:** o log ganha duas colunas — `mes_deteccao` (hoje `mes_referencia`, chave do `REPLACE WHERE`) e
`data_evento` (data real quando a fonte dá). Cada consumidor declara qual usa. Para fluxo mensal coerente
com o estoque, use detecção; para tempo-até-evento, use a data real.

### 3.2 Sobrevivência — C-01 **[código]**

`d_coorte_sobrevivencia.sql`:
- linha 19: `mes_primeiro_encerramento = MIN(inicio_vigencia)` das versões encerradas. Toda empresa **já**
  baixada no baseline recebe `2023-05`; `tempo_sobrevivencia_meses` vira `2023-05 − nascimento`.
- linha 21: `COALESCE(fim_vigencia, CURRENT_DATE)` — toda empresa viva é censurada **hoje** (2026-11), não
  no último snapshot (2023-08 no teste). O resultado muda a cada dia.
- `ja_foi_mei`/`ja_foi_simples` (`MAX` sobre o futuro) são covariáveis com vazamento (viés de tempo imortal
  em Cox); `FIRST()` após `DISTRIBUTE BY … SORT BY` não garante ordem; `GREATEST(1, …)` esconde duração ≤ 0.
- INAPTA (reversível) conta como falha terminal.

**Ponto de desenho mais importante:** a Receita mantém as baixadas no arquivo com `data_situacao_cadastral`.
Logo a curva de sobrevivência **não precisa de SCD2** — um único snapshot dá `(data_inicio_atividade,
data_baixa, situação)` de todas as coortes desde os anos 1960, com datas reais. O que a série de snapshots
acrescenta é o que varia no tempo (porte, CNAE, sócios, Simples) como covariável. Dizer isso no texto é mais
honesto e mais forte do que derivar a morte do mês de detecção. Requer levar `data_situacao_cadastral` até
`dim_empresa` (hoje não chega).

### 3.3 Definições que mudam de arquivo para arquivo

- **"Empresa ativa/baixada"**: matriz no glossário; mas o evento de estado cadastral é por **estabelecimento**
  (inclusive filial) e é agregado por CNPJ raiz na gold (C-03).
- **"Encerramento"**: BAIXADA em `fato_empresa_mes`; BAIXADA/INAPTA/NULA na coorte.
- **"Falência"**: regex sobre a descrição do motivo (B22) — e o repo anterior tem uma lista por código que
  mistura falência, omissão e inaptidão sob o mesmo nome. Para um TCC, "falência" deveria ser só os motivos
  jurídicos de falência; o resto é "encerramento não voluntário" ou similar. Fixe a lista por **código** no
  glossário, com a tabela de motivos da RFB como fonte citável.
- **`crescimento_liquido = aberturas − baixas`** (`fato_indicadores_setoriais_mes.sql:31`) não fecha com
  Δestoque por construção: ignora ATIVA→INAPTA/SUSPENSA e reativações. Falta a identidade
  `estoque_final = estoque_inicial + entradas − saídas` como DQ — ela teria pego o B21 no primeiro run.

### 3.4 O snapshot não é "o mês"

`mes_referencia` é o nome da pasta da Receita; o conteúdo reflete a data de extração (dias antes da
publicação). "Situação em 2023-07" significa "situação na extração de julho". Isso desloca todo evento por
detecção em até ~1 mês e precisa estar no texto como limitação, com a data de extração de cada mês se ela
for recuperável (data de modificação dos ZIPs no WebDAV — vale guardar na landing).

### 3.5 Ausência ≠ sem mudança (C-10) **[código + medir]**

Nas três SCD2 simples, `valid_until = LEAD(mes_referencia)`: uma entidade que **some** do snapshot fica
`is_current = TRUE` para sempre. Só sócios modela ausência. Se a Receita nunca remove linhas, é inofensivo —
mas ninguém mediu. Um DQ resolve: `pks correntes na silver` − `pks do último snapshot bronze` = 0.

## 4. Achados de código

### 4.1 Gold / silver_final

**C-02 — `dim_empresa` trunca vigência** · alta · **[código + medir]**
`dim_empresa.sql:208-213` e `225-230`: `LEFT JOIN … ON sobreposição` seguido de
`WHERE GREATEST(inícios) < LEAST(fins)`. O `LEFT JOIN` só preserva a matriz quando **não há nenhuma**
sobreposição; com sobreposição parcial, o trecho descoberto é descartado.
Cenário: matriz `[2023-06, ∞)`; aparece em `dados_simples` só em 2023-08 → a fatia `[06, 08)` some. A
empresa entra no fato em 08 com `ativa_inicio=FALSE`, `ativa_fim=TRUE`, sem abertura: estoque sobe sem
abertura — o sintoma exato do B21. `fato_mudanca_estado` mascara o buraco com o fallback `primeira`
(linhas 102-109).
Correção: montar as fronteiras (união de todos os `valid_since`/`valid_until` por CNPJ) e juntar cada fonte
por contenção; ou grade mensal por CNPJ + colapso de ilhas. DQ: meses cobertos na dim = meses cobertos pela
matriz em `si.estabelecimentos`.

**C-03 — `teve_baixa_mes`/`teve_inaptidao_mes` não são transições** · alta · **[código + medir]**
`fato_empresa_mes.sql:68,70`: `situacao_cadastral_posterior = 'BAIXADA' OR categoria IN (…)` avaliado sobre
**qualquer** linha da OBT.
1. Empresa baixada em M com qualquer evento em M+1 (sócio saiu, saiu do MEI — comum logo após a baixa)
   recebe baixa de novo.
2. `ev_estado_cadastral` (`log_eventos_negocio.sql:54-75`) não filtra matriz: filial extinta → categoria de
   baixa/"falência" no CNPJ raiz com a matriz ATIVA.
3. `fato_indicadores_setoriais_mes.sql:20,33`: numerador não é subconjunto do denominador; a taxa pode
   passar de 1.000‰ ou existir com estoque zero.
Correção: flags derivadas **só** da transição da matriz (`ativa_inicio_mes AND situacao_fim = 'BAIXADA'`);
trazer `situacao_inicio_mes` para o fato; eventos de filial em coluna separada.

**C-07 — categorias mortas na OBT** · média · **[código]**
`obt_dashboard_eventos.sql:83-86` compara `sc.valor_posterior = 'BAIXADA'|'INAPTA'|'SUSPENSA'|'NULA'`, mas o
log grava `CONCAT_WS(' - ', situacao_desc, motivo)` (`log_eventos_negocio.sql:60`) → `'BAIXADA - 01'`.
`BAIXA VOLUNTARIA / OUTRAS`, `INAPTIDAO FISCAL`, `SUSPENSAO TEMPORARIA`, `INSCRICAO NULA / VICIOS` só saem
com motivo nulo; o resto cai no regex (B22) ou no `ELSE`, que devolve o **tipo do evento** como categoria.
Por isso o gráfico "Mudanças de situação cadastral por categoria" mistura `SOCIO ENTROU NO QSA`,
`MUDOU CNAE…` etc., e reativações não têm categoria. Resolver junto com B22: situação e código do motivo
como **colunas** do log/dimensão, classificação por código.

**C-11 — `dim_mudanca_estado` não é junk dimension** · média · **[código]**
A chave inclui `valor_anterior/posterior` (listas inteiras de CNAE secundário), `informacao_adicional` com
datas (`'Data informada pela Receita Federal: 20230512'`) e até `cnpj_basico` (`log:268`, evento de
qualificação de sócio) → milhões de linhas, uma por evento na prática; o fato junta por 5 colunas string
(`fato_mudanca_estado.sql:73-78`). Dimensão = `(entidade_tipo, evento, situação, motivo)`; o resto é
atributo degenerado do fato. Tirar o CNPJ de `informacao_adicional` (mistura chave de negócio com texto).

**C-12 — NULL→FALSE na gold, e o filtro de privacidade depende disso** · média · **[código + medir]**
`fato_empresa_mes.sql:89-90` e `obt_dashboard_eventos.sql:95-98` fazem `COALESCE(opcao_pelo_mei, FALSE)` —
contra a regra "NULL ≠ FALSE". `gerar_dashboard.py:94` lista razão social de quem tem
`NOT COALESCE(opcao_pelo_mei, FALSE) AND porte <> 'MEI'`: um MEI ainda sem linha em `dados_simples`
(o mesmo atraso do C-02) passa como "não MEI" e aparece com nome. Empresário individual não-MEI (natureza
2135) também tem nome de pessoa física na razão social. Filtrar por **natureza jurídica** e tratar NULL como
"não mostrar".

**C-13 — dashboard: a mesma métrica por dois caminhos** · média · **[código]**
"Aberturas" vem da OBT por evento (com 2023-05) num dataset e de `teve_abertura_mes` por empresa×mês (sem
2023-05) em outro, na mesma página. "Ativas" vem do fato setorial na página 1 e de `dashboard_mercado_ativo`
na página 2, sem DQ que as iguale. `b_saldo_uf` mostra saldo absoluto (só reproduz o tamanho da UF — usar
taxa). `mercado_empresas` não tem `ORDER BY`/`LIMIT` (milhões de linhas no warehouse). `SEM_CARGA_INICIAL`
usa `MIN(mes_referencia)` do fato: se um dia a carga começar em 2023-06, exclui um mês real.

**C-14 — vazamento para ML** · média · **[código]**
Em `fato_empresa_mes`, porte, UF, CNAE, situação e opções são estado de **fim** de mês: no mês da baixa a
`situacao_cadastral` já é `BAIXADA` (e o porte já mudou — é o B23). Só `ativa_inicio_mes` é defasada.
Para rótulo "baixa em t", as features têm de ser `*_inicio_mes` (t−1). IPCA e juro real entram
contemporâneos, só a Selic tem lag.

**C-15 — rótulos de "desconhecido" divergentes** · baixa · **[código]**
`'NAO INFORMADO'` (dim) × `'NAO IDENTIFICADO'` × `'NAO APLICAVEL'` (gold) para porte;
`'CODIGO NAO LOCALIZADO'` × `'OUTROS / NAO ESPECIFICADO'` para grande setor. Município vem de
`COALESCE(cidade_cep, domínio RFB, código)` (`dim_empresa.sql:260`) — fontes com grafias possivelmente
diferentes para o mesmo município **[medir]**. Sobre o **B24**: nenhum SQL de gold nem `dim_empresa` agrega
UF; `'PB,RN'` só pode vir pronto de `bronze.ceps.uf` — a ligação com B19 sugerida no backlog não se
sustenta. Validar `uf RLIKE '^[A-Z]{2}$'` na entrada do CEP.

**Outros (baixa):** `LAG` em `fato_empresa_mes.sql:102` pega a linha anterior, não o mês anterior (erra com
os buracos do C-02); os `params` com default "mês anterior" em SQLs cujo `REPLACE WHERE` exige literal são
código morto enganoso; comentário do seed macro diz "até 2026-06", o seed vai até 2025-12; IPCA 12m de 2022
é parcial; `ds_micro_top5_empresas.sql` não filtra MEI (sem uso hoje — apagar junto com
`vw_dashboard_eventos_cnae`, `ds_visao_macro`, `migracao_socios.sql`, `bronze/configs/cnaes.py`).

### 4.2 Silver intermediate

**C-05 — `socios.sql` não é idempotente** · alta · **[código]**
`socios.sql:153-178` (`estados_inativos_legado`): fecha em `mes_ini` todo vínculo corrente e ativo que não
esteja nos snapshots de `mes_ini` nem `mes_ini−1`. O comentário diz "na primeira execução", mas nada
restringe a isso nem a `valid_since < mes_ini`.
Cenário: sócio entra em 2023-07 → `[2023-07, ∞)` ativo. Reexecutar 2023-05..08 (retry, repair): ele não
está em 05 nem em 04 → nasce uma versão **inativa** `[2023-05, 2023-07)` e o log emite
`SOCIO RETORNOU AO QSA` em 2023-07. Vale para todo sócio que entrou depois de `mes_ini`.
O run de 08/11 teve repairs: **verificar se a tabela atual já está contaminada** (query Q5).
Correção: apagar o bloco (a tabela "legada" que ele corrigia não existe mais) e apagar `migracao_socios.sql`.

**C-06 — bronze vazia fecha o QSA inteiro** · alta · **[código]**
`socios.sql:59-78`: os `ASSERT_TRUE` estão num `SELECT … GROUP BY` sobre `bronze_raw`. Sem linhas na
janela, o `GROUP BY` devolve zero linhas, nenhum assert roda, `bronze_limpo` fica vazio e o bloco do C-05
emite **todos** os vínculos ativos como saída. O readme afirma o contrário ("a consulta falha em vez de
gerar inativações em massa"). Correção: validar em agregação sem `GROUP BY` (sempre uma linha). E o assert
só conta meses distintos — snapshot **parcial** passa; acrescentar "saídas no mês / ativos no mês anterior
< limiar".

**C-16 — eventos sem predecessora são perdidos** · média · **[código + medir]**
Todo evento de transição exige `antigo` e `novo`. Logo: (a) estabelecimento que aparece pela primeira vez
já BAIXADO (abriu e fechou entre snapshots) não gera evento de fim — some exatamente a vida mais curta, o
que enviesa sobrevivência; (b) `ENTROU NO MEI/SIMPLES` exige `FALSE → TRUE`; se o arquivo do Simples só
traz quem já optou alguma vez, a primeira opção aparece como linha nova `TRUE` e **nunca** vira evento
**[suspeita — depende do conteúdo do arquivo]**. Correção: primeira versão pós-baseline com estado
"terminal"/`TRUE` gera evento com `valor_antigo` NULL, ou usar as datas de opção/exclusão.

**C-17 — `EMPRESA NASCEU` por join de todas as versões** · média · **[código]**
`log_eventos_negocio.sql:36-52`: `estabelecimentos` (todas as versões da matriz) × `empresas` (todas as
versões) + `DISTINCT`. Se a Receita corrigir `data_inicio_atividade`, saem dois nascimentos; e o fan-out
versões×versões é pago antes do `DISTINCT`. Usar a primeira versão da matriz canônica e um `pk` por empresa.
O filtro `date_format(try_to_timestamp(…))` por linha pode ser `data_inicio_atividade BETWEEN '20230501'
AND '20230831'` sobre a string.

**C-18 — identidade de sócio por nome** · média · **[código + medir]**
`configs/socios.py:4-19`: para não-PJ a `pk` contém o nome. Correção de grafia na Receita = `pk` nova →
`SOCIO SAIU` da antiga, e **nenhum** `SOCIO ENTROU` da nova (a data de entrada é antiga, fora da janela):
viés líquido negativo no QSA. Medir saídas cujo `(cnpj_basico, cpf mascarado, data_entrada)` reaparece
ativo no mesmo mês com outra `pk`.

**C-19 — `change_hash` depende da ordem/conjunto de colunas da landing** · média · **[código]**
`bronze_lib.py:103-113, 219-224`: o hash cobre todas as colunas não técnicas, na ordem do Parquet;
`colunas_hash` não é usado nas 4 configs. Uma coluna nova ou reordenada na landing abre versão nova para
**todas** as entidades (sem evento, mas dobrando as SCD2). Fixar a lista explicitamente.

**Outros (baixa):** meses faltando no meio da janela só são validados em sócios — nas outras três, uma
mudança de julho com julho ausente é datada em agosto, em silêncio; `mes_ini > mes_fim` é no-op silencioso;
o `UPDATE` do MERGE só dispara se hash/vigência mudarem, então mudar uma regra de limpeza não corrige
versões antigas (em dev, reconstruir); `CONCAT_WS` devolve `''` e passa no DQ de `valor_novo` não nulo;
`socios` particionada por `is_current` é pior que não particionar (uma partição com ~99%, `UPDATE` move
linha entre partições).

### 4.3 Landing / bronze / jobs / CI

**C-09 — descartes silenciosos e nenhuma reconciliação** · alta · **[código + medir]**
`bronze_lib.py:145-148` filtra linhas com qualquer componente de PK vazio; `configs/socios.py:35-40` torna
`cnpj_cpf_socio` e `nome_socio_razao_social` obrigatórios. Sócio sem documento (provável em estrangeiros,
identificador 3) nunca entra na bronze — e o `coalesce(…, 'NA')` da `PK_SOCIOS_EXPR` nunca dispara, porque
o filtro vem antes: a intenção do código e o efeito se contradizem. Nada conta o que foi removido; a
volumetria só compara a bronze com ela mesma. Acrescentar ao DQ:
`linhas(landing) − linhas(bronze) − cópias_exatas − pk_inválidas = 0`, logado por mês.

**C-20 — DQs da bronze concorrem na mesma tabela** · média · **[suspeita]**
`data_quality.py:35,70-71`: 11 tasks paralelas fazem `CREATE IF NOT EXISTS` + `DELETE` + `INSERT` em
`dq_metrics_log` (não particionada). Receita para `ConcurrentAppendException` intermitente. O run de 08/11
passou, então pode ser raro; ainda assim, trocar por append-only (ler a última por `verificado_em`).
No mesmo arquivo: o `except Exception` das linhas 65-68 engole qualquer erro de leitura do histórico (B8);
a métrica é gravada **antes** das outras validações (um mês reprovado entra na média dos seguintes); para
estáticas `'estatica' < 'estatica'` é falso → a queda de volume nunca é verificada.

**C-21 — janela completa não cabe no Job atual** · média · **[código + aritmética]**
`gerar_job.py:77,190`: autoscale 1–4 `r5d.xlarge`, timeout de 4 h. Com 4 meses o run já soma horas (B17/B20);
~40 meses estouram o timeout depois de gastar o cluster. Rodar em blocos e resolver B17/B20 **antes** — é o
maior risco de custo do cronograma (meta de R$ 200/mês).

**C-22 — argumento digitado errado vira "mês anterior"** · média · **[código]**
`parse_known_args` em `run_sql.py:122`, `run_bronze.py:48`, `bronze_lib.py:307,318`,
`landing_lib.py:868,876`: `--mes_referencia_inicail 2023-05` é ignorado e o run fica verde processando outro
mês. Usar `parse_args()`.

**C-23 — a regra mais sensível (máscara MEI) não tem teste vivo** · média · **[código]**
`tests/test_extractor.py` testa `aplicar_mascara_mei`, que não existe mais; o CI nem coleta (B6). E mesmo
corrigindo o import, `pyproject.toml:37-40` tem `pythonpath = ["src"]`, incompatível com `from src.…`;
`[tool.poetry].packages` aponta para `cnpj_pipeline` (inexistente). Portar os 4 casos para
`limpar_chunk_empresas_mei` **sem tocar na regex**, `pythonpath = ["."]`. É o teste mais barato e mais
defensável do projeto.
A condição de aplicação é `porte = '01' AND natureza = '2135'` (`landing_lib.py:397`): não há evidência de
falha, mas vale medir uma vez se existe razão social com 11 dígitos fora dessa condição (Q8). Nota: porte
01 é **ME**, não MEI — a docstring confunde.

**C-24 — `gerar_job.py` não percebe SQL novo** · média · **[código]**
`gerar_job.py:29-64`: listas fixas. Um `.sql` novo em `ingest/` ou `tests/` simplesmente não entra no Job.
Checar cobertura: todo `*.sql` está nas listas ou numa lista de exclusão explícita. Também fixos no
gerador: branch `ceps_ibge_and_dashboard_v2` (o YAML fica errado após o merge) e o nome do bucket.

**Outros (baixa):** o token do WebDAV aparece nos logs de erro apesar da docstring dizer o contrário
(`landing_lib.py:238-244` — é público, mas a promessa é falsa); `.env.example:7` tem `#` colado ao valor (o
comentário vira parte do `GCP_PROJECT_ID`); `pyyaml` e `python-dateutil` usados e não declarados;
`pyproject` com e-mail de exemplo; a landing aceita mês publicado pela metade e o congela (checkpoint no S3,
`min_linhas` fixo de 2023) — exigir nº de ZIPs ≥ mês anterior; `_eh_consulta` (`run_sql.py:97`) deixaria de
executar um DQ que contivesse a palavra INSERT/MERGE (hoje nenhum contém — mais simples: sempre `.collect()`
em `tests/`); CI roda duas vezes em PR, não roda `ruff` nem `gerar_job --checar`, e o deploy é um `echo`.

**Passou na revisão:** injeção de SQL em `run_sql.py` (parâmetros validados por `fullmatch` de `YYYY-MM`);
idempotência da bronze mensal (`replaceWhere` por partição); `pk`/hash em JSON sem colisão por separador;
upload da landing só após validação; nenhum `print` com dado de linha; matriz canônica determinística;
`ponte_primaria` não multiplica linhas; colunas usadas pelo dashboard existem.

## 5. DQ — C-08 **[código]**

O padrão `ASSERT_TRUE` é bom. O conteúdo é fraco onde mais importa.

**Tautológicos (não podem falhar):**
- `dq_fato_mudanca_estado.sql:15-20` (`fks_nulas`): as FKs são `COALESCE(…, -1)`; os testes de órfão também
  não falham porque `-1` existe nas dimensões. **O DQ passa com 100% das FKs em −1.**
- `dq_dim_empresa.sql:6,7` (`ativos_com_fim`, `inativos_invalidos`) e `sk_grupo_cnae IS NULL`: impossíveis
  pela própria definição das colunas.
- `dq_ponte_cnae` (`flag_nula`), `dq_dim_grupo_cnae` (`qtd < 0`), `dq_dim_cnae` (`grande_setor IS NULL`),
  `dq_dim_mucanca_estado` (colunas com `COALESCE`), `dq_gold.sql:15,30` (idem).

**Ausentes — em ordem de valor:**
1. **Identidade de estoque** por mês: `estoque_final(M−1) = estoque_inicial(M)` e
   `estoque_final = estoque_inicial + entradas − saídas`. Pega B21, C-02, C-03.
2. **Reconciliação entre camadas:** landing = bronze (C-09); versão corrente da silver = hash do último
   snapshot bronze; eventos do log = linhas do fato, por mês e tipo; ativas no fato = `dashboard_mercado_ativo`.
3. **Taxa de fallback** com limiar: % de `sk_empresa_posterior = −1`, % de `cnpj_raiz IS NULL` (vira
   `'00000000'` na OBT sem ninguém medir).
4. **Cobertura temporal** da `dim_empresa` (C-02) e continuidade SCD2 (`valid_until` = próximo `valid_since`;
   exatamente uma versão corrente — hoje só testa "> 1", nunca "0"; `is_current` NULL passa).
5. **Sócios:** primeira versão de uma `pk` nunca é `vinculo_ativo = FALSE` (pega C-05); saídas/ativos < limiar.
6. **Regra do baseline:** zero eventos de transição em `valid_since = primeiro mês`.
7. **Domínio:** UF ∈ 27 (pega B24), categorias de evento ∈ lista fechada (pega C-07).

Um DQ que falha com contagem e amostra (como já foi feito no B18/B19) vale mais que dez de nulidade.

## 6. Performance e custo (além de B17/B20)

- **MERGE sem poda**: nenhuma SCD2 é clusterizada por `pk`; `ON pk AND valid_since` varre o alvo. Liquid
  clustering por `pk` (e remover a partição `is_current` de sócios).
- **Log lê cada SCD2 4–5 vezes** em self-joins (estabelecimentos 4×, dados_simples 4×, sócios 5×). Uma
  passada por tabela com `LAG` sobre `(pk, valid_since)` comparando várias colunas gera todos os eventos.
- **`fato_empresa_mes`** expande **todas** as versões, inclusive empresas baixadas há décadas (a maioria do
  cadastro), por join de intervalo: ~2 bilhões de linhas em 40 meses. Restringir a
  `ativa_inicio OR ativa_fim OR teve evento`.
- **`dim_empresa` re-fatia por telefone/e-mail/endereço**: o hash da SCD2 cobre tudo, então toda mudança de
  contato abre versão na dimensão analítica (56 M versões em 4 meses). Separar atributos analíticos de
  contato reduz a dimensão e resolve de quebra a questão de dado pessoal em silver_final.
- `fato_indicadores_setoriais_mes` é `CREATE OR REPLACE` total a cada run; `ds_coorte_sobrevivencia` é uma
  **view** com `GROUP BY` sobre a dimensão inteira, recalculada a cada consulta — materializar.
- **Antes de pagar a janela completa**: implementar a etapa exploratória local já prevista no backlog
  (recorte de um município em DuckDB). C-01, C-03, C-05, C-07 e C-16 são reproduzíveis em amostra, a custo zero.

## 7. Plano sugerido

1. **Decidir (1 reunião, sem código):** relógio do evento, unidade de análise, lista de motivos de
   encerramento/falência por código. Registrar em `decisions.md`.
2. **Medir (uma subida de cluster):** rodar o lote da seção 8 e anotar os números no backlog.
3. **Corrigir o que invalida resultado:** C-05/C-06 (sócios) → C-02 (dim_empresa) → C-03 + C-07 + B22 + B23
   (flags e categorias, de uma vez, por colunas estruturadas) → C-01 (coorte com data real).
4. **DQ de reconciliação** (seção 5, itens 1–3) antes de reexecutar — é o que diz se o passo 3 funcionou.
5. **Limpeza barata:** apagar código morto e arquivos vazios; teste da máscara MEI; `parse_args`;
   sincronizar docs (§2.1).
6. Só então B17/B20 e a janela completa.

## 8. Lote de medição (um notebook, uma subida de cluster)

```sql
-- Q1 (C-02) CNPJs cuja dim começa depois da matriz
SELECT COUNT(*) FROM (
  SELECT d.cnpj_raiz FROM (SELECT cnpj_raiz, MIN(inicio_vigencia) ini FROM tcc_receita.silver_final.dim_empresa WHERE sk_empresa <> -1 GROUP BY 1) d
  JOIN (SELECT cnpj_basico, MIN(valid_since) ini FROM tcc_receita.silver_intermediate.estabelecimentos WHERE identificador_matriz_filial = '1' GROUP BY 1) e
    ON e.cnpj_basico = d.cnpj_raiz AND d.ini > e.ini);

-- Q2 (C-03) baixas fora da população em risco, e baixas repetidas por empresa
SELECT mes_referencia, COUNT(*) total, SUM(CASE WHEN NOT ativa_inicio_mes THEN 1 ELSE 0 END) fora_do_risco
FROM tcc_receita.gold.fato_empresa_mes WHERE teve_baixa_mes GROUP BY 1 ORDER BY 1;
SELECT n_meses_com_baixa, COUNT(*) FROM (SELECT cnpj_raiz, COUNT(*) n_meses_com_baixa
  FROM tcc_receita.gold.fato_empresa_mes WHERE teve_baixa_mes GROUP BY 1) GROUP BY 1 ORDER BY 1;

-- Q3 (C-03) eventos de estado cadastral: matriz x filial
SELECT e.identificador_matriz_filial, COUNT(*) FROM tcc_receita.silver_intermediate.log_eventos_negocio l
JOIN (SELECT DISTINCT pk, identificador_matriz_filial FROM tcc_receita.silver_intermediate.estabelecimentos) e USING (pk)
WHERE l.evento = 'MUDOU ESTADO CADASTRAL' GROUP BY 1;

-- Q4 (C-07) distribuição real das categorias
SELECT categoria_macro_evento, COUNT(*) FROM tcc_receita.gold.obt_dashboard_eventos GROUP BY 1 ORDER BY 2 DESC;

-- Q5 (C-05) sócios cuja primeira versão é inativa (deve ser 0)
SELECT COUNT(*) FROM (SELECT pk, MIN_BY(vinculo_ativo, valid_since) primeira
  FROM tcc_receita.silver_intermediate.socios GROUP BY pk) WHERE primeira = FALSE;

-- Q6 (C-10) entidades correntes ausentes do último snapshot
SELECT COUNT(*) FROM tcc_receita.silver_intermediate.estabelecimentos s
WHERE s.is_current AND NOT EXISTS (SELECT 1 FROM tcc_receita.bronze.estabelecimentos b
  WHERE b.pk = s.pk AND b.mes_referencia = (SELECT MAX(mes_referencia) FROM tcc_receita.bronze.estabelecimentos));

-- Q7 (C-08) fallback no fato, por tipo
SELECT entidade_tipo, COUNT(*) total, SUM(CASE WHEN sk_empresa_posterior = -1 THEN 1 ELSE 0 END) sem_empresa,
       SUM(CASE WHEN cnpj_raiz IS NULL THEN 1 ELSE 0 END) sem_cnpj
FROM tcc_receita.silver_final.fato_mudanca_estado GROUP BY 1;

-- Q8 (C-23) razão social com 11 dígitos seguidos fora da condição da máscara (só a contagem; não listar)
SELECT natureza_juridica, porte_empresa, COUNT(*) FROM tcc_receita.bronze.empresas
WHERE mes_referencia = '2023-08' AND razao_social RLIKE '[0-9]{11}'
  AND NOT (porte_empresa = '01' AND natureza_juridica = '2135') GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20;

-- Q9 (C-16) primeira aparição já encerrada, depois do baseline
SELECT situacao_cadastral_desc, COUNT(*) FROM (SELECT pk, MIN(valid_since) ini, MIN_BY(situacao_cadastral_desc, valid_since) situacao_cadastral_desc
  FROM tcc_receita.silver_intermediate.estabelecimentos GROUP BY pk) WHERE ini > '2023-05' GROUP BY 1;
```

Para a landing (local, sem cluster): contar sócios com `cnpj_cpf_socio = ''` por `identificador_socio` (C-09).

## 9. O que esta revisão não cobriu

- Nenhuma execução: magnitudes marcadas **[medir]** são hipóteses até a seção 8 rodar.
- `tcc_receita.lvdash.json` (só as queries dos datasets e os primeiros widgets), notebooks,
  `scripts/docs_html_template.html`, `poetry.lock`, `.claude/context_from_other_llm.md`.
- O repositório do texto do TCC (`/home/Junj/Documents/TCC`) não foi aberto; a seção 2.3 se apoia em `docs/tcc.md`.
- O YAML do Job foi conferido contra o gerador por leitura, não por `gerar_job --checar`.
