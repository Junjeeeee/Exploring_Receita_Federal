# TCC — documento vivo (texto da monografia)

> Repositório separado: **`/home/Junj/Documents/TCC`** (git próprio, LaTeX abnTeX2, modelo DCC/UFRJ,
> Bacharelado em Ciência da Computação). Estado em 2026-11-08.
>
> **O texto é escrito exclusivamente pelo autor** (regra de autoria da UFRJ). Este arquivo só aponta o que está
> defasado e recomenda mudanças; o agente nunca edita o repositório do TCC.

## Escopo e prazos (decidido em 2026-11-08)

- **Entrega do texto final:** fim de janeiro de 2027. Reuniões semanais com a orientadora: segundas, 13h.
- **Entregas intermediárias:** a planejar (ver seção *Cronograma sugerido*).
- **Prioridade:** (1) pipeline completo + **dashboard** + análises (sobrevivência/painel, ainda não citadas no
  texto, enriquecem esta entrega) → entrega garantida; (2) só então grafo (protótipo em
  `/home/Junj/Documents/Repos/mineracao_receita`) e ML/testes de hipótese. Hipóteses dos slides são exemplos,
  não finais.
- **Execução:** teste com 2023-05..2023-08 (landing já no S3); validado, rodar a janela completa.
- **Atualização diária** não faz sentido (fonte é mensal) → texto deve falar em carga mensal; um bot que detecte
  nova publicação da RFB fica como trabalho futuro.
- **Ética/LGPD:** dados públicos; CPF já tratado na landing. Dashboard não expõe nomes de sócios nem endereço de
  MEI (questão ética, não legal). Discussão mais forte só se houver visualizador de grafo público.

## Uso de IA (para a declaração exigida pela UFRJ)

- Uso de IA é permitido desde que o autor assuma responsabilidade e entenda o código; exige **declaração de uso**.
- O autor deve revisar todo o código/documentação gerados antes da entrega final.
- Registro sugerido para a declaração: assistentes de código (Claude Code e outros LLMs) usados para
  revisão/correção de bugs, refatoração, documentação (`docs/`) e apoio ao modelo dimensional; o texto da
  monografia é de autoria própria.

## Cronograma sugerido (a validar na reunião de segunda)

| Até | Marco |
|---|---|
| 2026-11-16 | Specs de revisão do código (bronze/silver/gold) e do Job Databricks aprovadas |
| 2026-11-30 | Correções aplicadas; 1ª execução bronze→silver→gold (2023-05..08) num job cluster spot; DQ verde |
| 2026-12-14 | Dashboard v1 sobre os 4 meses; backfill da janela completa iniciado |
| 2026-12-31 | Janela completa processada; dashboard final; análises (sobrevivência/painel) |
| 2027-01-15 | Cap. 3 e Resultados escritos pelo autor; revisão de todo o código gerado com IA |
| fim jan/2027 | Texto final (+ grafo/ML só se sobrar tempo, senão "trabalhos futuros") |

## Build
`pdflatex main && bibtex main && makeglossaries main && pdflatex main && pdflatex main` (inferido; não há `.latexmkrc`).
Bib: `elementos-postextuais/referencias.bib` (BibTeX, `abntex2cite [alf]`).
⚠ `capitulo-2.tex:26,49` usa caminho **absoluto** em `\includegraphics` → trocar por `figuras/...`.

## Estrutura e status

| Arquivo | Conteúdo | Status |
|---|---|---|
| `elementos-pretextuais/personalizar.tex` | título, autor, orientador | **template** (nomes fictícios) |
| `resumo.tex`, `abstract.tex` | | **template** (palavras-chave de exemplo) |
| `abreviaturas.tex`, `simbolos.tex`, `postextuais/glossario.tex` | | **template** (faltam CNPJ, RFB, SCD, CDC, CNAE, MEI, QSA...) |
| `capitulo-1.tex` Introdução | contexto, proposta lakehouse+medalhão, 4 casos de uso | rascunho; **sem pergunta de pesquisa, objetivos, justificativa, estrutura** |
| `capitulo-2.tex` Fundamentação | S3, Delta Lake, Lakehouse, Medalhão, Grafos (stub), relacionados (Rolim 2024, Paulo 2022, Rede-cnpj, Minha Receita, Sinarc) | parcial |
| `capitulo-3.tex` Metodologia | CNPJ, ETL, bronze, prata inicial (SCD2+log, CDC), prata final, ouro, dashboard, grafos, falência; seção solta "Não sei aonde por" | rascunho no futuro ("teremos") |
| `capitulo-4.tex` | equações/códigos/glossário | **template puro** → virar Resultados |
| `capitulo-5.tex` Conclusão | | **template** |
| `apendice-1.tex` | longtable `tab:regras_silver` (regras de higienização) | conteúdo real, título de template |
| `anexo-1.tex` | dicionário de dados RFB (`figuras/dicionario_rfb.pdf`) | real |
| `apendice-2.tex`, `anexo-2.tex` | lorem ipsum | remover |
| `Old/`, `codigos/`, `figuras/grafico-tcc.png`, `figuras/ModeloMonografia...img002.png` | sobras | removíveis |

## Apresentação inicial (fonte: `~/Downloads/Apresentação TCC.pdf`, 34 slides, desatualizada)
- **Autor:** João David Jotta Mendonça Estorque · **Orientadora:** Vivian dos Santos Silva.
- **Título provisório:** *The Application of Business Intelligence, Graph Visualizations and Machine Learning
  on top of the Brazilian National Register of Legal Entities with a Data Lakehouse Architecture*.
- **Budget:** S3 + Databricks custeados pelo autor, meta ≤ **R$200/mês** (cuidado com tempo de cluster ligado).
- **Plano original de camadas:** bronze append-only particionada por mês (PySpark) → *silver source* (CDC entre as
  duas últimas partições, SCD2 + log, SQL) → *silver staging* (**nós e arestas** do grafo, fato p/ dashboard,
  resultado de BFS de grupos econômicos) → gold BI (OBT por alteração cadastral) + gold ML (dataset com rótulo
  "faliu em 6m/1a/2a").
  → No repo atual a *silver staging* virou **silver_final dimensional** (sem grafo).
- **Hipóteses de falência propostas:** (1) compartilhar telefone/e-mail com empresa que faliu ↑ falência;
  (2) telefone/e-mail compartilhado com >10 empresas ↑ falência (fraude?); (3) falência de outra empresa de um
  sócio ↑ falência; (4) sócio administrador PJ ↑ falência; (5) sócio administrador com outras empresas ↑ sucesso do MEI.
- **Visualização de grafo:** protótipo já existiu (slides de evolução temporal 2023-05 → 2026-05, provavelmente
  do trabalho de Mineração de Dados) — não está neste repo.
- **Prioridade declarada:** dashboard → visualização de grafos → ML/testes de hipótese ("atingir um ponto de fim já garante um TCC").
- **Artigos possíveis:** medalhão multipropósito (vs Rolim 2024); hipótese de falência comprovada; dashboard público.

## Tese implícita
Uma arquitetura **lakehouse medalhão** com **SCD2 + log de eventos** sobre os snapshots mensais do CNPJ
habilita múltiplos casos de uso: (a) dashboard de criação/término/alteração cadastral; (b) grafo temporal
com BFS por PJ; (c) algoritmos de grafo para agrupar empresas; (d) predição de falência / testes de hipótese.
O fonte comenta que (c) e (d) só entram "caso as primeiras sejam concluídas".

## Texto × código (código é a verdade)

| Texto diz | Repo | Status |
|---|---|---|
| S3 + Delta + medalhão | igual (Databricks UC `tcc_receita.*`) | OK |
| Databricks / Spark | nunca citados no texto | **não descrito** |
| Só dados CNPJ | também IBGE CNAE, CEPs (BigQuery), Selic/IPCA (seed) | **não descrito** |
| Carga incremental diária | orquestrador mensal via CLI; sem agendamento (diária descartada: fonte é mensal) | divergente → corrigir texto |
| Bronze com 9 tabelas, 3 metadados (mês, ts insert, ts update) | 12 tabelas; `pk`, `change_hash` SHA-256, `mes_referencia`, `bronze_processed_at` | divergente |
| Hash/PK criados na silver por concatenação | criados na **bronze**, SHA-256 sobre JSON | divergente |
| SCD2 + log, CDC por FULL OUTER JOIN | igual em conceito | OK; faltam intervalo meio-aberto, QSA por ausência, NULL≠FALSE |
| Prata final = nó empresa + views/grafo materializado + BFS | **modelo dimensional** (dims, ponte, fato), sem grafo | divergente / não implementado |
| Ouro = fato dashboard + features ML | OBT eventos, fato_empresa_mes, macro/setoriais, mercado ativo, `ds_*`, coortes | dashboard ✔ (parcial); ML ✘ |
| Grafo openCypher, BFS profundidade 3, features de grupo | inexistente | não implementado |
| Falência: features 2024-01, rótulos 2024-06/2025-01/2026-01 | fora do repo (experimento CatBoost); backfill só 2023-05..08 | não implementado |
| — | sobrevivência (KM/Cox), painel econométrico, DQ `ASSERT_TRUE`, SK/sentinelas, LGPD, categorias de evento | **não descrito no texto** |

## Bibliografia
13 entradas. `angles2018` é **citada mas ausente** no `.bib`; `wang2024datasets` está no `.bib` e nunca é citada.
Faltam: Kimball/SCD2, CDC, Kaplan-Meier/Cox, predição de falência (Altman etc.), grafos/openCypher, LGPD, Spark.

## Recomendações para o texto (prioridade)
1. **Escopo** (alinhar com a orientadora): dashboard + eventos (+ sobrevivência/painel) como núcleo; grafo e ML
   condicionados ("se concluído") ou em trabalhos futuros. Título provisório cita grafo e ML — avaliar.
2. Metadados, resumo/abstract, abreviaturas, glossário reais.
3. Cap. 1: problema, pergunta de pesquisa, objetivos geral/específicos, justificativa, contribuições, estrutura.
4. Cap. 3: reescrever no passado/impessoal, alinhar com `docs/` (stack, 12 tabelas, metadados, dimensional, eventos, D-04..D-09, LGPD, DQ); diagramas; absorver "Não sei aonde por".
5. Criar capítulo de **Resultados** (substitui cap. 4): volumes por camada, DQ, cobertura temporal, prints do dashboard, sobrevivência. **Depende** de concluir backfill e rodar silver/gold (ver `backlog.md`).
6. Cap. 5: conclusão, limitações, trabalhos futuros.
7. Cap. 2: completar grafos + `angles2018`, Sinarc; adicionar SCD2/dimensional, CDC, sobrevivência; corrigir legenda duplicada da Fig. 2.
8. Limpeza: lorem ipsum, `Old/`, `codigos/`, imagens não usadas; revisão ortográfica ("sofware", "descrevirei", "sessão"→"seção", "trás"→"traz").

## Como trabalhar no TCC
- Ao mudar o pipeline, verificar se o capítulo 3 (ou a tabela de divergências acima) precisa de atualização.
- Ao escrever texto, usar `docs/glossario.md` e `docs/decisions.md` como fonte das definições.
- Spec de feature (`docs/specs/_template.md`) pode ter seção "Impacto no TCC".
