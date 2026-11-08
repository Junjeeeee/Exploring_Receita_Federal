# Repo anterior: `mineracao_receita` (contexto)

> `/home/Junj/Documents/Repos/mineracao_receita` — trabalho da disciplina de Mineração de Dados (último commit
> "entrega final mineração", 2026-07-10). Resumo gerado por exploração somente leitura em 2026-11-08.
> **Não integrar antes da 1ª entrega (dashboard).** Serve de referência para grafo/ML e para definições.

## O que é
Lakehouse medalhão **local** (PySpark 3.5 + Delta + GraphFrames + DuckDB) restrito a **Niterói** (município RFB 5865),
com grafo em tabelas Delta (nós/arestas por snapshot `safra_dados`) e um experimento de **predição de "falência"
em 2 anos** com CatBoost. Sem README; especificação das features em `definicao_grupos.md`.

| Camada | Arquivo | Conteúdo |
|---|---|---|
| extração | `src/minerador_cnpj/extrator.py` | WebDAV RFB, filtro Niterói (Empresas, Estabelecimentos, Socios, Simples) |
| bronze | `src/transformation/bronze/generate_bronze_tables.py` | `node_id = origin_id_tipo`, `hash_id = xxhash64(node_id)` |
| silver_src | `src/transformation/silver_src/tables/*.sql` | um SQL por nó/aresta, `replaceWhere safra_dados` |
| silver_stg | `src/transformation/silver_stg/build_graph_stg.py` | união das arestas + grau (in+out) |
| gold | `src/transformation/gold/dataset_v0.py`, `dataset_final.py` | ABT por safra + rótulo futuro |
| análise | `src/analise_exploratoria/` | ML (`explore_dataset.ipynb`), ego-graphs pyvis, hubs, investigações |

**Grafo** — nós: `pessoa_juridica` (cnpj_basico), `estabelecimento`, `pessoa_fisica`, `telefone`, `email`, `cep`.
Arestas: `socio_de` (PF/PJ→PJ), `tem_filial`, `tem_telefone`, `tem_email`, `tem_cep`. Sem SCD2/eventos: cada
mês é um snapshot completo. Algoritmos: grau, power-law, hubs, ego-graph com **poda de super-nós**. Sem comunidades
nem componentes; "grupos" são vizinhanças de k saltos por joins. Não há visualização temporal do grafo.

**ML** — features em 2024-01, rótulo em 2026-01; 33 920 empresas, prevalência 9,9 %.
CatBoost (`Balanced`, CV 5 folds): **AUC 0,863, PR-AUC 0,420** (baseline 0,099), recall 0,78, precisão 0,28.
Importância dominada por atributos cadastrais (Simples, idade, saída do Simples, MEI, CNAE, capital);
features de grafo (G1/G2/G3) com importância baixa; G3 considerado insatisfatório pelo autor.

## Definições usadas lá (comparar com `docs/glossario.md`)
- **"Falência"** = `motivo_situacao_cadastral` **da matriz** ∈ {01,05,06,10,13,14,15,61,63,64,66}
  (falência, liquidação, omissão, inexistência de fato, fraude, inaptidão). Excluídos: 02,03,04,07 (M&A /
  reestruturação) e 71,73,74 (atraso administrativo → vazamento).
- **Viva** = motivo da matriz fora da lista acima. **Ativa** = situação 02.
- **Grupos** (`definicao_grupos.md`): G2 = sócios diretos + contatos; G1 = empresa→sócio→empresa→sócio;
  G3 = G1 + ligações por contato com grau ≤ 10 (spec diz 20).
- **Sócio PF** identificado por CPF mascarado (ou `cnpj_basico || nome`) → colisões possíveis.
- **MEI→PF**: arestas inferidas por nome limpo único (CPF removido da razão social por regex), filtro anti-homônimo.

## O que pode enriquecer este repo
1. **Dashboard (agora, barato):** usar a lista de motivos acima como categoria analítica de "encerramento
   negativo" vs. baixa voluntária/reestruturação nos eventos de término — melhora a leitura do dashboard e a
   análise de sobrevivência. Verificar como `log_eventos_negocio` classifica término hoje.
2. **Faixa etária do sócio administrador** (qualificações 05, 16, 49): já calculada lá, pede só `faixa_etaria`
   de socios — candidata ao dashboard (pedido "se for barato").
3. **Futuro (grafo/ML):** os eventos SCD2 daqui resolvem duas limitações de lá: momento real das mudanças e
   empresas que somem da base (lá descartadas com `dropna`, viés de sobrevivência). Lições: rótulo na mesma
   safra vaza (`g2_qtd_filiais_falidas`); usar split temporal e calibrar limiar; poda de super-nós de contato.
4. **Divergências a decidir quando chegar lá:** chave do sócio PF (CPF mascarado colide); limite de grau 10×20;
   escopo Niterói × Brasil; LGPD (lá o nome do sócio PF fica no nó — aqui a regra é não expor).
   Ideia do autor: pseudonimizar o nome **palavra a palavra** (sobrenome compartilhado pode indicar parentesco).
   Atenção: hash simples de nome é revertível por dicionário (nomes têm pouca entropia) → usar HMAC com chave
   secreta fora do repo; continua sendo dado pessoal pseudonimizado (LGPD), não anonimizado. Sobrenomes comuns
   (Silva, Santos…) geram falso parentesco → ponderar por frequência.
