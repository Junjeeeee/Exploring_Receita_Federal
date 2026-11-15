# Dashboard AI/BI — `tcc_receita.lvdash.json`

**Gerado** por `python -m src.gold.dashboard.gerar_dashboard` (não editar o JSON à mão: mudanças feitas na UI
se perdem na próxima geração; se gostar de um ajuste feito na UI, traga-o para o gerador).
Status: **v2 (6 páginas) validada na amostra de 1%** — todos os datasets executam e a prévia foi atualizada;
a renderização dos widgets **não foi conferida visualmente** (ver lista de risco abaixo). Ao gerar, o script valida
referências (datasets, colunas, filtros, grade), texto de abertura em toda página e agregação nos contadores.
Spec: `docs/specs/features/2026-11-14-dashboard-v2-narrativa.md`.
Contrato de colunas: `docs/specs/features/2026-11-14-semantica-eventos-e-fluxos.md` (§ 2.4).

## Como importar
1. Rodar o Job até as tasks `gold_*` ficarem verdes.
2. *Dashboards → Create → ⋮ / seta → Import dashboard from file* → escolher `tcc_receita.lvdash.json`.
3. Selecionar o SQL warehouse no topo do editor e clicar em *Refresh*.
4. Widget com erro de formato: abrir o widget e reconfigurar pela UI (as consultas dos datasets continuam valendo).

## Custo — dashboard AI/BI exige SQL warehouse
Não roda em job cluster. Opção mais barata para a reunião:
- **Serverless 2X-Small**, *auto stop* = 5 min (ou o menor permitido), *scaling* 1–1.
- Ligar ~10 min antes da reunião, dar *Refresh* e deixar parar sozinho depois. Custo esperado: poucos dólares.
- Alternativa sem serverless: *Pro* 2X-Small (sobe mais devagar, ~5 min; auto stop mínimo 10 min).

## Páginas (uma pergunta por página)
| Página | Pergunta | Big numbers | Gráficos | Filtros |
|---|---|---|---|---|
| 1. Resumo | O número de empresas ativas está crescendo? | ativas, aberturas, baixas, crescimento líquido do último mês + variação em relação ao mês anterior; aberturas por baixa | ativas mês a mês; passaram a estar ativas × deixaram de estar | UF, porte, área |
| 2. Quem | Quem abre e quem fecha? | % MEI nas aberturas e nas baixas; setor que mais/menos cresce | composição por porte; crescimento líquido e baixas por 1.000 ativas por setor; top 20 CNAEs | UF, área (só contadores e porte) |
| 3. Onde | Onde cresce mais? | UF que mais/menos cresce (crescimento líquido por 1.000 ativas) | ranking de UF por taxa; tabela de taxas | — (Brasil) |
| 4. Como fecha | Por que e com que idade fecham? | % falência; idade mediana na baixa; % < 2 anos; reativações | baixas por categoria de motivo; por faixa de idade; baixas/inaptidões/suspensões/reativações por mês; eventos por tipo | UF, porte, área |
| 5. Mercado ativo | Quem são os concorrentes locais? | ativas | porte, 20 maiores bairros do município, setor; 1.000 maiores empresas (LGPD) | UF, município, bairro, CNAE (todos os widgets) |
| 6. Sobre os dados | Como ler? | — | notas fixas; Selic/IPCA (contexto) | — |

Como os números respeitam os filtros (filtro de campo = `WHERE` nas linhas do dataset):
- último mês e Δ são colunas por linha em `resumo_mes` (Δ = valor(M) − valor(M−1), linear); o widget só soma;
- percentuais = `AVG` de indicador 0/100 em `fluxos_empresa` (uma linha por empresa com abertura ou baixa);
- taxas por 1.000 ativas são razões de somas: vêm prontas (`taxas_setor`, `taxas_uf`, `extremos_*`) e são Brasil.

Texto exibido sem jargão: `validar()` recusa "estoque", "saldo", "fluxo", "snapshot", "baseline", "detecção" e "Δ"
em títulos, rótulos, descrições e textos (nomes de coluna não contam). "Crescimento líquido" é definido onde aparece.

Ordem das barras: rankings de uma série ordenados por valor (maior no topo nas horizontais, à esquerda nas
verticais); séries de cor por porte em ordem fixa `ORDEM_PORTE` (MEI > ME > DEMAIS > EPP > não informado), a maior
na base da pilha; faixas de idade em ordem natural (`ORDEM_FAIXA_IDADE`). Sintaxe `custom-order` copiada dos
dashboards de exemplo da Databricks (dbdemos).

Mercado ativo: os três datasets usam o mesmo bairro normalizado (`BAIRRO`: nulo/vazio → `NAO INFORMADO`);
`mercado_bairros` tem grão bairro × CNAE (agregado) restrito aos 20 maiores bairros de cada município, então os
quatro filtros valem para todos os widgets.

Widgets de maior risco de formato (sintaxe não vista em JSON exportado pelo workspace):
`c1_ab_por_baixa` e `c1_ab_por_baixa_ant` (`try_divide(SUM(..), SUM(..))`). `c4_idade` usa `PERCENTILE(col, 0.5)`; a ordem das séries de cor de `b2_porte` (`custom-order` no encoding de cor) e qual ponta vai na base da pilha,
forma vista num dashboard exportado pela Databricks. Se um deles quebrar, reconfigurar pela UI e trazer o JSON
regravado para o gerador.

## Regras de leitura
- Fluxos por **mês de detecção** no snapshot (extração ~dia 10); empresa = matriz do CNPJ raiz; 2023-05 é baseline
  (sem fluxo); crescimento líquido = passaram a estar ativas − deixaram de estar ativas (inclui reativações,
  inaptidões, suspensões).
- Aberturas vêm só de `fato_empresa_mes` / `fato_indicadores_setoriais_mes`; a OBT não conta abertura.
- Idade na baixa = data real da baixa − início de atividade (`fato_empresa_mes.idade_na_baixa_meses`).
- Limitação: `LIMIT` em dataset (`mercado_empresas`, 1.000 maiores do Brasil) é aplicado antes dos filtros de página.
