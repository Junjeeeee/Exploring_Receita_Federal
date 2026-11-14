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
| 1. Resumo | O estoque está crescendo? | ativas, aberturas, baixas, saldo do último mês + Δ vs. mês anterior; aberturas por baixa | estoque mês a mês; entradas × saídas | UF, porte, área |
| 2. Quem | Quem abre e quem fecha? | % MEI nas aberturas e nas baixas; setor que mais/menos cresce | composição por porte; saldo e fechamento por 1.000 ativas por área; top 20 CNAEs | UF, área (só contadores e porte) |
| 3. Onde | Onde cresce mais? | UF com maior/menor saldo por 1.000 ativas | ranking de UF por taxa; tabela de taxas | — (Brasil) |
| 4. Como fecha | Por que e com que idade fecham? | % falência; idade mediana na baixa; % < 2 anos; reativações | baixas por categoria de motivo; por faixa de idade; baixas/inaptidões/suspensões/reativações por mês; eventos por tipo | UF, porte, área |
| 5. Mercado ativo | Quem são os concorrentes locais? | ativas | porte, bairro, área; 1.000 maiores empresas (LGPD) | UF, município, CNAE |
| 6. Sobre os dados | Como ler? | — | notas fixas; Selic/IPCA (contexto) | — |

Como os números respeitam os filtros (filtro de campo = `WHERE` nas linhas do dataset):
- último mês e Δ são colunas por linha em `resumo_mes` (Δ = valor(M) − valor(M−1), linear); o widget só soma;
- percentuais = `AVG` de indicador 0/100 em `fluxos_empresa` (uma linha por empresa com abertura ou baixa);
- taxas por 1.000 ativas são razões de somas: vêm prontas (`taxas_setor`, `taxas_uf`, `extremos_*`) e são Brasil.

Widgets de maior risco de formato (sintaxe não vista em JSON exportado pelo workspace):
`c1_ab_por_baixa` e `c1_ab_por_baixa_ant` (`try_divide(SUM(..), SUM(..))`). `c4_idade` usa `PERCENTILE(col, 0.5)`,
forma vista num dashboard exportado pela Databricks. Se um deles quebrar, reconfigurar pela UI e trazer o JSON
regravado para o gerador.

## Regras de leitura
- Fluxos por **mês de detecção** no snapshot (extração ~dia 10); empresa = matriz do CNPJ raiz; 2023-05 é baseline
  (sem fluxo); saldo = entradas − saídas do estoque de ativas (inclui reativações, inaptidões, suspensões).
- Aberturas vêm só de `fato_empresa_mes` / `fato_indicadores_setoriais_mes`; a OBT não conta abertura.
- Idade na baixa = data real da baixa − início de atividade (`fato_empresa_mes.idade_na_baixa_meses`).
- Limitação: `LIMIT` em dataset (`mercado_empresas`, 1.000 maiores do Brasil) é aplicado antes dos filtros de página.
