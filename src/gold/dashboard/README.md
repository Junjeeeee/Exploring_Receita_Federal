# Dashboard AI/BI — `tcc_receita.lvdash.json`

**Gerado** por `python -m src.gold.dashboard.gerar_dashboard` (não editar o JSON à mão: mudanças feitas na UI
se perdem na próxima geração; se gostar de um ajuste feito na UI, traga-o para o gerador).
Status: **preparado, não testado** (formato do JSON escrito sem export de referência; ver spec).

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

## Páginas
| Página | Datasets | Conteúdo |
|---|---|---|
| Visão geral | `painel`, `estoque`, `eventos`, `taxas_setor`, `top_cnaes`, `macro` | ativas, aberturas×baixas, saldo por UF, taxa de fechamento por grande área, eventos por tipo/categoria, top CNAEs, Selic/IPCA |
| Mercado ativo | `mercado`, `mercado_empresas` | concorrência local por UF/município/CNAE; lista só não-MEI, sem endereço |

Filtros de página são filtros de campo (aplicam a todos os datasets com a coluna). `taxas_setor`, `top_cnaes`
e `macro` são Brasil e não reagem aos filtros.
