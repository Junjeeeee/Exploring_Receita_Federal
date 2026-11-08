# Feature: Dashboard v0 (reunião de 2026-11-09)

Status: **preparado, não testado** · 2026-11-08 · modo protótipo (D-12), spec retroativa resumida.

## Requisito
Dashboard no Databricks AI/BI para mostrar a dinâmica empresarial (aberturas, baixas, eventos, mercado ativo)
sobre a janela de teste (2023-05..2023-08), sem refazer transformações e sem expor dados pessoais.

## Design
- `src/gold/dashboard/gerar_dashboard.py` gera `tcc_receita.lvdash.json` (datasets SQL + widgets + layout).
- Fontes: `fato_indicadores_setoriais_mes`, `obt_dashboard_eventos`, `fato_indicadores_macro_mes`,
  `dashboard_mercado_ativo`. Datasets já agregados (poucas linhas → warehouse pequeno basta).
- 2023-05 (carga inicial) excluído das séries de fluxo.
- LGPD: lista de empresas só não-MEI e sem logradouro; nenhuma informação de sócio.
- `ds_micro_*` (parâmetros `:param_*`) **não** usados: substituídos por filtros de campo na página "Mercado ativo".

## Tarefas
- [x] Gerador + JSON + README
- [ ] Importar no workspace e corrigir widgets que o formato não aceitar
- [ ] Decidir destino de `ds_micro_*` e `vw_dashboard_eventos_cnae` (provável remoção)
- [ ] Taxas que reajam aos filtros (hoje `taxas_setor` é Brasil)

## Critérios de aceite
- Import sem erro; todos os widgets renderizam após o Job completo.
- Nenhum nome de sócio, CPF, nome de MEI ou endereço visível.

## Impacto no TCC
Prints para o capítulo de Resultados; descrição dos indicadores (taxa por 1.000 ativas) cabe na Metodologia.
