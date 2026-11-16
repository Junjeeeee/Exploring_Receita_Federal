# Feature: Dashboard v0 (reunião de 2026-11-09)

Status: **substituído pela v2** — [`2026-11-14-dashboard-v2-narrativa.md`](2026-11-14-dashboard-v2-narrativa.md) (layout em 6 páginas). Datasets, LGPD e pendências abaixo continuam valendo onde a v2 não diz o contrário.
Status anterior: **preparado, não testado** (atualizado em 2026-11-14 para a semântica de eventos) · 2026-11-08 · modo protótipo (D-12), spec retroativa resumida.

## Requisito
Dashboard no Databricks AI/BI para mostrar a dinâmica empresarial (aberturas, baixas, eventos, mercado ativo)
sobre a janela de teste (2023-05..2023-08), sem refazer transformações e sem expor dados pessoais.

## Design
- `src/gold/dashboard/gerar_dashboard.py` gera `tcc_receita.lvdash.json` (datasets SQL + widgets + layout).
- Fontes: `fato_indicadores_setoriais_mes` (única fonte de aberturas), `obt_dashboard_eventos` (eventos e mudanças de situação da matriz), `fato_indicadores_macro_mes`,
  `dashboard_mercado_ativo`. Datasets já agregados (poucas linhas → warehouse pequeno basta).
- 2023-05 (carga inicial) excluído das séries de fluxo.
- LGPD: lista de empresas só não-MEI e sem logradouro; nenhuma informação de sócio.
- `ds_micro_*` (parâmetros `:param_*`) **não** usados: substituídos por filtros de campo na página "Mercado ativo".

## Tarefas
- [x] Gerador + JSON + README
- [x] Formato alinhado ao JSON regravado pelo workspace (grade 12, `layoutVersion` GRID_V1, `lines` com "\n")
- [x] Colunas novas (spec de semântica §2.4): painel com reativações/entradas/saídas/baixas de ativas/suspensões;
      `taxas_setor` com `qtd_baixas_de_ativas`; `eventos` com `entidade_tipo`/`eh_matriz`; dataset `situacao`
      (matriz, `COUNT(DISTINCT cnpj_raiz)`); `taxas_uf`; `mercado_bairros` (top 20 por município);
      `mercado_empresas` com `exibir_razao_social` + `ORDER BY capital_social DESC LIMIT 1000` (C-13)
- [x] Widgets novos: contadores de reativações e saldo; série "Reativações"; `b_saldo_uf` em taxa por 1.000 ativas
- [x] Validação local no gerador (datasets, colunas, filtros, grade)
- [ ] Importar a versão nova no workspace, rodar `EXPLAIN` dos datasets e corrigir widgets que o formato não aceitar
- [ ] Decidir destino de `ds_micro_*` e `vw_dashboard_eventos_cnae` (provável remoção)
- [ ] Taxas que reajam aos filtros (hoje `taxas_setor` é Brasil e `taxas_uf` só filtra UF: o widget não faz razão de somas)
- [ ] Reavaliar `LIMIT` do `mercado_empresas` (filtros de UF/município atuam só sobre as 1.000 maiores do país)

## Critérios de aceite
- Import sem erro; todos os widgets renderizam após o Job completo.
- Nenhum nome de sócio, CPF, nome de MEI ou endereço visível.

## Impacto no TCC
Prints para o capítulo de Resultados; descrição dos indicadores (taxa por 1.000 ativas) cabe na Metodologia.
