# Spec — Gold / Dashboard v2

**Status:** em desenvolvimento na branch `ceps_ibge_and_dashboard_v2`. Parte do SQL está quebrado (ver backlog).

## Objetivo
Entregar agregados e datasets prontos para dashboards (Databricks), estudos de sobrevivência e painel econométrico,
sem que o dashboard refaça transformações.

## Objetos

| Arquivo | Objeto | Grão | Materialização | Uso |
|---|---|---|---|---|
| `obt_dashboard_eventos.sql` | tabela | evento × mês com estado ant/post | Delta, `REPLACE WHERE` janela | base de eventos; define `categoria_macro_evento` |
| `vw_dashboard_eventos_cnae.sql` | view | evento × CNAE (prim+sec, ANTERIOR/POSTERIOR) | view | multi-CNAE; usar `COUNT(DISTINCT cnpj_raiz)`; **possivelmente legado** |
| `fato_empresa_mes.sql` | tabela | `cnpj_raiz` × mês | Delta, `REPLACE WHERE` | estoque: `ativa_inicio_mes`, `ativa_fim_mes`, `teve_abertura/baixa/falencia/inaptidao_mes`, `idade_meses` |
| `fato_indicadores_macro_mes.sql` | tabela | mês | CREATE OR REPLACE (seed) | Selic, IPCA 12m, juro real, lags 1/3/6/12 |
| `fato_indicadores_setoriais_mes.sql` | tabela | mês × UF × porte × CNAE | CREATE OR REPLACE | estoques, fluxos, taxas por 1.000, + macro (painel) |
| `dashboard_mercado_ativo.sql` | tabela | matriz ativa atual | CREATE OR REPLACE + ZORDER | ranking de capital, lat/long |
| `d_coorte_sobrevivencia.sql` | view `ds_coorte_sobrevivencia` | `cnpj_raiz` | view | coortes, censura à direita, KM/Cox |
| `ds_visao_macro.sql` | view | mês × porte × evento × categoria × área × divisão × UF | view | visão macro + indicadores |
| `ds_micro_comparativo_geografico.sql` | query `:param_*` | mês × categoria × porte | dataset dashboard | bairro × cidade × UF × Brasil para um CNAE |
| `ds_micro_comparativo_setorial.sql` | query `:param_*` | mês × categoria | dataset dashboard | CNAE × classe × grande área — **quebrado** |
| `ds_micro_top5_empresas.sql` | query `:param_*` | top 5 por nível geográfico | dataset dashboard | capital social desc, mais antiga |
| `dq/dq_gold.sql` | DQ | | | count fato = count OBT; nulos na OBT; unicidade `fato_empresa_mes` |

Parâmetros de dashboard: `:param_uf`, `:param_municipio`, `:param_bairro`, `:param_cnae`.

## Dependências internas (ordem importa)
`fato_indicadores_macro_mes` → `obt_dashboard_eventos` → `fato_empresa_mes` → `fato_indicadores_setoriais_mes`;
`obt` + macro → `ds_visao_macro`; `dashboard_mercado_ativo` → `ds_micro_top5_empresas`.

## Pontos de atenção
- `fato_empresa_mes` depende das **strings** de `categoria_macro_evento` da OBT (acoplamento frágil).
- CTE `ponte_primaria` duplicada em ~5 arquivos.
- Seed macro termina em 2025-12 → meses posteriores com macro NULL.
