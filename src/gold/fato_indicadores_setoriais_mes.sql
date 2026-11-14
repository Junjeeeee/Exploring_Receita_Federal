-- Gold | fato_indicadores_setoriais_mes
-- Grão: mês × UF × porte × CNAE principal (com classe, divisão e grande área).
-- Fonte: gold.fato_empresa_mes (empresa × mês) + gold.fato_indicadores_macro_mes.
-- Spec: docs/specs/features/2026-11-14-semantica-eventos-e-fluxos.md (seção 2.4).
--
-- Estoque e fluxos fecham por construção em cada linha deste painel:
--     estoque_final_ativas = estoque_inicial_ativas + qtd_entradas_estoque − qtd_saidas_estoque
-- (no baseline não há fluxo: o estoque final é o estoque de abertura).
-- Atenção: estoque_inicial(M) = estoque_final(M−1) vale no TOTAL do mês, não por grupo — uma empresa que
-- muda de UF/CNAE/porte entre M−1 e M é contada no grupo de M.

CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE OR REPLACE TABLE tcc_receita.gold.fato_indicadores_setoriais_mes
COMMENT 'Camada Gold: painel mensal (mês × UF × porte × CNAE) com estoque de ativas, fluxos de entrada/saída, eventos de situação, taxas por 1.000 ativas no início do mês e indicadores macroeconômicos.'
AS
WITH agregado_setorial AS (
    -- Contagens por grupo. "_de_ativas" = só empresas que estavam ATIVAS no início do mês: são subconjunto
    -- de estoque_inicial_ativas e por isso servem de numerador das taxas (nunca passam de 1.000‰).
    SELECT
        f.mes_referencia,
        f.data_referencia,
        f.uf,
        f.porte_empresa,
        f.grande_area_cnae,
        f.divisao_cnae,
        f.classe_cnae,
        f.codigo_cnae,
        f.nome_cnae,
        COUNT_IF(f.ativa_inicio_mes) AS estoque_inicial_ativas,
        COUNT_IF(f.ativa_fim_mes) AS estoque_final_ativas,
        COUNT_IF(f.teve_abertura_mes) AS qtd_aberturas,
        COUNT_IF(f.teve_baixa_mes) AS qtd_baixas,
        COUNT_IF(f.teve_falencia_mes) AS qtd_falencias,
        COUNT_IF(f.teve_inaptidao_mes) AS qtd_inaptidoes,
        AVG(CASE WHEN f.ativa_fim_mes THEN CAST(f.idade_meses AS DOUBLE) END) AS idade_media_ativas_meses,
        COUNT_IF(f.teve_reativacao_mes) AS qtd_reativacoes,
        COUNT_IF(f.entrou_estoque_mes) AS qtd_entradas_estoque,
        COUNT_IF(f.saiu_estoque_mes) AS qtd_saidas_estoque,
        COUNT_IF(f.teve_baixa_mes AND f.ativa_inicio_mes) AS qtd_baixas_de_ativas,
        COUNT_IF(f.teve_suspensao_mes) AS qtd_suspensoes,
        COUNT_IF(f.teve_falencia_mes AND f.ativa_inicio_mes) AS qtd_falencias_de_ativas,
        COUNT_IF(f.teve_inaptidao_mes AND f.ativa_inicio_mes) AS qtd_inaptidoes_de_ativas
    FROM tcc_receita.gold.fato_empresa_mes f
    GROUP BY
        f.mes_referencia, f.data_referencia, f.uf, f.porte_empresa,
        f.grande_area_cnae, f.divisao_cnae, f.classe_cnae, f.codigo_cnae, f.nome_cnae
)
SELECT /*+ BROADCAST(m) */
    a.mes_referencia,
    a.data_referencia,
    a.uf,
    a.porte_empresa,
    a.grande_area_cnae,
    a.divisao_cnae,
    a.classe_cnae,
    a.codigo_cnae,
    a.nome_cnae,
    a.estoque_inicial_ativas,
    a.estoque_final_ativas,
    a.qtd_aberturas,
    -- Todas as baixas do mês (inclui quem já estava INAPTA/SUSPENSA). Para taxa use qtd_baixas_de_ativas.
    a.qtd_baixas,
    a.qtd_falencias,
    a.qtd_inaptidoes,
    a.idade_media_ativas_meses,
    -- Saldo do estoque de ativas: fecha com estoque_final − estoque_inicial por construção.
    (a.qtd_entradas_estoque - a.qtd_saidas_estoque) AS crescimento_liquido,
    -- Taxas por 1.000 empresas ativas no início do mês (população em risco).
    ROUND(1000.0 * a.qtd_aberturas            / NULLIF(a.estoque_inicial_ativas, 0), 4) AS taxa_abertura_por_mil,
    ROUND(1000.0 * a.qtd_baixas_de_ativas     / NULLIF(a.estoque_inicial_ativas, 0), 4) AS taxa_fechamento_por_mil,
    ROUND(1000.0 * a.qtd_falencias_de_ativas  / NULLIF(a.estoque_inicial_ativas, 0), 4) AS taxa_falencia_por_mil,
    ROUND(1000.0 * a.qtd_inaptidoes_de_ativas / NULLIF(a.estoque_inicial_ativas, 0), 4) AS taxa_inaptidao_por_mil,
    m.selic_mensal_pct,
    m.selic_anualizada_media_pct,
    m.ipca_mensal_pct,
    m.ipca_acumulado_12m_pct,
    m.juro_real_ex_post_anual_pct,
    m.selic_anualizada_lag_1m,
    m.selic_anualizada_lag_3m,
    m.selic_anualizada_lag_6m,
    m.selic_anualizada_lag_12m,
    NOW() AS data_carga_etl,
    -- Colunas novas (2026-11-14), no fim para não deslocar as existentes.
    a.qtd_reativacoes,
    a.qtd_entradas_estoque,
    a.qtd_saidas_estoque,
    a.qtd_baixas_de_ativas,
    a.qtd_suspensoes,
    a.qtd_falencias_de_ativas,
    a.qtd_inaptidoes_de_ativas
FROM agregado_setorial a
LEFT JOIN tcc_receita.gold.fato_indicadores_macro_mes m
    ON a.mes_referencia = m.mes_referencia;
