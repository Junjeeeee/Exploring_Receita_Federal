CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE OR REPLACE TABLE tcc_receita.gold.fato_indicadores_setoriais_mes
COMMENT 'Camada Gold: Painel mensal (Mês x CNAE x UF x Porte) com estoque em risco, aberturas, baixas, taxas por 1.000 empresas e indicadores macroeconômicos.'
AS
WITH agregado_setorial AS (
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
        SUM(CASE WHEN f.ativa_inicio_mes = TRUE THEN 1 ELSE 0 END) AS estoque_inicial_ativas,
        SUM(CASE WHEN f.ativa_fim_mes = TRUE THEN 1 ELSE 0 END) AS estoque_final_ativas,
        SUM(CASE WHEN f.teve_abertura_mes = TRUE THEN 1 ELSE 0 END) AS qtd_aberturas,
        SUM(CASE WHEN f.teve_baixa_mes = TRUE THEN 1 ELSE 0 END) AS qtd_baixas,
        SUM(CASE WHEN f.teve_falencia_mes = TRUE THEN 1 ELSE 0 END) AS qtd_falencias,
        SUM(CASE WHEN f.teve_inaptidao_mes = TRUE THEN 1 ELSE 0 END) AS qtd_inaptidoes,
        AVG(CASE WHEN f.ativa_fim_mes = TRUE THEN CAST(f.idade_meses AS DOUBLE) ELSE NULL END) AS idade_media_ativas_meses
    FROM tcc_receita.gold.fato_empresa_mes f
    GROUP BY 
        f.mes_referencia, f.data_referencia, f.uf, f.porte_empresa,
        f.grande_area_cnae, f.divisao_cnae, f.classe_cnae, f.codigo_cnae, f.nome_cnae
)
SELECT /*+ BROADCAST(m) */
    a.*,
    (a.qtd_aberturas - a.qtd_baixas) AS crescimento_liquido,
    ROUND(1000.0 * a.qtd_aberturas  / NULLIF(a.estoque_inicial_ativas, 0), 4) AS taxa_abertura_por_mil,
    ROUND(1000.0 * a.qtd_baixas     / NULLIF(a.estoque_inicial_ativas, 0), 4) AS taxa_fechamento_por_mil,
    ROUND(1000.0 * a.qtd_falencias  / NULLIF(a.estoque_inicial_ativas, 0), 4) AS taxa_falencia_por_mil,
    ROUND(1000.0 * a.qtd_inaptidoes / NULLIF(a.estoque_inicial_ativas, 0), 4) AS taxa_inaptidao_por_mil,
    m.selic_mensal_pct,
    m.selic_anualizada_media_pct,
    m.ipca_mensal_pct,
    m.ipca_acumulado_12m_pct,
    m.juro_real_ex_post_anual_pct,
    m.selic_anualizada_lag_1m,
    m.selic_anualizada_lag_3m,
    m.selic_anualizada_lag_6m,
    m.selic_anualizada_lag_12m,
    NOW() AS data_carga_etl
FROM agregado_setorial a
LEFT JOIN tcc_receita.gold.fato_indicadores_macro_mes m
    ON a.mes_referencia = m.mes_referencia;