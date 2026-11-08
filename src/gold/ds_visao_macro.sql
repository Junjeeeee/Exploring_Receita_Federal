CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE OR REPLACE VIEW tcc_receita.gold.ds_visao_macro
COMMENT 'Camada Gold: Dataset macro de eventos mensais separando volume de eventos da contagem de empresas distintas.'
AS
SELECT 
    e.mes_referencia,
    e.data_referencia,
    COALESCE(e.porte_empresa, 'NAO IDENTIFICADO') AS porte_empresa,
    e.tipo_evento,
    e.categoria_macro_evento,
    e.grande_area_cnae,
    e.divisao_cnae,
    e.uf,
    COUNT(*) AS quantidade_eventos,
    COUNT(DISTINCT e.cnpj_raiz) AS empresas_com_evento,
    m.selic_anualizada_media_pct,
    m.ipca_acumulado_12m_pct,
    m.juro_real_ex_post_anual_pct
FROM tcc_receita.gold.obt_dashboard_eventos e
LEFT JOIN tcc_receita.gold.fato_indicadores_macro_mes m
    ON e.mes_referencia = m.mes_referencia
GROUP BY ALL;