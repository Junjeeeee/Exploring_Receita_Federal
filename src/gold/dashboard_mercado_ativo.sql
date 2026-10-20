CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE OR REPLACE TABLE tcc_receita.gold.dashboard_mercado_ativo AS
WITH bridge_primaria AS (
    SELECT SK_cnae_group, SK_cnae 
    FROM tcc_receita.silver_final.bridge_cnaes 
    WHERE is_primary = TRUE
)
SELECT /*+ BROADCAST(cnae) */
    c.cnpj_raiz,
    c.cnpj_matriz,
    c.legal_name AS razao_social,
    c.matriz_fantasy_name AS nome_fantasia,
    c.company_size AS porte_empresa,
    c.capital_social,
    c.start_of_activity_dt AS data_inicio_atividade,
    c.state AS uf,
    c.city AS municipio,
    c.neighborhood AS bairro,
    cnae.cnae AS codigo_cnae,
    cnae.cnae_name AS nome_cnae,
    cnae.class_name AS classe_cnae,
    cnae.great_area_of_activity_name AS grande_area_cnae,
    NOW() AS etl_load_ts
FROM tcc_receita.silver_final.dim_company c
LEFT JOIN bridge_primaria b 
    ON c.SK_cnae_group = b.SK_cnae_group
LEFT JOIN tcc_receita.silver_final.dim_cnae cnae 
    ON b.SK_cnae = cnae.SK_cnae
WHERE c.is_current = TRUE 
  AND c.matriz_cadastral_status = 'ATIVA';

OPTIMIZE tcc_receita.gold.dashboard_mercado_ativo ZORDER BY (uf, municipio, nome_cnae);