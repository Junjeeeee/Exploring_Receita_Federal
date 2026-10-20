CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE OR REPLACE VIEW tcc_receita.gold.vw_dashboard_eventos_cnae_secundario AS
SELECT 
    t.date AS data_referencia,
    sc.state_change_type AS tipo_evento,
    c_after.cnpj_raiz,
    c_after.legal_name AS razao_social,
    c_after.company_size AS porte_empresa,
    c_after.state AS uf,
    c_after.city AS municipio,
    cnae.cnae AS codigo_cnae,
    cnae.cnae_name AS nome_cnae,
    b.is_primary -- Permite filtrar se é o CNAE principal (TRUE) ou secundário (FALSE)
FROM tcc_receita.silver_final.fact_state_change f
INNER JOIN tcc_receita.silver_final.dim_time t 
    ON f.SK_time = t.SK_time
INNER JOIN tcc_receita.silver_final.dim_state_change sc 
    ON f.SK_state_change = sc.SK_state_change
INNER JOIN tcc_receita.silver_final.dim_company c_after 
    ON f.SK_company_after = c_after.SK_company
-- Aqui entram TODOS os CNAEs (principais e secundários) via Bridge:
INNER JOIN tcc_receita.silver_final.bridge_cnaes b 
    ON f.SK_cnae_group_after = b.SK_cnae_group
INNER JOIN tcc_receita.silver_final.dim_cnae cnae 
    ON b.SK_cnae = cnae.SK_cnae;