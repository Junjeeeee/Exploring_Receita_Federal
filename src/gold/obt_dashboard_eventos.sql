CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE TABLE IF NOT EXISTS tcc_receita.gold.obt_dashboard_eventos (
    data_referencia DATE,
    ano INT,
    mes INT,
    tipo_evento STRING,
    transicao_detalhe STRING,
    motivo_detalhe STRING,
    categoria_macro_evento STRING,
    cnpj_raiz STRING,
    razao_social STRING,
    porte_empresa STRING,
    capital_social DOUBLE,
    uf STRING,
    municipio STRING,
    bairro STRING,
    codigo_cnae STRING,
    nome_cnae STRING,
    classe_cnae STRING,
    grupo_cnae STRING,
    divisao_cnae STRING,
    grande_area_cnae STRING,
    etl_load_ts TIMESTAMP
);

-- 1. Idempotência com fallback para Mês-1
DELETE FROM tcc_receita.gold.obt_dashboard_eventos
WHERE data_referencia >= to_date(concat(COALESCE(NULLIF(TRIM({{mes_referencia_inicial}}), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')), '-01'), 'yyyy-MM-dd')
  AND data_referencia <= to_date(concat(COALESCE(NULLIF(TRIM({{mes_referencia_final}}), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')), '-01'), 'yyyy-MM-dd');

-- 2. Inserção Incremental com Broadcast nas dimensões pequenas
INSERT INTO tcc_receita.gold.obt_dashboard_eventos
WITH params AS (
    SELECT 
        CAST(regexp_replace(COALESCE(NULLIF(TRIM({{mes_referencia_inicial}}), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')), '-', '') AS INT) AS nk_ini,
        CAST(regexp_replace(COALESCE(NULLIF(TRIM({{mes_referencia_final}}), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')), '-', '') AS INT) AS nk_fim
),
bridge_primaria AS (
    SELECT SK_cnae_group, SK_cnae 
    FROM tcc_receita.silver_final.bridge_cnaes 
    WHERE is_primary = TRUE
)
SELECT /*+ BROADCAST(t, sc, cnae) */
    t.date AS data_referencia,
    t.year AS ano,
    t.month AS mes,
    sc.state_change_type AS tipo_evento,
    sc.state_change_detail AS transicao_detalhe,
    sc.description AS motivo_detalhe,
    CASE 
        WHEN sc.state_change_type = 'EMPRESA NASCEU' THEN 'ABERTURA'
        WHEN sc.state_change_type = 'MUDOU ESTADO CADASTRAL' AND sc.description LIKE '%FALENCIA%' THEN 'FECHAMENTO POR FALÊNCIA'
        WHEN sc.state_change_type = 'MUDOU ESTADO CADASTRAL' AND sc.description LIKE '%OBITO DO MEI%' THEN 'EXTINÇÃO POR ÓBITO (MEI)'
        WHEN sc.state_change_type = 'MUDOU ESTADO CADASTRAL' AND (sc.description LIKE '%EXTINCAO POR ENCERRAMENTO LIQUIDACAO%' OR sc.description LIKE '%LIQUIDACAO JUDICIAL%') THEN 'EXTINÇÃO POR LIQUIDAÇÃO'
        WHEN sc.state_change_type = 'MUDOU ESTADO CADASTRAL' AND (sc.description LIKE '%OMISSAO DE DECLARACOES%' OR sc.description LIKE '%INAPTIDAO%') THEN 'INAPTIDÃO FISCAL / OMISSÃO'
        WHEN sc.state_change_type = 'MUDOU ESTADO CADASTRAL' AND sc.state_change_detail LIKE '%BAIXADA%' THEN 'BAIXA VOLUNTÁRIA / OUTRAS'
        WHEN sc.state_change_type = 'MUDOU ESTADO CADASTRAL' AND sc.state_change_detail LIKE '%INAPTA%' THEN 'INAPTIDÃO FISCAL'
        WHEN sc.state_change_type = 'MUDOU ESTADO CADASTRAL' AND sc.state_change_detail LIKE '%SUSPENSA%' THEN 'SUSPENSÃO TEMPORÁRIA'
        WHEN sc.state_change_type = 'MUDOU ESTADO CADASTRAL' AND sc.state_change_detail LIKE '%NULA%' THEN 'INSCRIÇÃO NULA / VÍCIOS'
        ELSE sc.state_change_type
    END AS categoria_macro_evento,
    c_after.cnpj_raiz,
    c_after.legal_name AS razao_social,
    c_after.company_size AS porte_empresa,
    c_after.capital_social,
    c_after.state AS uf,
    c_after.city AS municipio,
    c_after.neighborhood AS bairro,
    cnae.cnae AS codigo_cnae,
    cnae.cnae_name AS nome_cnae,
    cnae.class_name AS classe_cnae,
    cnae.group_name AS grupo_cnae,
    cnae.division_name AS divisao_cnae,
    cnae.great_area_of_activity_name AS grande_area_cnae,
    NOW() AS etl_load_ts
FROM tcc_receita.silver_final.fact_state_change f
INNER JOIN tcc_receita.silver_final.dim_time t 
    ON f.SK_time = t.SK_time
CROSS JOIN params p
INNER JOIN tcc_receita.silver_final.dim_state_change sc 
    ON f.SK_state_change = sc.SK_state_change
INNER JOIN tcc_receita.silver_final.dim_company c_after 
    ON f.SK_company_after = c_after.SK_company
LEFT JOIN bridge_primaria b 
    ON f.SK_cnae_group_after = b.SK_cnae_group
LEFT JOIN tcc_receita.silver_final.dim_cnae cnae 
    ON b.SK_cnae = cnae.SK_cnae
WHERE t.NK_time >= p.nk_ini
  AND t.NK_time <= p.nk_fim;

OPTIMIZE tcc_receita.gold.obt_dashboard_eventos ZORDER BY (data_referencia, uf, nome_cnae);