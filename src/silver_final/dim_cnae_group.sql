CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_cnae_group AS
WITH combinacoes_matriz AS (
    SELECT 
        COALESCE(cnae_fiscal_principal, '') AS cnae_prim,
        COALESCE(cnae_fiscal_secundaria, '') AS cnae_sec,
        valid_since,
        valid_until,
        is_current
    FROM tcc_receita.silver_intermediate.estabelecimentos
    WHERE identificador_matriz_filial = '1'
)
SELECT 
    ABS(xxhash64(cnae_prim, cnae_sec)) AS SK_cnae_group,
    concat_ws('|', cnae_prim, cnae_sec) AS NK_cnae_group,
    MIN(valid_since) AS valid_from,
    MAX(valid_until) AS valid_until,
    MAX(is_current)  AS is_current,
    NOW() AS etl_load_ts
FROM combinacoes_matriz
GROUP BY cnae_prim, cnae_sec;