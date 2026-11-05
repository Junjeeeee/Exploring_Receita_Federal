CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_grupo_cnae AS
WITH combinacoes_matriz AS (
    SELECT 
        COALESCE(cnae_fiscal_principal, '') AS cnae_prim,
        COALESCE(cnae_fiscal_secundaria, '') AS cnae_sec,
        valid_since,
        valid_until,
        is_current
    FROM tcc_receita.silver_intermediate.estabelecimentos
    WHERE identificador_matriz_filial = '1'
),
grupos_agrupados AS (
    SELECT 
        ABS(xxhash64(cnae_prim, cnae_sec)) AS sk_grupo_cnae,
        CONCAT_WS('|', cnae_prim, cnae_sec) AS nk_grupo_cnae,
        MIN(valid_since) AS inicio_vigencia,
        CASE 
            WHEN MAX(is_current) = TRUE THEN CAST(NULL AS STRING)
            ELSE MAX(valid_until)
        END AS fim_vigencia,
        MAX(is_current) AS registro_ativo,
        NOW() AS data_carga_etl
    FROM combinacoes_matriz
    GROUP BY cnae_prim, cnae_sec
),
ghost_row AS (
    SELECT 
        CAST(-1 AS BIGINT) AS sk_grupo_cnae,
        'NAO_APLICAVEL' AS nk_grupo_cnae,
        '1900-01' AS inicio_vigencia,
        CAST(NULL AS STRING) AS fim_vigencia,
        FALSE AS registro_ativo,
        NOW() AS data_carga_etl
)
SELECT * FROM ghost_row
UNION ALL
SELECT * FROM grupos_agrupados;