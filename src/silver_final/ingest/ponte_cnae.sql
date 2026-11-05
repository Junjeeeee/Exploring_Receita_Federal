CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.ponte_cnae AS
WITH combinacoes_unicas AS (
    SELECT DISTINCT
        COALESCE(cnae_fiscal_principal, '') AS cnae_prim,
        COALESCE(cnae_fiscal_secundaria, '') AS cnae_sec
    FROM tcc_receita.silver_intermediate.estabelecimentos
    WHERE identificador_matriz_filial = '1'
),
grupos_hash AS (
    SELECT 
        ABS(xxhash64(cnae_prim, cnae_sec)) AS sk_grupo_cnae,
        CONCAT_WS('|', cnae_prim, cnae_sec) AS nk_grupo_cnae,
        NULLIF(TRIM(cnae_prim), '') AS cnae_prim,
        NULLIF(TRIM(cnae_sec), '') AS cnae_sec
    FROM combinacoes_unicas
),
primarios AS (
    SELECT 
        sk_grupo_cnae, 
        nk_grupo_cnae, 
        cnae_prim AS nk_cnae, 
        TRUE AS cnae_principal
    FROM grupos_hash 
    WHERE cnae_prim IS NOT NULL
),
secundarios AS (
    SELECT 
        sk_grupo_cnae, 
        nk_grupo_cnae, 
        NULLIF(TRIM(cnae_item), '') AS nk_cnae, 
        FALSE AS cnae_principal
    FROM grupos_hash 
    LATERAL VIEW EXPLODE(SPLIT(cnae_sec, ',')) t AS cnae_item
    WHERE cnae_sec IS NOT NULL
),
uniao AS (
    SELECT * FROM primarios
    UNION ALL
    SELECT * FROM secundarios WHERE nk_cnae IS NOT NULL
),
ponte_enriquecida AS (
    SELECT /*+ BROADCAST(d) */
        u.sk_grupo_cnae,
        COALESCE(d.sk_cnae, CAST(-1 AS BIGINT)) AS sk_cnae,
        u.nk_grupo_cnae,
        u.nk_cnae,
        MAX(u.cnae_principal) AS cnae_principal,
        NOW() AS data_carga_etl
    FROM uniao u
    LEFT JOIN tcc_receita.silver_final.dim_cnae d 
        ON u.nk_cnae = d.codigo_cnae
    GROUP BY u.sk_grupo_cnae, COALESCE(d.sk_cnae, CAST(-1 AS BIGINT)), u.nk_grupo_cnae, u.nk_cnae
),
ghost_row AS (
    SELECT
        CAST(-1 AS BIGINT) AS sk_grupo_cnae,
        CAST(-1 AS BIGINT) AS sk_cnae,
        'NAO_APLICAVEL' AS nk_grupo_cnae,
        '0000000' AS nk_cnae,
        FALSE AS cnae_principal,
        NOW() AS data_carga_etl
)
SELECT * FROM ghost_row
UNION ALL
SELECT * FROM ponte_enriquecida;