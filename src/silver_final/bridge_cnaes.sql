CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.bridge_cnaes AS
WITH combinacoes_unicas AS (
    SELECT DISTINCT
        COALESCE(cnae_fiscal_principal, '') AS cnae_prim,
        COALESCE(cnae_fiscal_secundaria, '') AS cnae_sec
    FROM tcc_receita.silver_intermediate.estabelecimentos
    WHERE identificador_matriz_filial = '1'
),
grupos_hash AS (
    SELECT 
        ABS(xxhash64(cnae_prim, cnae_sec)) AS SK_cnae_group,
        concat_ws('|', cnae_prim, cnae_sec) AS NK_cnae_group,
        NULLIF(cnae_prim, '') AS cnae_prim,
        NULLIF(cnae_sec, '') AS cnae_sec
    FROM combinacoes_unicas
),
primarios AS (
    SELECT SK_cnae_group, NK_cnae_group, CAST(cnae_prim AS INT) AS NK_cnae, TRUE AS is_primary
    FROM grupos_hash WHERE cnae_prim IS NOT NULL
),
secundarios AS (
    SELECT SK_cnae_group, NK_cnae_group, CAST(TRIM(cnae_item) AS INT) AS NK_cnae, FALSE AS is_primary
    FROM grupos_hash 
    LATERAL VIEW explode(split(cnae_sec, ',')) AS cnae_item
    WHERE cnae_sec IS NOT NULL
),
uniao AS (
    SELECT * FROM primarios
    UNION ALL
    SELECT * FROM secundarios WHERE NK_cnae IS NOT NULL
)
SELECT /*+ BROADCAST(d) */
    u.SK_cnae_group,
    d.SK_cnae,
    u.NK_cnae_group,
    u.NK_cnae,
    MAX(u.is_primary) AS is_primary,
    NOW() AS etl_load_ts
FROM uniao u
INNER JOIN tcc_receita.silver_final.dim_cnae d 
    ON u.NK_cnae = d.NK_cnae
GROUP BY u.SK_cnae_group, d.SK_cnae, u.NK_cnae_group, u.NK_cnae;