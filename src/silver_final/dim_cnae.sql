CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_cnae AS
WITH cnae_rf AS (
    SELECT TRIM(codigo) AS codigo, MAX(UPPER(TRIM(descricao))) AS descricao
    FROM tcc_receita.bronze.cnaes GROUP BY TRIM(codigo)
),
cnae_ibge AS (
    SELECT TRIM(codigo) AS codigo, MAX(UPPER(TRIM(descricao))) AS descricao,
        MAX(secao) AS great_area_of_activity, MAX(nome_secao) AS great_area_of_activity_name,
        MAX(divisao) AS division, MAX(nome_divisao) AS division_name,
        MAX(grupo) AS group, MAX(nome_grupo) AS group_name,
        MAX(classe) AS class, MAX(nome_classe) AS class_name
    FROM tcc_receita.bronze.cnaes_ibge GROUP BY TRIM(codigo)
),
base_cnae AS (
    SELECT 
        CAST(COALESCE(rf.codigo, ibge.codigo) AS INT) AS NK_cnae,
        COALESCE(rf.codigo, ibge.codigo) AS cnae,
        COALESCE(rf.descricao, ibge.descricao) AS cnae_name,
        ibge.great_area_of_activity, ibge.great_area_of_activity_name,
        ibge.division, ibge.division_name, ibge.group, ibge.group_name,
        ibge.class, ibge.class_name
    FROM cnae_rf rf
    FULL OUTER JOIN cnae_ibge ibge ON rf.codigo = ibge.codigo
    WHERE CAST(COALESCE(rf.codigo, ibge.codigo) AS INT) IS NOT NULL
)
SELECT 
    ABS(xxhash64(NK_cnae)) AS SK_cnae,
    *,
    NOW() AS etl_load_ts
FROM base_cnae;