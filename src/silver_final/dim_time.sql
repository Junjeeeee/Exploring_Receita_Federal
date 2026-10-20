CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_time AS
WITH meses_distintos AS (
    SELECT DISTINCT mes_referencia
    FROM tcc_receita.silver_intermediate.log_eventos_negocio
),
base_time AS (
    SELECT 
        CAST(regexp_replace(mes_referencia, '-', '') AS INT) AS NK_time,
        to_date(concat(mes_referencia, '-01'), 'yyyy-MM-dd') AS date
    FROM meses_distintos
)
SELECT 
    ABS(xxhash64(NK_time)) AS SK_time,
    NK_time,
    date,
    year(date) AS year,
    CASE WHEN month(date) <= 6 THEN 1 ELSE 2 END AS semester,
    quarter(date) AS quarter,
    month(date) AS month,
    TRUE AS is_business_day,
    NOW() AS etl_load_ts
FROM base_time;