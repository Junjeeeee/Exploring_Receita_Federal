CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_tempo AS
WITH meses_distintos AS (
    SELECT DISTINCT mes_referencia
    FROM tcc_receita.silver_intermediate.log_eventos_negocio
    WHERE mes_referencia IS NOT NULL
),
base_tempo AS (
    SELECT 
        CAST(REGEXP_REPLACE(mes_referencia, '-', '') AS INT) AS nk_tempo,
        mes_referencia,
        TO_DATE(CONCAT(mes_referencia, '-01'), 'yyyy-MM-dd') AS data_referencia
    FROM meses_distintos
),
ghost_row AS (
    SELECT 
        CAST(-1 AS BIGINT) AS sk_tempo,
        190001 AS nk_tempo,
        '1900-01' AS mes_referencia,
        TO_DATE('1900-01-01', 'yyyy-MM-dd') AS data_referencia,
        1900 AS ano,
        1 AS semestre,
        1 AS trimestre,
        1 AS mes,
        FALSE AS dia_util,
        NOW() AS data_carga_etl
)
SELECT * FROM ghost_row
UNION ALL
SELECT 
    ABS(xxhash64(nk_tempo)) AS sk_tempo,
    nk_tempo,
    mes_referencia,
    data_referencia,
    YEAR(data_referencia) AS ano,
    CASE WHEN MONTH(data_referencia) <= 6 THEN 1 ELSE 2 END AS semestre,
    QUARTER(data_referencia) AS trimestre,
    MONTH(data_referencia) AS mes,
    TRUE AS dia_util,
    NOW() AS data_carga_etl
FROM base_tempo;