CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_state_change AS
WITH logs_matriz AS (
    SELECT DISTINCT
        evento AS state_change_type,
        concat_ws(' -> ', valor_antigo, valor_novo) AS state_change_detail,
        informacao_adicional AS description
    FROM tcc_receita.silver_intermediate.log_eventos_negocio
    WHERE pk NOT LIKE '%-%' OR split_part(pk, '-', 2) = '0001'
)
SELECT 
    ABS(xxhash64(state_change_type, state_change_detail, COALESCE(description, ''))) AS SK_state_change,
    concat_ws('|', state_change_type, state_change_detail, COALESCE(description, '')) AS NK_state_change,
    state_change_type,
    state_change_detail,
    description,
    NOW() AS etl_load_ts
FROM logs_matriz;