CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_mudanca_estado AS
WITH logs_matriz AS (
    SELECT DISTINCT
        evento AS tipo_mudanca_estado,
        CONCAT_WS(' -> ', valor_antigo, valor_novo) AS detalhe_mudanca_estado,
        informacao_adicional AS descricao_adicional
    FROM tcc_receita.silver_intermediate.log_eventos_negocio
    WHERE pk NOT LIKE '%-%' OR SPLIT_PART(pk, '-', 2) = '0001'
),
ghost_row AS (
    SELECT 
        CAST(-1 AS BIGINT) AS sk_mudanca_estado,
        'NAO_APLICAVEL' AS nk_mudanca_estado,
        'NAO APLICAVEL' AS tipo_mudanca_estado,
        'NAO APLICAVEL' AS detalhe_mudanca_estado,
        'REGISTO FANTASMA PARA INTEGRIDADE REFERENCIAL' AS descricao_adicional,
        NOW() AS data_carga_etl
)
SELECT * FROM ghost_row
UNION ALL
SELECT 
    ABS(xxhash64(tipo_mudanca_estado, detalhe_mudanca_estado, COALESCE(descricao_adicional, ''))) AS sk_mudanca_estado,
    CONCAT_WS('|', tipo_mudanca_estado, detalhe_mudanca_estado, COALESCE(descricao_adicional, '')) AS nk_mudanca_estado,
    tipo_mudanca_estado,
    detalhe_mudanca_estado,
    descricao_adicional,
    NOW() AS data_carga_etl
FROM logs_matriz;