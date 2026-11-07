-- Silver Final | dim_tempo
-- Dimensão mensal contínua; considera somente meses válidos YYYY-MM.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_final.dim_tempo (
    sk_tempo BIGINT COMMENT 'Chave substituta hash 64-bit do NK YYYYMM.',
    nk_tempo INT COMMENT 'Chave natural inteira no formato YYYYMM.',
    mes_referencia STRING COMMENT 'Mês canônico YYYY-MM.',
    data_referencia DATE COMMENT 'Primeiro dia do mês.',
    data_fim_mes DATE COMMENT 'Último dia do mês calendário.',
    ano INT COMMENT 'Ano calendário.',
    semestre INT COMMENT 'Semestre (1 ou 2).',
    trimestre INT COMMENT 'Trimestre (1 a 4).',
    mes INT COMMENT 'Mês numérico (1 a 12).',
    nome_mes STRING COMMENT 'Nome do mês em português.',
    qtd_dias_mes INT COMMENT 'Quantidade de dias do mês.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na Silver Final.'
)
USING DELTA
COMMENT 'Silver Final: dimensão calendário mensal contínua para análises temporais.';

INSERT OVERWRITE tcc_receita.silver_final.dim_tempo (
    sk_tempo, nk_tempo, mes_referencia, data_referencia, data_fim_mes,
    ano, semestre, trimestre, mes, nome_mes, qtd_dias_mes, data_carga_etl
)
WITH meses_log AS (
    SELECT TO_DATE(CONCAT(TRIM(mes_referencia), '-01'), 'yyyy-MM-dd') AS mes_data
    FROM tcc_receita.silver_intermediate.log_eventos_negocio
    WHERE mes_referencia RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
),
limites AS (
    SELECT
        LEAST(DATE'2020-01-01', COALESCE(MIN(mes_data), DATE'2020-01-01')) AS dt_min,
        GREATEST(
            ADD_MONTHS(TRUNC(CURRENT_DATE(), 'MM'), 12),
            COALESCE(MAX(mes_data), TRUNC(CURRENT_DATE(), 'MM'))
        ) AS dt_max
    FROM meses_log
),
sequencia_mensal AS (
    SELECT EXPLODE(SEQUENCE(dt_min, dt_max, INTERVAL 1 MONTH)) AS data_referencia
    FROM limites
),
base_tempo AS (
    SELECT
        CAST(DATE_FORMAT(data_referencia, 'yyyyMM') AS INT) AS nk_tempo,
        DATE_FORMAT(data_referencia, 'yyyy-MM') AS mes_referencia,
        data_referencia,
        LAST_DAY(data_referencia) AS data_fim_mes,
        YEAR(data_referencia) AS ano,
        CASE WHEN MONTH(data_referencia) <= 6 THEN 1 ELSE 2 END AS semestre,
        QUARTER(data_referencia) AS trimestre,
        MONTH(data_referencia) AS mes,
        CASE MONTH(data_referencia)
            WHEN 1 THEN 'JANEIRO' WHEN 2 THEN 'FEVEREIRO' WHEN 3 THEN 'MARÇO'
            WHEN 4 THEN 'ABRIL' WHEN 5 THEN 'MAIO' WHEN 6 THEN 'JUNHO'
            WHEN 7 THEN 'JULHO' WHEN 8 THEN 'AGOSTO' WHEN 9 THEN 'SETEMBRO'
            WHEN 10 THEN 'OUTUBRO' WHEN 11 THEN 'NOVEMBRO' WHEN 12 THEN 'DEZEMBRO'
        END AS nome_mes,
        DAY(LAST_DAY(data_referencia)) AS qtd_dias_mes
    FROM sequencia_mensal
),
sentinela AS (
    SELECT
        CAST(-1 AS BIGINT) AS sk_tempo,
        190001 AS nk_tempo,
        '1900-01' AS mes_referencia,
        DATE'1900-01-01' AS data_referencia,
        DATE'1900-01-31' AS data_fim_mes,
        1900 AS ano, 1 AS semestre, 1 AS trimestre, 1 AS mes,
        'NAO APLICAVEL' AS nome_mes,
        31 AS qtd_dias_mes,
        CURRENT_TIMESTAMP() AS data_carga_etl
)
SELECT * FROM sentinela
UNION ALL
SELECT
    (XXHASH64(nk_tempo) & 9223372036854775807) AS sk_tempo,
    nk_tempo, mes_referencia, data_referencia, data_fim_mes,
    ano, semestre, trimestre, mes, nome_mes, qtd_dias_mes,
    CURRENT_TIMESTAMP() AS data_carga_etl
FROM base_tempo
WHERE nk_tempo <> 190001;
