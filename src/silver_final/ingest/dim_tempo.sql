CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_tempo (
    sk_tempo BIGINT COMMENT 'Chave substituta (Hash 64-bit positivo sobre nk_tempo). -1 para Não Aplicável.',
    nk_tempo INT COMMENT 'Chave natural inteira do mês no formato YYYYMM (ex: 202305).',
    mes_referencia STRING COMMENT 'Mês e ano no formato canônico YYYY-MM.',
    data_referencia DATE COMMENT 'Primeiro dia do mês de referência (YYYY-MM-01).',
    data_fim_mes DATE COMMENT 'Último dia do calendário para o mês de referência.',
    ano INT COMMENT 'Ano calendário (YYYY).',
    semestre INT COMMENT 'Semestre do ano (1 ou 2).',
    trimestre INT COMMENT 'Trimestre do ano (1 a 4).',
    mes INT COMMENT 'Mês numérico do ano (1 a 12).',
    nome_mes STRING COMMENT 'Nome do mês em português.',
    qtd_dias_mes INT COMMENT 'Quantidade total de dias corridos no mês (28 a 31).',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na camada Silver Final.'
)
USING DELTA
COMMENT 'Camada Silver Final: Dimensão Calendário Mensal contínua para análises de séries temporais.';

INSERT OVERWRITE tcc_receita.silver_final.dim_tempo
WITH limites AS (
    SELECT 
        LEAST(
            TO_DATE('2020-01-01', 'yyyy-MM-dd'),
            COALESCE(MIN(TO_DATE(CONCAT(mes_referencia, '-01'), 'yyyy-MM-dd')), TO_DATE('2020-01-01', 'yyyy-MM-dd'))
        ) AS dt_min,
        GREATEST(
            ADD_MONTHS(TRUNC(CURRENT_DATE(), 'MM'), 12),
            COALESCE(MAX(TO_DATE(CONCAT(mes_referencia, '-01'), 'yyyy-MM-dd')), TRUNC(CURRENT_DATE(), 'MM'))
        ) AS dt_max
    FROM tcc_receita.silver_intermediate.log_eventos_negocio
    WHERE mes_referencia IS NOT NULL AND REGEXP_LIKE(mes_referencia, '^[0-9]{4}-[0-9]{2}$')
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
            WHEN 1 THEN 'JANEIRO'
            WHEN 2 THEN 'FEVEREIRO'
            WHEN 3 THEN 'MARÇO'
            WHEN 4 THEN 'ABRIL'
            WHEN 5 THEN 'MAIO'
            WHEN 6 THEN 'JUNHO'
            WHEN 7 THEN 'JULHO'
            WHEN 8 THEN 'AGOSTO'
            WHEN 9 THEN 'SETEMBRO'
            WHEN 10 THEN 'OUTUBRO'
            WHEN 11 THEN 'NOVEMBRO'
            WHEN 12 THEN 'DEZEMBRO'
        END AS nome_mes,
        DAY(LAST_DAY(data_referencia)) AS qtd_dias_mes
    FROM sequencia_mensal
),
ghost_row AS (
    SELECT 
        CAST(-1 AS BIGINT) AS sk_tempo,
        190001 AS nk_tempo,
        '1900-01' AS mes_referencia,
        TO_DATE('1900-01-01', 'yyyy-MM-dd') AS data_referencia,
        TO_DATE('1900-01-31', 'yyyy-MM-dd') AS data_fim_mes,
        1900 AS ano,
        1 AS semestre,
        1 AS trimestre,
        1 AS mes,
        'NAO APLICAVEL' AS nome_mes,
        31 AS qtd_dias_mes,
        NOW() AS data_carga_etl
)
SELECT * FROM ghost_row
UNION ALL
SELECT 
    (xxhash64(nk_tempo) & 9223372036854775807) AS sk_tempo,
    nk_tempo,
    mes_referencia,
    data_referencia,
    data_fim_mes,
    ano,
    semestre,
    trimestre,
    mes,
    nome_mes,
    qtd_dias_mes,
    NOW() AS data_carga_etl
FROM base_tempo
WHERE nk_tempo != 190001;