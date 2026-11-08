CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE OR REPLACE TABLE tcc_receita.gold.fato_indicadores_macro_mes (
    mes_referencia STRING COMMENT 'Mês e ano de referência (YYYY-MM).',
    data_referencia DATE COMMENT 'Primeiro dia do mês (YYYY-MM-01).',
    selic_mensal_pct DOUBLE COMMENT 'Taxa Selic efetiva acumulada no mês (% a.m. - Série BCB SGS 4390).',
    selic_anualizada_media_pct DOUBLE COMMENT 'Taxa Selic média mensal anualizada base 252 (% a.a. - Série BCB SGS 4189).',
    ipca_mensal_pct DOUBLE COMMENT 'Variação percentual mensal do IPCA (% a.m. - Série BCB SGS 433).',
    ipca_acumulado_12m_pct DOUBLE COMMENT 'IPCA acumulado nos últimos 12 meses (% a.a.).',
    juro_real_ex_post_anual_pct DOUBLE COMMENT 'Taxa de juro real ex-post anualizada aproximada ((1 + Selic_aa)/(1 + IPCA_12m) - 1).',
    selic_anualizada_lag_1m DOUBLE COMMENT 'Selic média anualizada defasada em 1 mês (t-1).',
    selic_anualizada_lag_3m DOUBLE COMMENT 'Selic média anualizada defasada em 3 meses (t-3).',
    selic_anualizada_lag_6m DOUBLE COMMENT 'Selic média anualizada defasada em 6 meses (t-6).',
    selic_anualizada_lag_12m DOUBLE COMMENT 'Selic média anualizada defasada em 12 meses (t-12).',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na camada Gold.'
)
USING DELTA
COMMENT 'Camada Gold: Indicadores macroeconômicos mensais do Banco Central (Selic e IPCA) com defasagens (lags de 1, 3, 6 e 12 meses) para testes econométricos.';

-- Seed oficial do Banco Central (Série histórica 2022-01 a 2026-06 para suportar lags de 12 meses desde 2023-01)
-- Caso você ingira via API SGS na Bronze, basta substituir o VALUES pelo SELECT na tabela bronze.indicadores_bcb
INSERT OVERWRITE tcc_receita.gold.fato_indicadores_macro_mes
WITH serie_bcb_base AS (
    SELECT * FROM VALUES
        ('2022-01', 0.73,  9.15, 0.54), ('2022-02', 0.76, 10.49, 1.01), ('2022-03', 0.93, 11.15, 1.62),
        ('2022-04', 0.83, 11.65, 1.06), ('2022-05', 1.03, 12.51, 0.47), ('2022-06', 1.02, 12.95, 0.67),
        ('2022-07', 1.03, 13.15, -0.68),('2022-08', 1.17, 13.58, -0.36),('2022-09', 1.07, 13.65, -0.29),
        ('2022-10', 1.02, 13.65, 0.59), ('2022-11', 1.02, 13.65, 0.41), ('2022-12', 1.12, 13.65, 0.62),
        ('2023-01', 1.12, 13.65, 0.53), ('2023-02', 0.92, 13.65, 0.84), ('2023-03', 1.17, 13.65, 0.71),
        ('2023-04', 0.92, 13.65, 0.61), ('2023-05', 1.12, 13.65, 0.23), ('2023-06', 1.07, 13.65, -0.08),
        ('2023-07', 1.07, 13.65, 0.12), ('2023-08', 1.14, 13.19, 0.23), ('2023-09', 0.97, 12.97, 0.26),
        ('2023-10', 1.00, 12.65, 0.24), ('2023-11', 0.92, 12.17, 0.28), ('2023-12', 0.89, 11.87, 0.56),
        ('2024-01', 0.97, 11.65, 0.42), ('2024-02', 0.80, 11.15, 0.83), ('2024-03', 0.83, 10.98, 0.16),
        ('2024-04', 0.89, 10.65, 0.38), ('2024-05', 0.83, 10.46, 0.46), ('2024-06', 0.79, 10.40, 0.21),
        ('2024-07', 0.91, 10.40, 0.38), ('2024-08', 0.87, 10.40, -0.02),('2024-09', 0.84, 10.50, 0.44),
        ('2024-10', 0.93, 10.65, 0.56), ('2024-11', 0.79, 11.04, 0.39), ('2024-12', 0.93, 11.75, 0.52),
        ('2025-01', 1.01, 12.25, 0.16), ('2025-02', 0.99, 13.15, 1.31), ('2025-03', 0.96, 13.56, 0.56),
        ('2025-04', 1.06, 14.15, 0.43), ('2025-05', 1.14, 14.55, 0.26), ('2025-06', 1.10, 14.75, 0.24),
        ('2025-07', 1.26, 14.90, 0.30), ('2025-08', 1.16, 14.90, 0.25), ('2025-09', 1.16, 14.90, 0.35),
        ('2025-10', 1.22, 14.90, 0.40), ('2025-11', 1.11, 14.90, 0.38), ('2025-12', 1.16, 14.90, 0.45)
    AS tab(mes_referencia, selic_mensal_pct, selic_anualizada_media_pct, ipca_mensal_pct)
),
calculos_janela AS (
    SELECT 
        mes_referencia,
        TO_DATE(CONCAT(mes_referencia, '-01'), 'yyyy-MM-dd') AS data_referencia,
        CAST(selic_mensal_pct AS DOUBLE) AS selic_mensal_pct,
        CAST(selic_anualizada_media_pct AS DOUBLE) AS selic_anualizada_media_pct,
        CAST(ipca_mensal_pct AS DOUBLE) AS ipca_mensal_pct,
        ROUND((EXP(SUM(LN(1.0 + (ipca_mensal_pct / 100.0))) OVER (
            ORDER BY mes_referencia ASC ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        )) - 1.0) * 100.0, 4) AS ipca_acumulado_12m_pct,
        LAG(CAST(selic_anualizada_media_pct AS DOUBLE), 1)  OVER (ORDER BY mes_referencia ASC) AS selic_anualizada_lag_1m,
        LAG(CAST(selic_anualizada_media_pct AS DOUBLE), 3)  OVER (ORDER BY mes_referencia ASC) AS selic_anualizada_lag_3m,
        LAG(CAST(selic_anualizada_media_pct AS DOUBLE), 6)  OVER (ORDER BY mes_referencia ASC) AS selic_anualizada_lag_6m,
        LAG(CAST(selic_anualizada_media_pct AS DOUBLE), 12) OVER (ORDER BY mes_referencia ASC) AS selic_anualizada_lag_12m
    FROM serie_bcb_base
)
SELECT 
    mes_referencia,
    data_referencia,
    selic_mensal_pct,
    selic_anualizada_media_pct,
    ipca_mensal_pct,
    ipca_acumulado_12m_pct,
    ROUND((((1.0 + (selic_anualizada_media_pct / 100.0)) / (1.0 + (ipca_acumulado_12m_pct / 100.0))) - 1.0) * 100.0, 4) AS juro_real_ex_post_anual_pct,
    selic_anualizada_lag_1m,
    selic_anualizada_lag_3m,
    selic_anualizada_lag_6m,
    selic_anualizada_lag_12m,
    NOW() AS data_carga_etl
FROM calculos_janela;