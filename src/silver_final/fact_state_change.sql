CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_final.fact_state_change (
    SK_state_change BIGINT,
    SK_company_before BIGINT,
    SK_company_after BIGINT,
    SK_cnae_group_before BIGINT,
    SK_cnae_group_after BIGINT,
    SK_time BIGINT,
    etl_load_ts TIMESTAMP
);

-- 1. Idempotência com fallback automático para Mês-1 se os parâmetros vierem vazios
DELETE FROM tcc_receita.silver_final.fact_state_change 
WHERE SK_time IN (
    SELECT SK_time FROM tcc_receita.silver_final.dim_time 
    WHERE NK_time >= CAST(regexp_replace(COALESCE(NULLIF(TRIM({{mes_referencia_inicial}}), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')), '-', '') AS INT)
      AND NK_time <= CAST(regexp_replace(COALESCE(NULLIF(TRIM({{mes_referencia_final}}), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')), '-', '') AS INT)
);

-- 2. Inserção Incremental Otimizada
INSERT INTO tcc_receita.silver_final.fact_state_change
WITH params AS (
    SELECT 
        COALESCE(NULLIF(TRIM({{mes_referencia_inicial}}), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM({{mes_referencia_final}}), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_fim
),
logs_matriz AS (
    SELECT 
        split_part(l.pk, '-', 1) AS cnpj_raiz,
        l.mes_referencia,
        l.evento,
        ABS(xxhash64(CAST(regexp_replace(l.mes_referencia, '-', '') AS INT))) AS SK_time,
        ABS(xxhash64(l.evento, concat_ws(' -> ', l.valor_antigo, l.valor_novo), COALESCE(l.informacao_adicional, ''))) AS SK_state_change
    FROM tcc_receita.silver_intermediate.log_eventos_negocio l
    CROSS JOIN params p
    WHERE l.mes_referencia >= p.mes_ini 
      AND l.mes_referencia <= p.mes_fim
      AND (l.pk NOT LIKE '%-%' OR split_part(l.pk, '-', 2) = '0001')
),
-- Filtra dim_company apenas para os CNPJs que tiveram eventos na janela (reduz drasticamente o Join)
cnpjs_afetados AS (
    SELECT DISTINCT cnpj_raiz FROM logs_matriz
),
comp_filtrada AS (
    SELECT c.SK_company, c.SK_cnae_group, c.cnpj_raiz, c.valid_since, c.valid_until
    FROM tcc_receita.silver_final.dim_company c
    INNER JOIN cnpjs_afetados a ON c.cnpj_raiz = a.cnpj_raiz
),
fatos_pareados AS (
    SELECT 
        l.SK_state_change,
        c_before.SK_company AS SK_company_before,
        c_after.SK_company  AS SK_company_after,
        c_before.SK_cnae_group AS SK_cnae_group_before,
        c_after.SK_cnae_group  AS SK_cnae_group_after,
        l.SK_time,
        NOW() AS etl_load_ts,
        ROW_NUMBER() OVER (
            PARTITION BY l.cnpj_raiz, l.mes_referencia, l.SK_state_change 
            ORDER BY 
                CASE WHEN l.mes_referencia >= c_after.valid_since AND l.mes_referencia < c_after.valid_until THEN 0 ELSE 1 END,
                c_after.valid_since ASC
        ) AS rn
    FROM logs_matriz l
    INNER JOIN comp_filtrada c_after
        ON l.cnpj_raiz = c_after.cnpj_raiz
       AND (
           (l.mes_referencia >= c_after.valid_since AND l.mes_referencia < c_after.valid_until)
           OR (l.evento = 'EMPRESA NASCEU' AND l.mes_referencia < c_after.valid_since)
       )
    LEFT JOIN comp_filtrada c_before
        ON l.cnpj_raiz = c_before.cnpj_raiz
       AND l.mes_referencia = c_before.valid_until
)
SELECT 
    SK_state_change,
    SK_company_before,
    SK_company_after,
    SK_cnae_group_before,
    SK_cnae_group_after,
    SK_time,
    etl_load_ts
FROM fatos_pareados
WHERE rn = 1;