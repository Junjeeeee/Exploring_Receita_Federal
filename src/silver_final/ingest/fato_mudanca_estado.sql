CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_final.fato_mudanca_estado (
    sk_mudanca_estado BIGINT,
    sk_empresa_anterior BIGINT,
    sk_empresa_posterior BIGINT,
    sk_grupo_cnae_anterior BIGINT,
    sk_grupo_cnae_posterior BIGINT,
    sk_tempo BIGINT,
    data_carga_etl TIMESTAMP
);

-- 1. Idempotência com fallback automático para Mês-1 se os parâmetros vierem vazios
DELETE FROM tcc_receita.silver_final.fato_mudanca_estado 
WHERE sk_tempo IN (
    SELECT sk_tempo FROM tcc_receita.silver_final.dim_tempo 
    WHERE nk_tempo >= CAST(REGEXP_REPLACE(COALESCE(NULLIF(TRIM({{mes_referencia_inicial}}), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')), '-', '') AS INT)
      AND nk_tempo <= CAST(REGEXP_REPLACE(COALESCE(NULLIF(TRIM({{mes_referencia_final}}), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')), '-', '') AS INT)
);

-- 2. Inserção Incremental Otimizada
INSERT INTO tcc_receita.silver_final.fato_mudanca_estado
WITH params AS (
    SELECT 
        COALESCE(NULLIF(TRIM({{mes_referencia_inicial}}), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM({{mes_referencia_final}}), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_fim
),
logs_matriz AS (
    SELECT 
        SPLIT_PART(l.pk, '-', 1) AS cnpj_raiz,
        l.mes_referencia,
        l.evento,
        ABS(xxhash64(CAST(REGEXP_REPLACE(l.mes_referencia, '-', '') AS INT))) AS sk_tempo,
        ABS(xxhash64(l.evento, CONCAT_WS(' -> ', l.valor_antigo, l.valor_novo), COALESCE(l.informacao_adicional, ''))) AS sk_mudanca_estado
    FROM tcc_receita.silver_intermediate.log_eventos_negocio l
    CROSS JOIN params p
    WHERE l.mes_referencia >= p.mes_ini 
      AND l.mes_referencia <= p.mes_fim
      AND (l.pk NOT LIKE '%-%' OR SPLIT_PART(l.pk, '-', 2) = '0001')
),
-- Filtra dim_empresa apenas para os CNPJs que tiveram eventos na janela (reduz drasticamente o Join)
cnpjs_afetados AS (
    SELECT DISTINCT cnpj_raiz FROM logs_matriz
),
emp_filtrada AS (
    SELECT 
        c.sk_empresa, 
        c.sk_grupo_cnae, 
        c.cnpj_raiz, 
        c.inicio_vigencia, 
        COALESCE(c.fim_vigencia, '9999-12-31') AS fim_vigencia_calc
    FROM tcc_receita.silver_final.dim_empresa c
    INNER JOIN cnpjs_afetados a ON c.cnpj_raiz = a.cnpj_raiz
    WHERE c.sk_empresa != -1
),
fatos_pareados AS (
    SELECT 
        l.sk_mudanca_estado,
        COALESCE(c_before.sk_empresa, CAST(-1 AS BIGINT)) AS sk_empresa_anterior,
        c_after.sk_empresa AS sk_empresa_posterior,
        COALESCE(c_before.sk_grupo_cnae, CAST(-1 AS BIGINT)) AS sk_grupo_cnae_anterior,
        c_after.sk_grupo_cnae AS sk_grupo_cnae_posterior,
        l.sk_tempo,
        NOW() AS data_carga_etl,
        ROW_NUMBER() OVER (
            PARTITION BY l.cnpj_raiz, l.mes_referencia, l.sk_mudanca_estado 
            ORDER BY 
                CASE WHEN l.mes_referencia >= c_after.inicio_vigencia AND l.mes_referencia < c_after.fim_vigencia_calc THEN 0 ELSE 1 END,
                c_after.inicio_vigencia ASC
        ) AS rn
    FROM logs_matriz l
    INNER JOIN emp_filtrada c_after
        ON l.cnpj_raiz = c_after.cnpj_raiz
       AND (
           (l.mes_referencia >= c_after.inicio_vigencia AND l.mes_referencia < c_after.fim_vigencia_calc)
           OR (l.evento = 'EMPRESA NASCEU' AND l.mes_referencia < c_after.inicio_vigencia)
       )
    LEFT JOIN emp_filtrada c_before
        ON l.cnpj_raiz = c_before.cnpj_raiz
       AND l.mes_referencia = c_before.fim_vigencia_calc
)
SELECT 
    sk_mudanca_estado,
    sk_empresa_anterior,
    sk_empresa_posterior,
    sk_grupo_cnae_anterior,
    sk_grupo_cnae_posterior,
    sk_tempo,
    data_carga_etl
FROM fatos_pareados
WHERE rn = 1;