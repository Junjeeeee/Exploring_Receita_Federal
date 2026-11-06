CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_final.fato_mudanca_estado (
    sk_mudanca_estado BIGINT COMMENT 'FK para dim_mudanca_estado.',
    sk_empresa_anterior BIGINT COMMENT 'FK para dim_empresa representando o estado imediatamente anterior ao evento (-1 para EMPRESA NASCEU ou sem histórico prévio).',
    sk_empresa_posterior BIGINT COMMENT 'FK para dim_empresa representando o estado vigente a partir do evento (-1 se não encontrado).',
    sk_grupo_cnae_anterior BIGINT COMMENT 'FK para dim_grupo_cnae anterior ao evento (-1 se inexistente).',
    sk_grupo_cnae_posterior BIGINT COMMENT 'FK para dim_grupo_cnae vigente a partir do evento (-1 se inexistente).',
    sk_tempo BIGINT COMMENT 'FK para dim_tempo correspondente ao mês do evento.',
    cnpj_raiz STRING COMMENT 'Dimensão degenerada: radical de 8 dígitos do CNPJ para auditoria e contagem distinta de empresas.',
    mes_referencia STRING COMMENT 'Partição: mês de ocorrência do evento (YYYY-MM).',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na camada Silver Final.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Silver Final: Tabela Fato de Eventos de Mudança de Estado (pareando as versões anterior e posterior da empresa no SCD2).';

-- Reescrita atômica por partição (não depende de dim_tempo para deletar e garante rollback integral em caso de falha)
INSERT OVERWRITE tcc_receita.silver_final.fato_mudanca_estado
REPLACE WHERE mes_referencia >= COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM'))
          AND mes_referencia <= COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM'))
WITH params AS (
    SELECT 
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_fim
),
logs_matriz AS (
    SELECT 
        REGEXP_EXTRACT(l.pk, '^([0-9]{8})', 1) AS cnpj_raiz,
        l.mes_referencia,
        COALESCE(NULLIF(TRIM(l.evento), ''), 'EVENTO_DESCONHECIDO') AS evento,
        (xxhash64(CAST(REGEXP_REPLACE(l.mes_referencia, '-', '') AS INT)) & 9223372036854775807) AS sk_tempo,
        (xxhash64(
            CONCAT_WS('|',
                COALESCE(NULLIF(TRIM(l.evento), ''), 'EVENTO_DESCONHECIDO'),
                COALESCE(NULLIF(TRIM(l.valor_antigo), ''), 'NULO'),
                COALESCE(NULLIF(TRIM(l.valor_novo), ''), 'NULO'),
                COALESCE(NULLIF(TRIM(l.informacao_adicional), ''), 'SEM_DETALHE_ADICIONAL')
            )
        ) & 9223372036854775807) AS sk_mudanca_estado
    FROM tcc_receita.silver_intermediate.log_eventos_negocio l
    CROSS JOIN params p
    WHERE l.mes_referencia >= p.mes_ini 
      AND l.mes_referencia <= p.mes_fim
      AND (
          (l.pk NOT LIKE '%-%' AND l.pk NOT LIKE '%|%')
          OR SPLIT_PART(l.pk, '-', 2) = '0001'
          OR SPLIT_PART(l.pk, '|', 2) = '0001'
      )
),
cnpjs_afetados AS (
    SELECT DISTINCT cnpj_raiz FROM logs_matriz WHERE cnpj_raiz IS NOT NULL AND cnpj_raiz != ''
),
-- Prepara o histórico SCD2 de dim_empresa já encadeado com a versão anterior de cada fatia temporal
emp_historico AS (
    SELECT 
        c.sk_empresa, 
        c.sk_grupo_cnae, 
        c.cnpj_raiz, 
        c.inicio_vigencia, 
        COALESCE(c.fim_vigencia, '9999-12') AS fim_vigencia_calc,
        LAG(c.sk_empresa) OVER (PARTITION BY c.cnpj_raiz ORDER BY c.inicio_vigencia ASC) AS prev_sk_empresa,
        LAG(c.sk_grupo_cnae) OVER (PARTITION BY c.cnpj_raiz ORDER BY c.inicio_vigencia ASC) AS prev_sk_grupo_cnae,
        ROW_NUMBER() OVER (PARTITION BY c.cnpj_raiz ORDER BY c.inicio_vigencia ASC) AS ordem_versao
    FROM tcc_receita.silver_final.dim_empresa c
    INNER JOIN cnpjs_afetados a ON c.cnpj_raiz = a.cnpj_raiz
    WHERE c.sk_empresa != -1
),
-- 1. Eventos de Nascimento: Estado anterior é sempre -1; Estado posterior é a versão vigente no mês (ou a 1ª versão conhecida da matriz)
fatos_nascimento AS (
    SELECT 
        l.sk_mudanca_estado,
        CAST(-1 AS BIGINT) AS sk_empresa_anterior,
        COALESCE(c_after.sk_empresa, CAST(-1 AS BIGINT)) AS sk_empresa_posterior,
        CAST(-1 AS BIGINT) AS sk_grupo_cnae_anterior,
        COALESCE(c_after.sk_grupo_cnae, CAST(-1 AS BIGINT)) AS sk_grupo_cnae_posterior,
        l.sk_tempo,
        l.cnpj_raiz,
        l.mes_referencia,
        NOW() AS data_carga_etl,
        ROW_NUMBER() OVER (
            PARTITION BY l.cnpj_raiz, l.mes_referencia, l.sk_mudanca_estado
            ORDER BY 
                CASE WHEN l.mes_referencia >= c_after.inicio_vigencia AND l.mes_referencia < c_after.fim_vigencia_calc THEN 0 ELSE 1 END,
                c_after.inicio_vigencia ASC
        ) AS rn
    FROM logs_matriz l
    LEFT JOIN emp_historico c_after
        ON l.cnpj_raiz = c_after.cnpj_raiz
       AND (
           (l.mes_referencia >= c_after.inicio_vigencia AND l.mes_referencia < c_after.fim_vigencia_calc)
           OR c_after.ordem_versao = 1
       )
    WHERE l.evento = 'EMPRESA NASCEU'
),
-- 2. Eventos de Transição Cadastral/Tributária: Pareamento exato no intervalo semiaberto [inicio_vigencia, fim_vigencia_calc)
fatos_transicao AS (
    SELECT 
        l.sk_mudanca_estado,
        COALESCE(c_after.prev_sk_empresa, c_before_fallback.sk_empresa, CAST(-1 AS BIGINT)) AS sk_empresa_anterior,
        COALESCE(c_after.sk_empresa, CAST(-1 AS BIGINT)) AS sk_empresa_posterior,
        COALESCE(c_after.prev_sk_grupo_cnae, c_before_fallback.sk_grupo_cnae, CAST(-1 AS BIGINT)) AS sk_grupo_cnae_anterior,
        COALESCE(c_after.sk_grupo_cnae, CAST(-1 AS BIGINT)) AS sk_grupo_cnae_posterior,
        l.sk_tempo,
        l.cnpj_raiz,
        l.mes_referencia,
        NOW() AS data_carga_etl,
        ROW_NUMBER() OVER (
            PARTITION BY l.cnpj_raiz, l.mes_referencia, l.sk_mudanca_estado
            ORDER BY 
                CASE WHEN c_after.inicio_vigencia = l.mes_referencia THEN 0 ELSE 1 END,
                c_after.inicio_vigencia DESC
        ) AS rn
    FROM logs_matriz l
    LEFT JOIN emp_historico c_after
        ON l.cnpj_raiz = c_after.cnpj_raiz
       AND l.mes_referencia >= c_after.inicio_vigencia 
       AND l.mes_referencia < c_after.fim_vigencia_calc
    LEFT JOIN emp_historico c_before_fallback
        ON l.cnpj_raiz = c_before_fallback.cnpj_raiz
       AND l.mes_referencia = c_before_fallback.fim_vigencia_calc
    WHERE l.evento != 'EMPRESA NASCEU'
),
uniao_fatos AS (
    SELECT * FROM fatos_nascimento WHERE rn = 1
    UNION ALL
    SELECT * FROM fatos_transicao WHERE rn = 1
)
SELECT 
    sk_mudanca_estado,
    sk_empresa_anterior,
    sk_empresa_posterior,
    sk_grupo_cnae_anterior,
    sk_grupo_cnae_posterior,
    sk_tempo,
    cnpj_raiz,
    mes_referencia,
    data_carga_etl
FROM uniao_fatos;