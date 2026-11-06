CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_mudanca_estado (
    sk_mudanca_estado BIGINT COMMENT 'Chave substituta (Hash 64-bit positivo sobre nk_mudanca_estado). -1 para Não Aplicável.',
    nk_mudanca_estado STRING COMMENT 'Chave natural canônica do evento com tratamento explícito de nulos.',
    tipo_mudanca_estado STRING COMMENT 'Tipo do evento de negócio (ex: EMPRESA NASCEU, MUDOU ESTADO CADASTRAL, ENTROU NO SIMPLES).',
    valor_anterior STRING COMMENT 'Valor do atributo antes da transição (NULO quando inexistente).',
    valor_posterior STRING COMMENT 'Valor do atributo após a transição (NULO quando inexistente).',
    detalhe_mudanca_estado STRING COMMENT 'Representação legível da transição: valor_anterior -> valor_posterior.',
    descricao_adicional STRING COMMENT 'Informação complementar do evento (ex: descrição do motivo cadastral ou data).',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na camada Silver Final.'
)
USING DELTA
COMMENT 'Camada Silver Final: Catálogo de tipos e detalhes de eventos de transição de estado das matrizes.';

INSERT OVERWRITE tcc_receita.silver_final.dim_mudanca_estado
WITH logs_matriz AS (
    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(evento), ''), 'EVENTO_DESCONHECIDO') AS tipo_mudanca_estado,
        COALESCE(NULLIF(TRIM(valor_antigo), ''), 'NULO') AS valor_anterior,
        COALESCE(NULLIF(TRIM(valor_novo), ''), 'NULO') AS valor_posterior,
        CONCAT(
            COALESCE(NULLIF(TRIM(valor_antigo), ''), 'NULO'),
            ' -> ',
            COALESCE(NULLIF(TRIM(valor_novo), ''), 'NULO')
        ) AS detalhe_mudanca_estado,
        COALESCE(NULLIF(TRIM(informacao_adicional), ''), 'SEM_DETALHE_ADICIONAL') AS descricao_adicional
    FROM tcc_receita.silver_intermediate.log_eventos_negocio
    WHERE (pk NOT LIKE '%-%' AND pk NOT LIKE '%|%')
       OR SPLIT_PART(pk, '-', 2) = '0001'
       OR SPLIT_PART(pk, '|', 2) = '0001'
),
eventos_canonicos AS (
    SELECT 
        CONCAT_WS('|', tipo_mudanca_estado, valor_anterior, valor_posterior, descricao_adicional) AS nk_mudanca_estado,
        tipo_mudanca_estado,
        valor_anterior,
        valor_posterior,
        detalhe_mudanca_estado,
        NULLIF(descricao_adicional, 'SEM_DETALHE_ADICIONAL') AS descricao_adicional
    FROM logs_matriz
),
ghost_row AS (
    SELECT 
        CAST(-1 AS BIGINT) AS sk_mudanca_estado,
        'NAO_APLICAVEL' AS nk_mudanca_estado,
        'NAO APLICAVEL' AS tipo_mudanca_estado,
        'NULO' AS valor_anterior,
        'NULO' AS valor_posterior,
        'NULO -> NULO' AS detalhe_mudanca_estado,
        'REGISTO FANTASMA PARA INTEGRIDADE REFERENCIAL' AS descricao_adicional,
        NOW() AS data_carga_etl
)
SELECT * FROM ghost_row
UNION ALL
SELECT 
    (xxhash64(nk_mudanca_estado) & 9223372036854775807) AS sk_mudanca_estado,
    nk_mudanca_estado,
    tipo_mudanca_estado,
    valor_anterior,
    valor_posterior,
    detalhe_mudanca_estado,
    descricao_adicional,
    NOW() AS data_carga_etl
FROM eventos_canonicos;