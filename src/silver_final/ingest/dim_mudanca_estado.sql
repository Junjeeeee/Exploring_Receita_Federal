-- Silver Final | dim_mudanca_estado
-- Dimensão genérica de assinaturas de eventos. entidade_tipo faz parte da NK
-- para não confundir eventos iguais em granularidades diferentes.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_final.dim_mudanca_estado (
    sk_mudanca_estado BIGINT COMMENT 'Chave substituta hash 64-bit da assinatura do evento.',
    nk_mudanca_estado STRING COMMENT 'Chave natural serializada em JSON, incluindo granularidade, evento e valores.',
    entidade_tipo STRING COMMENT 'Granularidade do evento: EMPRESA, ESTABELECIMENTO, TRIBUTARIO ou SOCIO.',
    tipo_mudanca_estado STRING COMMENT 'Nome da regra/evento de negócio.',
    valor_anterior STRING COMMENT 'Valor do atributo antes da transição; NULO quando ausente.',
    valor_posterior STRING COMMENT 'Valor do atributo depois da transição; NULO quando ausente.',
    detalhe_mudanca_estado STRING COMMENT 'Representação legível: valor_anterior -> valor_posterior.',
    descricao_adicional STRING COMMENT 'Informação complementar não identificadora do evento.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na Silver Final.'
)
USING DELTA
COMMENT 'Silver Final: catálogo de assinaturas de eventos de transição de empresas, estabelecimentos, tributação e relações societárias.';

INSERT OVERWRITE tcc_receita.silver_final.dim_mudanca_estado (
    sk_mudanca_estado, nk_mudanca_estado, entidade_tipo,
    tipo_mudanca_estado, valor_anterior, valor_posterior,
    detalhe_mudanca_estado, descricao_adicional, data_carga_etl
)
WITH eventos_normalizados AS (
    SELECT DISTINCT
        COALESCE(NULLIF(UPPER(TRIM(entidade_tipo)), ''), 'ENTIDADE_DESCONHECIDA') AS entidade_tipo,
        COALESCE(NULLIF(TRIM(evento), ''), 'EVENTO_DESCONHECIDO') AS tipo_mudanca_estado,
        COALESCE(NULLIF(TRIM(valor_antigo), ''), 'NULO') AS valor_anterior,
        COALESCE(NULLIF(TRIM(valor_novo), ''), 'NULO') AS valor_posterior,
        COALESCE(NULLIF(TRIM(informacao_adicional), ''), 'SEM_DETALHE_ADICIONAL') AS descricao_adicional_norm
    FROM tcc_receita.silver_intermediate.log_eventos_negocio
),
canonicos AS (
    SELECT
        TO_JSON(NAMED_STRUCT(
            'entidade_tipo', entidade_tipo,
            'evento', tipo_mudanca_estado,
            'valor_anterior', valor_anterior,
            'valor_posterior', valor_posterior,
            'informacao_adicional', descricao_adicional_norm
        )) AS nk_mudanca_estado,
        entidade_tipo,
        tipo_mudanca_estado,
        valor_anterior,
        valor_posterior,
        CONCAT(valor_anterior, ' -> ', valor_posterior) AS detalhe_mudanca_estado,
        CASE WHEN descricao_adicional_norm = 'SEM_DETALHE_ADICIONAL'
             THEN CAST(NULL AS STRING) ELSE descricao_adicional_norm END AS descricao_adicional
    FROM eventos_normalizados
),
sentinela AS (
    SELECT
        CAST(-1 AS BIGINT) AS sk_mudanca_estado,
        '__NAO_APLICAVEL__' AS nk_mudanca_estado,
        'NAO_APLICAVEL' AS entidade_tipo,
        'NAO APLICAVEL' AS tipo_mudanca_estado,
        'NULO' AS valor_anterior,
        'NULO' AS valor_posterior,
        'NULO -> NULO' AS detalhe_mudanca_estado,
        'REGISTRO FANTASMA PARA INTEGRIDADE REFERENCIAL' AS descricao_adicional,
        CURRENT_TIMESTAMP() AS data_carga_etl
)
SELECT * FROM sentinela
UNION ALL
SELECT
    (XXHASH64(nk_mudanca_estado) & 9223372036854775807) AS sk_mudanca_estado,
    nk_mudanca_estado,
    entidade_tipo,
    tipo_mudanca_estado,
    valor_anterior,
    valor_posterior,
    detalhe_mudanca_estado,
    descricao_adicional,
    CURRENT_TIMESTAMP() AS data_carga_etl
FROM canonicos;
