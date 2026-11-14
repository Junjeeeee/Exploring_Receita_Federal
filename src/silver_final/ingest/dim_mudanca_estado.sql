-- Silver Final | dim_mudanca_estado
-- Dimensão genérica de assinaturas de eventos. entidade_tipo faz parte da NK
-- para não confundir eventos iguais em granularidades diferentes.
-- codigo_motivo (motivo da situação cadastral) também é parte da NK, e categoria_evento
-- concentra a regra de negócio de classificação (spec 2026-11-14, seção 2.3).
-- Carga é rebuild total (INSERT OVERWRITE); CREATE OR REPLACE porque o schema mudou.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_mudanca_estado (
    sk_mudanca_estado BIGINT COMMENT 'Chave substituta hash 64-bit da assinatura do evento.',
    nk_mudanca_estado STRING COMMENT 'Chave natural serializada em JSON, incluindo granularidade, evento e valores.',
    entidade_tipo STRING COMMENT 'Granularidade do evento: EMPRESA, ESTABELECIMENTO, TRIBUTARIO ou SOCIO.',
    tipo_mudanca_estado STRING COMMENT 'Nome da regra/evento de negócio.',
    valor_anterior STRING COMMENT 'Valor do atributo antes da transição; NULO quando ausente.',
    valor_posterior STRING COMMENT 'Valor do atributo depois da transição; NULO quando ausente.',
    detalhe_mudanca_estado STRING COMMENT 'Representação legível: valor_anterior -> valor_posterior.',
    descricao_adicional STRING COMMENT 'Informação complementar não identificadora do evento.',
    codigo_motivo STRING COMMENT 'Código do motivo da situação cadastral (tabela de motivos da Receita); NULL quando o evento não tem motivo. Faz parte da chave natural.',
    categoria_evento STRING COMMENT 'Categoria para dashboards. Em MUDOU ESTADO CADASTRAL, deriva de (valor_anterior, valor_posterior, codigo_motivo); EMPRESA NASCEU vira ABERTURA; os demais eventos repetem o próprio nome.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na Silver Final.'
)
USING DELTA
COMMENT 'Silver Final: catálogo de assinaturas de eventos de transição de empresas, estabelecimentos, tributação e relações societárias.';

INSERT OVERWRITE tcc_receita.silver_final.dim_mudanca_estado (
    sk_mudanca_estado, nk_mudanca_estado, entidade_tipo,
    tipo_mudanca_estado, valor_anterior, valor_posterior,
    detalhe_mudanca_estado, descricao_adicional,
    codigo_motivo, categoria_evento, data_carga_etl
)
WITH eventos_normalizados AS (
    SELECT DISTINCT
        COALESCE(NULLIF(UPPER(TRIM(entidade_tipo)), ''), 'ENTIDADE_DESCONHECIDA') AS entidade_tipo,
        COALESCE(NULLIF(TRIM(evento), ''), 'EVENTO_DESCONHECIDO') AS tipo_mudanca_estado,
        COALESCE(NULLIF(TRIM(valor_antigo), ''), 'NULO') AS valor_anterior,
        COALESCE(NULLIF(TRIM(valor_novo), ''), 'NULO') AS valor_posterior,
        COALESCE(NULLIF(TRIM(informacao_adicional), ''), 'SEM_DETALHE_ADICIONAL') AS descricao_adicional_norm,
        NULLIF(TRIM(codigo_motivo), '') AS codigo_motivo  -- NULL = evento sem motivo
    FROM tcc_receita.silver_intermediate.log_eventos_negocio
),
canonicos AS (
    SELECT
        TO_JSON(NAMED_STRUCT(
            'entidade_tipo', entidade_tipo,
            'evento', tipo_mudanca_estado,
            'valor_anterior', valor_anterior,
            'valor_posterior', valor_posterior,
            'informacao_adicional', descricao_adicional_norm,
            'codigo_motivo', COALESCE(codigo_motivo, 'SEM_MOTIVO')
        )) AS nk_mudanca_estado,
        entidade_tipo,
        tipo_mudanca_estado,
        valor_anterior,
        valor_posterior,
        CONCAT(valor_anterior, ' -> ', valor_posterior) AS detalhe_mudanca_estado,
        CASE WHEN descricao_adicional_norm = 'SEM_DETALHE_ADICIONAL'
             THEN CAST(NULL AS STRING) ELSE descricao_adicional_norm END AS descricao_adicional,
        codigo_motivo,
        CASE
            WHEN tipo_mudanca_estado = 'EMPRESA NASCEU' THEN 'ABERTURA'
            WHEN tipo_mudanca_estado = 'MUDOU ESTADO CADASTRAL' THEN
                -- Ordem importa: vale a primeira condição que casar (valor_anterior já vem 'NULO' se ausente).
                CASE
                    -- Situação igual, só o motivo mudou (ex.: BAIXADA 71 -> BAIXADA 05): não é baixa nova.
                    WHEN valor_anterior = valor_posterior THEN 'MUDANCA DE MOTIVO'
                    WHEN valor_posterior = 'ATIVA' THEN 'REATIVACAO'
                    WHEN valor_posterior = 'BAIXADA' AND codigo_motivo IN ('05', '06', '10') THEN 'FALENCIA OU LIQUIDACAO JUDICIAL'
                    WHEN valor_posterior = 'BAIXADA' AND codigo_motivo = '75' THEN 'EXTINCAO POR OBITO (MEI)'
                    WHEN valor_posterior = 'BAIXADA' AND codigo_motivo IN ('02', '03', '04') THEN 'REORGANIZACAO SOCIETARIA'
                    WHEN valor_posterior = 'BAIXADA' AND codigo_motivo IN ('01', '37', '54') THEN 'BAIXA VOLUNTARIA'
                    WHEN valor_posterior = 'BAIXADA' AND codigo_motivo IN ('13', '14', '15', '63', '64', '66', '71', '73') THEN 'BAIXA DE OFICIO'
                    WHEN valor_posterior = 'BAIXADA' THEN 'BAIXA - OUTROS MOTIVOS'
                    WHEN valor_posterior = 'INAPTA' THEN 'INAPTIDAO'
                    WHEN valor_posterior = 'SUSPENSA' THEN 'SUSPENSAO'
                    WHEN valor_posterior = 'NULA' THEN 'ANULACAO'
                    ELSE 'OUTRA MUDANCA CADASTRAL'
                END
            ELSE tipo_mudanca_estado
        END AS categoria_evento
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
        CAST(NULL AS STRING) AS codigo_motivo,
        'NAO APLICAVEL' AS categoria_evento,
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
    codigo_motivo,
    categoria_evento,
    CURRENT_TIMESTAMP() AS data_carga_etl
FROM canonicos;
