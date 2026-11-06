CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_grupo_cnae (
    sk_grupo_cnae BIGINT COMMENT 'Chave substituta (Hash 64-bit positivo sobre nk_grupo_cnae). -1 para Não Aplicável.',
    nk_grupo_cnae STRING COMMENT 'Chave natural canônica da combinação: cnae_primario|lista_cnaes_secundarios.',
    cnae_primario STRING COMMENT 'Código de 7 dígitos do CNAE principal da combinação.',
    lista_cnaes_secundarios STRING COMMENT 'Lista ordenada e sem duplicatas de CNAEs secundários separados por vírgula.',
    qtd_cnaes_secundarios INT COMMENT 'Quantidade de atividades econômicas secundárias na combinação.',
    possui_cnae_secundario BOOLEAN COMMENT 'Indica se a combinação possui ao menos um CNAE secundário.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na camada Silver Final.'
)
USING DELTA
COMMENT 'Camada Silver Final: Dimensão atemporal (Outrigger) de combinações únicas de CNAEs primários e secundários.';

INSERT OVERWRITE tcc_receita.silver_final.dim_grupo_cnae
WITH combinacoes_distintas AS (
    SELECT DISTINCT
        NULLIF(TRIM(cnae_fiscal_principal), '') AS cnae_primario,
        NULLIF(
            ARRAY_JOIN(
                ARRAY_SORT(
                    ARRAY_DISTINCT(
                        FILTER(
                            TRANSFORM(SPLIT(COALESCE(TRIM(cnae_fiscal_secundaria), ''), ','), x -> TRIM(x)),
                            x -> x != ''
                        )
                    )
                ),
                ','
            ),
            ''
        ) AS lista_cnaes_secundarios
    FROM tcc_receita.silver_intermediate.estabelecimentos
    WHERE identificador_matriz_filial = '1'
),
grupos_canonicos AS (
    SELECT 
        CONCAT_WS('|', COALESCE(cnae_primario, 'SEM_PRIMARIO'), COALESCE(lista_cnaes_secundarios, 'SEM_SECUNDARIO')) AS nk_grupo_cnae,
        cnae_primario,
        lista_cnaes_secundarios,
        CASE 
            WHEN lista_cnaes_secundarios IS NULL THEN 0 
            ELSE SIZE(SPLIT(lista_cnaes_secundarios, ',')) 
        END AS qtd_cnaes_secundarios,
        (lista_cnaes_secundarios IS NOT NULL) AS possui_cnae_secundario
    FROM combinacoes_distintas
    WHERE cnae_primario IS NOT NULL OR lista_cnaes_secundarios IS NOT NULL
),
ghost_row AS (
    SELECT 
        CAST(-1 AS BIGINT) AS sk_grupo_cnae,
        'NAO_APLICAVEL' AS nk_grupo_cnae,
        '0000000' AS cnae_primario,
        CAST(NULL AS STRING) AS lista_cnaes_secundarios,
        0 AS qtd_cnaes_secundarios,
        FALSE AS possui_cnae_secundario,
        NOW() AS data_carga_etl
)
SELECT * FROM ghost_row
UNION ALL
SELECT 
    (xxhash64(nk_grupo_cnae) & 9223372036854775807) AS sk_grupo_cnae,
    nk_grupo_cnae,
    cnae_primario,
    lista_cnaes_secundarios,
    qtd_cnaes_secundarios,
    possui_cnae_secundario,
    NOW() AS data_carga_etl
FROM grupos_canonicos;