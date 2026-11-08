-- Silver Final | dim_grupo_cnae
-- A normalização de CNAE aqui é a mesma usada em dim_empresa:
-- remover caracteres não numéricos e aceitar somente códigos de sete dígitos.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_final.dim_grupo_cnae (
    sk_grupo_cnae BIGINT COMMENT 'Chave substituta hash 64-bit; -1 representa grupo não aplicável.',
    nk_grupo_cnae STRING COMMENT 'Chave natural canônica: cnae_primario|lista_cnaes_secundarios.',
    cnae_primario STRING COMMENT 'Código CNAE principal normalizado com sete dígitos.',
    lista_cnaes_secundarios STRING COMMENT 'Lista ordenada, sem duplicatas, de CNAEs secundários de sete dígitos.',
    qtd_cnaes_secundarios INT COMMENT 'Quantidade de CNAEs secundários válidos.',
    possui_cnae_secundario BOOLEAN COMMENT 'Indica se há pelo menos um CNAE secundário válido.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na Silver Final.'
)
USING DELTA
COMMENT 'Silver Final: dimensão atemporal de combinações únicas de CNAEs principal e secundários da matriz.';

INSERT OVERWRITE tcc_receita.silver_final.dim_grupo_cnae (
    sk_grupo_cnae, nk_grupo_cnae, cnae_primario,
    lista_cnaes_secundarios, qtd_cnaes_secundarios,
    possui_cnae_secundario, data_carga_etl
)
WITH origem AS (
    SELECT
        CASE
            WHEN LENGTH(REGEXP_REPLACE(TRIM(cnae_fiscal_principal), '[^0-9]', '')) = 7
            THEN REGEXP_REPLACE(TRIM(cnae_fiscal_principal), '[^0-9]', '')
        END AS cnae_primario,
        ARRAY_SORT(ARRAY_DISTINCT(FILTER(
            TRANSFORM(
                SPLIT(COALESCE(TRIM(cnae_fiscal_secundaria), ''), ','),
                x -> CASE
                    WHEN LENGTH(REGEXP_REPLACE(TRIM(x), '[^0-9]', '')) = 7
                    THEN REGEXP_REPLACE(TRIM(x), '[^0-9]', '')
                END
            ),
            x -> x IS NOT NULL
        ))) AS cnaes_secundarios_array
    FROM tcc_receita.silver_intermediate.estabelecimentos
    WHERE identificador_matriz_filial = '1'
),
combinacoes AS (
    SELECT DISTINCT
        cnae_primario,
        NULLIF(ARRAY_JOIN(cnaes_secundarios_array, ','), '') AS lista_cnaes_secundarios
    FROM origem
    WHERE cnae_primario IS NOT NULL OR SIZE(cnaes_secundarios_array) > 0
),
canonicos AS (
    SELECT
        CONCAT_WS('|', COALESCE(cnae_primario, 'SEM_PRIMARIO'),
                       COALESCE(lista_cnaes_secundarios, 'SEM_SECUNDARIO')) AS nk_grupo_cnae,
        cnae_primario,
        lista_cnaes_secundarios,
        CASE WHEN lista_cnaes_secundarios IS NULL THEN 0
             ELSE SIZE(SPLIT(lista_cnaes_secundarios, ',')) END AS qtd_cnaes_secundarios,
        lista_cnaes_secundarios IS NOT NULL AS possui_cnae_secundario
    FROM combinacoes
),
sentinela AS (
    SELECT CAST(-1 AS BIGINT) AS sk_grupo_cnae,
           '__NAO_APLICAVEL__' AS nk_grupo_cnae,
           CAST(NULL AS STRING) AS cnae_primario,
           CAST(NULL AS STRING) AS lista_cnaes_secundarios,
           0 AS qtd_cnaes_secundarios,
           FALSE AS possui_cnae_secundario,
           CURRENT_TIMESTAMP() AS data_carga_etl
)
SELECT * FROM sentinela
UNION ALL
SELECT
    (XXHASH64(nk_grupo_cnae) & 9223372036854775807) AS sk_grupo_cnae,
    nk_grupo_cnae,
    cnae_primario,
    lista_cnaes_secundarios,
    qtd_cnaes_secundarios,
    possui_cnae_secundario,
    CURRENT_TIMESTAMP() AS data_carga_etl
FROM canonicos;
