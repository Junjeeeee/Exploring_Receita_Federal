-- Silver Final | dim_cnae
-- Recria a dimensão a partir do domínio CNAE do IBGE presente na Bronze.
-- -1 = não aplicável/ausente; os códigos oficiais não encontrados em uma
-- ponte recebem -2 por meio do registro CNAE_NAO_ENCONTRADO.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_final.dim_cnae (
    sk_cnae BIGINT COMMENT 'Chave substituta hash 64-bit não negativa; -1 não aplicável; -2 CNAE não localizado no domínio.',
    nk_cnae STRING COMMENT 'Chave natural canônica: código CNAE de sete dígitos ou identificador reservado de sentinela.',
    codigo_cnae STRING COMMENT 'Código oficial da subclasse CNAE; NULL nas linhas sentinela.',
    descricao_cnae STRING COMMENT 'Descrição oficial da subclasse CNAE no IBGE.',
    codigo_secao STRING COMMENT 'Código da Seção CNAE (uma letra).',
    nome_secao STRING COMMENT 'Nome da Seção CNAE.',
    codigo_divisao STRING COMMENT 'Código da Divisão CNAE (dois dígitos).',
    nome_divisao STRING COMMENT 'Nome da Divisão CNAE.',
    codigo_grupo STRING COMMENT 'Código do Grupo CNAE (três dígitos).',
    nome_grupo STRING COMMENT 'Nome do Grupo CNAE.',
    codigo_classe STRING COMMENT 'Código da Classe CNAE (cinco dígitos).',
    nome_classe STRING COMMENT 'Nome da Classe CNAE.',
    grande_setor_economico STRING COMMENT 'Agrupamento analítico derivado da Seção CNAE; não é classificação oficial do IBGE.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na Silver Final.'
)
USING DELTA
COMMENT 'Silver Final: dimensão hierárquica das subclasses CNAE 2.0 do IBGE.';

INSERT OVERWRITE tcc_receita.silver_final.dim_cnae (
    sk_cnae, nk_cnae, codigo_cnae, descricao_cnae,
    codigo_secao, nome_secao, codigo_divisao, nome_divisao,
    codigo_grupo, nome_grupo, codigo_classe, nome_classe,
    grande_setor_economico, data_carga_etl
)
WITH origem AS (
    SELECT
        REGEXP_REPLACE(TRIM(codigo), '[^0-9]', '') AS codigo_limpo,
        UPPER(NULLIF(TRIM(descricao), '')) AS descricao_cnae,
        UPPER(NULLIF(TRIM(secao), '')) AS codigo_secao,
        UPPER(NULLIF(TRIM(nome_secao), '')) AS nome_secao,
        REGEXP_REPLACE(TRIM(divisao), '[^0-9]', '') AS divisao_limpa,
        UPPER(NULLIF(TRIM(nome_divisao), '')) AS nome_divisao,
        REGEXP_REPLACE(TRIM(grupo), '[^0-9]', '') AS grupo_limpo,
        UPPER(NULLIF(TRIM(nome_grupo), '')) AS nome_grupo,
        REGEXP_REPLACE(TRIM(classe), '[^0-9]', '') AS classe_limpa,
        UPPER(NULLIF(TRIM(nome_classe), '')) AS nome_classe,
        bronze_processed_at
    FROM tcc_receita.bronze.cnaes_ibge
    WHERE codigo IS NOT NULL
),
normalizada AS (
    SELECT
        codigo_limpo AS nk_cnae,
        descricao_cnae,
        codigo_secao,
        nome_secao,
        CASE WHEN LENGTH(divisao_limpa) = 2 THEN divisao_limpa END AS codigo_divisao,
        nome_divisao,
        CASE WHEN LENGTH(grupo_limpo) = 3 THEN grupo_limpo END AS codigo_grupo,
        nome_grupo,
        CASE WHEN LENGTH(classe_limpa) = 5 THEN classe_limpa END AS codigo_classe,
        nome_classe,
        bronze_processed_at
    FROM origem
    WHERE LENGTH(codigo_limpo) = 7
      AND codigo_limpo RLIKE '^[0-9]{7}$'
),
deduplicada AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY nk_cnae
               ORDER BY bronze_processed_at DESC NULLS LAST,
                        descricao_cnae ASC NULLS LAST,
                        codigo_secao ASC NULLS LAST,
                        nome_secao ASC NULLS LAST
           ) AS rn
    FROM normalizada
),
linhas AS (
    SELECT
        (XXHASH64(nk_cnae) & 9223372036854775807) AS sk_cnae,
        nk_cnae,
        nk_cnae AS codigo_cnae,
        descricao_cnae,
        codigo_secao,
        nome_secao,
        codigo_divisao,
        nome_divisao,
        codigo_grupo,
        nome_grupo,
        codigo_classe,
        nome_classe,
        CASE
            WHEN codigo_secao = 'A' THEN 'AGROPECUARIA'
            WHEN codigo_secao IN ('B', 'C', 'D', 'E') THEN 'INDUSTRIA'
            WHEN codigo_secao = 'F' THEN 'CONSTRUCAO'
            WHEN codigo_secao = 'G' THEN 'COMERCIO'
            WHEN codigo_secao IN ('H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U') THEN 'SERVICOS'
            ELSE 'OUTROS / NAO ESPECIFICADO'
        END AS grande_setor_economico,
        CURRENT_TIMESTAMP() AS data_carga_etl
    FROM deduplicada
    WHERE rn = 1
),
sentinelas AS (
    SELECT
        CAST(-1 AS BIGINT) AS sk_cnae,
        '__NAO_APLICAVEL__' AS nk_cnae,
        CAST(NULL AS STRING) AS codigo_cnae,
        'NAO INFORMADO / NAO APLICAVEL' AS descricao_cnae,
        CAST(NULL AS STRING) AS codigo_secao,
        'NAO ESPECIFICADO' AS nome_secao,
        CAST(NULL AS STRING) AS codigo_divisao,
        'NAO ESPECIFICADO' AS nome_divisao,
        CAST(NULL AS STRING) AS codigo_grupo,
        'NAO ESPECIFICADO' AS nome_grupo,
        CAST(NULL AS STRING) AS codigo_classe,
        'NAO ESPECIFICADO' AS nome_classe,
        'OUTROS / NAO ESPECIFICADO' AS grande_setor_economico,
        CURRENT_TIMESTAMP() AS data_carga_etl
    UNION ALL
    SELECT
        CAST(-2 AS BIGINT), '__CNAE_NAO_ENCONTRADO__', CAST(NULL AS STRING),
        'CODIGO CNAE AUSENTE NO DOMINIO IBGE', CAST(NULL AS STRING),
        'NAO APLICAVEL', CAST(NULL AS STRING), 'NAO APLICAVEL',
        CAST(NULL AS STRING), 'NAO APLICAVEL', CAST(NULL AS STRING),
        'NAO APLICAVEL', 'CODIGO NAO LOCALIZADO', CURRENT_TIMESTAMP()
)
SELECT * FROM sentinelas
UNION ALL
SELECT * FROM linhas;
