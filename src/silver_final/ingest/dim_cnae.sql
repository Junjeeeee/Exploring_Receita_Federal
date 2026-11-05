CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

-- 1. Catálogo Hierárquico Completo de CNAEs (IBGE)
CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_cnae AS
SELECT 
    CAST(-1 AS BIGINT) AS sk_cnae,
    '0000000' AS codigo_cnae,
    'NAO INFORMADO / NAO APLICAVEL' AS descricao_cnae,
    'Z' AS codigo_secao,
    'NAO ESPECIFICADO' AS nome_secao,
    '00' AS codigo_divisao,
    'NAO ESPECIFICADO' AS nome_divisao,
    '000' AS codigo_grupo,
    'NAO ESPECIFICADO' AS nome_grupo,
    '00000' AS codigo_classe,
    'NAO ESPECIFICADO' AS nome_classe,
    'OUTROS / NAO ESPECIFICADO' AS grande_setor_economico,
    NOW() AS data_carga_etl
UNION ALL
SELECT 
    ABS(xxhash64(TRIM(codigo))) AS sk_cnae,
    TRIM(codigo) AS codigo_cnae,
    MAX(UPPER(TRIM(descricao))) AS descricao_cnae,
    MAX(UPPER(TRIM(secao))) AS codigo_secao,
    MAX(UPPER(TRIM(nome_secao))) AS nome_secao,
    MAX(UPPER(TRIM(divisao))) AS codigo_divisao,
    MAX(UPPER(TRIM(nome_divisao))) AS nome_divisao,
    MAX(UPPER(TRIM(grupo))) AS codigo_grupo,
    MAX(UPPER(TRIM(nome_grupo))) AS nome_grupo,
    MAX(UPPER(TRIM(classe))) AS codigo_classe,
    MAX(UPPER(TRIM(nome_classe))) AS nome_classe,
    CASE 
        WHEN MAX(UPPER(TRIM(secao))) = 'A' THEN 'AGROPECUARIA'
        WHEN MAX(UPPER(TRIM(secao))) IN ('B', 'C', 'D', 'E') THEN 'INDUSTRIA'
        WHEN MAX(UPPER(TRIM(secao))) = 'F' THEN 'CONSTRUCAO'
        WHEN MAX(UPPER(TRIM(secao))) = 'G' THEN 'COMERCIO'
        WHEN MAX(UPPER(TRIM(secao))) IN ('H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U') THEN 'SERVICOS'
        ELSE 'OUTROS / NAO ESPECIFICADO'
    END AS grande_setor_economico,
    NOW() AS data_carga_etl
FROM tcc_receita.bronze.cnaes_ibge
WHERE codigo IS NOT NULL AND TRIM(codigo) != ''
GROUP BY TRIM(codigo);

-- 2. Dimensão de Grupo de CNAEs (Resolve relacionamento N:N sem Fan Trap)
CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_grupo_cnae AS
WITH combinacoes AS (
    SELECT 
        COALESCE(cnae_fiscal_principal, '') AS cnae_primario,
        COALESCE(cnae_fiscal_secundaria, '') AS cnae_secundario,
        MIN(valid_since) AS inicio_vigencia,
        CASE WHEN MAX(is_current) = TRUE THEN CAST(NULL AS STRING) ELSE MAX(valid_until) END AS fim_vigencia,
        MAX(is_current) AS registro_ativo
    FROM tcc_receita.silver_intermediate.estabelecimentos
    WHERE identificador_matriz_filial = '1'
    GROUP BY 1, 2
)
SELECT
    CAST(-1 AS BIGINT) AS sk_grupo_cnae,
    '0000000' AS cnae_primario,
    CAST(NULL AS STRING) AS lista_cnaes_secundarios,
    0 AS qtd_cnaes_secundarios,
    '1900-01' AS inicio_vigencia,
    CAST(NULL AS STRING) AS fim_vigencia,
    FALSE AS registro_ativo,
    NOW() AS data_carga_etl
UNION ALL
SELECT 
    ABS(xxhash64(cnae_primario, cnae_secundario)) AS sk_grupo_cnae,
    NULLIF(cnae_primario, '') AS cnae_primario,
    NULLIF(cnae_secundario, '') AS lista_cnaes_secundarios,
    CASE WHEN cnae_secundario = '' THEN 0 ELSE SIZE(SPLIT(cnae_secundario, ',')) END AS qtd_cnaes_secundarios,
    inicio_vigencia,
    fim_vigencia,
    registro_ativo,
    NOW() AS data_carga_etl
FROM combinacoes;

-- 3. Tabela Ponte (Bridge Table: Grupo CNAE <-> CNAE Individual)
CREATE OR REPLACE TABLE tcc_receita.silver_final.ponte_grupo_cnae AS
WITH primarios AS (
    SELECT 
        sk_grupo_cnae,
        ABS(xxhash64(TRIM(cnae_primario))) AS sk_cnae,
        TRIM(cnae_primario) AS codigo_cnae,
        'PRIMARIO' AS tipo_cnae
    FROM tcc_receita.silver_final.dim_grupo_cnae
    WHERE sk_grupo_cnae != -1 AND cnae_primario IS NOT NULL
),
secundarios AS (
    SELECT 
        sk_grupo_cnae,
        ABS(xxhash64(TRIM(cod_sec))) AS sk_cnae,
        TRIM(cod_sec) AS codigo_cnae,
        'SECUNDARIO' AS tipo_cnae
    FROM tcc_receita.silver_final.dim_grupo_cnae
    LATERAL VIEW EXPLODE(SPLIT(lista_cnaes_secundarios, ',')) t AS cod_sec
    WHERE sk_grupo_cnae != -1 AND lista_cnaes_secundarios IS NOT NULL AND TRIM(cod_sec) != ''
)
SELECT * FROM primarios
UNION ALL
SELECT * FROM secundarios;