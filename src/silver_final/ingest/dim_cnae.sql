CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_cnae (
    sk_cnae BIGINT COMMENT 'Chave substituta (Hash 64-bit positivo sobre nk_cnae). -1 para Não Aplicável.',
    nk_cnae STRING COMMENT 'Chave natural: código numérico de 7 dígitos da subclasse CNAE.',
    codigo_cnae STRING COMMENT 'Código numérico de 7 dígitos da subclasse CNAE.',
    descricao_cnae STRING COMMENT 'Descrição oficial da subclasse CNAE no IBGE.',
    codigo_secao STRING COMMENT 'Código da Seção CNAE (1 letra).',
    nome_secao STRING COMMENT 'Descrição da Seção CNAE.',
    codigo_divisao STRING COMMENT 'Código da Divisão CNAE (2 dígitos).',
    nome_divisao STRING COMMENT 'Descrição da Divisão CNAE.',
    codigo_grupo STRING COMMENT 'Código do Grupo CNAE (3 dígitos).',
    nome_grupo STRING COMMENT 'Descrição do Grupo CNAE.',
    codigo_classe STRING COMMENT 'Código da Classe CNAE (5 dígitos).',
    nome_classe STRING COMMENT 'Descrição da Classe CNAE.',
    grande_setor_economico STRING COMMENT 'Classificação analítica derivada da Seção CNAE para agregação macroeconômica (não oficial IBGE).',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na camada Silver Final.'
)
USING DELTA
COMMENT 'Camada Silver Final: Dimensão hierárquica de atividades econômicas (CNAE 2.0 IBGE).';

INSERT OVERWRITE tcc_receita.silver_final.dim_cnae
WITH cnaes_dedup AS (
    SELECT 
        LPAD(REGEXP_REPLACE(TRIM(codigo), '[^0-9]', ''), 7, '0') AS nk_cnae,
        UPPER(TRIM(descricao)) AS descricao_cnae,
        UPPER(TRIM(secao)) AS codigo_secao,
        UPPER(TRIM(nome_secao)) AS nome_secao,
        LPAD(REGEXP_REPLACE(TRIM(divisao), '[^0-9]', ''), 2, '0') AS codigo_divisao,
        UPPER(TRIM(nome_divisao)) AS nome_divisao,
        LPAD(REGEXP_REPLACE(TRIM(grupo), '[^0-9]', ''), 3, '0') AS codigo_grupo,
        UPPER(TRIM(nome_grupo)) AS nome_grupo,
        LPAD(REGEXP_REPLACE(TRIM(classe), '[^0-9]', ''), 5, '0') AS codigo_classe,
        UPPER(TRIM(nome_classe)) AS nome_classe,
        ROW_NUMBER() OVER (
            PARTITION BY LPAD(REGEXP_REPLACE(TRIM(codigo), '[^0-9]', ''), 7, '0')
            ORDER BY bronze_processed_at DESC, descricao ASC
        ) AS rn
    FROM tcc_receita.bronze.cnaes_ibge
    WHERE codigo IS NOT NULL 
      AND REGEXP_REPLACE(TRIM(codigo), '[^0-9]', '') != ''
),
ghost_row AS (
    SELECT 
        CAST(-1 AS BIGINT) AS sk_cnae,
        '0000000' AS nk_cnae,
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
)
SELECT * FROM ghost_row
UNION ALL
SELECT 
    (xxhash64(nk_cnae) & 9223372036854775807) AS sk_cnae,
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
    NOW() AS data_carga_etl
FROM cnaes_dedup
WHERE rn = 1 
  AND nk_cnae != '0000000'
  AND REGEXP_LIKE(nk_cnae, '^[0-9]{7}$');