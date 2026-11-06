CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.ponte_cnae (
    sk_grupo_cnae BIGINT COMMENT 'FK para dim_grupo_cnae.',
    sk_cnae BIGINT COMMENT 'FK para dim_cnae (-1 se o código histórico não constar na tabela oficial vigente).',
    nk_grupo_cnae STRING COMMENT 'Chave natural canônica do grupo de CNAEs.',
    nk_cnae STRING COMMENT 'Código de 7 dígitos do CNAE individual.',
    cnae_principal BOOLEAN COMMENT 'TRUE se o CNAE for o principal do grupo, FALSE se for secundário.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na camada Silver Final.'
)
USING DELTA
COMMENT 'Camada Silver Final: Tabela Ponte (Bridge) resolvendo a relação N:N entre Grupos de CNAE e CNAEs individuais.';

INSERT OVERWRITE tcc_receita.silver_final.ponte_cnae
WITH primarios AS (
    SELECT 
        sk_grupo_cnae,
        nk_grupo_cnae,
        LPAD(REGEXP_REPLACE(TRIM(cnae_primario), '[^0-9]', ''), 7, '0') AS nk_cnae,
        TRUE AS cnae_principal
    FROM tcc_receita.silver_final.dim_grupo_cnae
    WHERE sk_grupo_cnae != -1 
      AND cnae_primario IS NOT NULL
),
secundarios AS (
    SELECT 
        sk_grupo_cnae,
        nk_grupo_cnae,
        LPAD(REGEXP_REPLACE(TRIM(cnae_item), '[^0-9]', ''), 7, '0') AS nk_cnae,
        FALSE AS cnae_principal
    FROM tcc_receita.silver_final.dim_grupo_cnae
    LATERAL VIEW EXPLODE(SPLIT(lista_cnaes_secundarios, ',')) t AS cnae_item
    WHERE sk_grupo_cnae != -1 
      AND lista_cnaes_secundarios IS NOT NULL 
      AND TRIM(cnae_item) != ''
),
uniao AS (
    SELECT * FROM primarios
    UNION ALL
    SELECT * FROM secundarios WHERE nk_cnae IS NOT NULL AND nk_cnae != ''
),
ponte_enriquecida AS (
    SELECT /*+ BROADCAST(d) */
        u.sk_grupo_cnae,
        COALESCE(d.sk_cnae, CAST(-1 AS BIGINT)) AS sk_cnae,
        u.nk_grupo_cnae,
        u.nk_cnae,
        MAX(u.cnae_principal) AS cnae_principal,
        NOW() AS data_carga_etl
    FROM uniao u
    LEFT JOIN tcc_receita.silver_final.dim_cnae d 
        ON u.nk_cnae = d.nk_cnae
    GROUP BY u.sk_grupo_cnae, COALESCE(d.sk_cnae, CAST(-1 AS BIGINT)), u.nk_grupo_cnae, u.nk_cnae
),
ghost_row AS (
    SELECT
        CAST(-1 AS BIGINT) AS sk_grupo_cnae,
        CAST(-1 AS BIGINT) AS sk_cnae,
        'NAO_APLICAVEL' AS nk_grupo_cnae,
        '0000000' AS nk_cnae,
        FALSE AS cnae_principal,
        NOW() AS data_carga_etl
)
SELECT * FROM ghost_row
UNION ALL
SELECT * FROM ponte_enriquecida;