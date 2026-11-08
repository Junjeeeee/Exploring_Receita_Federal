-- Silver Final | ponte_cnae
-- -1 na dim_cnae = não aplicável; -2 = CNAE válido informado, mas ausente do domínio IBGE.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_final.ponte_cnae (
    sk_grupo_cnae BIGINT COMMENT 'FK para dim_grupo_cnae.',
    sk_cnae BIGINT COMMENT 'FK para dim_cnae; -2 quando o código não existe no domínio IBGE; -1 não aplicável.',
    nk_grupo_cnae STRING COMMENT 'Chave natural canônica do grupo de CNAEs.',
    nk_cnae STRING COMMENT 'Código CNAE individual normalizado com sete dígitos.',
    cnae_principal BOOLEAN COMMENT 'TRUE se o CNAE é principal; TRUE prevalece se também estiver na lista secundária.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na Silver Final.'
)
USING DELTA
COMMENT 'Silver Final: ponte N:N entre combinações de CNAEs e CNAEs individuais.';

INSERT OVERWRITE tcc_receita.silver_final.ponte_cnae (
    sk_grupo_cnae, sk_cnae, nk_grupo_cnae, nk_cnae,
    cnae_principal, data_carga_etl
)
WITH primarios AS (
    SELECT
        sk_grupo_cnae,
        nk_grupo_cnae,
        cnae_primario AS nk_cnae,
        TRUE AS cnae_principal
    FROM tcc_receita.silver_final.dim_grupo_cnae
    WHERE sk_grupo_cnae <> -1
      AND cnae_primario RLIKE '^[0-9]{7}$'
),
secundarios AS (
    SELECT
        g.sk_grupo_cnae,
        g.nk_grupo_cnae,
        TRIM(item.cnae_item) AS nk_cnae,
        FALSE AS cnae_principal
    FROM tcc_receita.silver_final.dim_grupo_cnae g
    LATERAL VIEW EXPLODE(SPLIT(g.lista_cnaes_secundarios, ',')) item AS cnae_item
    WHERE g.sk_grupo_cnae <> -1
      AND g.lista_cnaes_secundarios IS NOT NULL
      AND TRIM(item.cnae_item) RLIKE '^[0-9]{7}$'
),
relacoes AS (
    SELECT * FROM primarios
    UNION ALL
    SELECT * FROM secundarios
),
consolidadas AS (
    SELECT
        r.sk_grupo_cnae,
        COALESCE(c.sk_cnae, CAST(-2 AS BIGINT)) AS sk_cnae,
        r.nk_grupo_cnae,
        r.nk_cnae,
        MAX(CAST(r.cnae_principal AS INT)) = 1 AS cnae_principal
    FROM relacoes r
    LEFT JOIN tcc_receita.silver_final.dim_cnae c
        ON r.nk_cnae = c.nk_cnae
    GROUP BY r.sk_grupo_cnae, COALESCE(c.sk_cnae, CAST(-2 AS BIGINT)),
             r.nk_grupo_cnae, r.nk_cnae
),
sentinela AS (
    SELECT CAST(-1 AS BIGINT) AS sk_grupo_cnae,
           CAST(-1 AS BIGINT) AS sk_cnae,
           '__NAO_APLICAVEL__' AS nk_grupo_cnae,
           '__NAO_APLICAVEL__' AS nk_cnae,
           FALSE AS cnae_principal,
           CURRENT_TIMESTAMP() AS data_carga_etl
)
SELECT * FROM sentinela
UNION ALL
SELECT
    sk_grupo_cnae, sk_cnae, nk_grupo_cnae, nk_cnae,
    cnae_principal, CURRENT_TIMESTAMP() AS data_carga_etl
FROM consolidadas;
