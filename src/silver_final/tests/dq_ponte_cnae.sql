WITH metricas AS (
    SELECT 
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_grupo_cnae = -1 AND sk_cnae = -1 THEN 1 ELSE 0 END) AS qtd_ghost_row,
        SUM(CASE WHEN sk_grupo_cnae IS NULL OR sk_cnae IS NULL OR nk_cnae IS NULL THEN 1 ELSE 0 END) AS chaves_nulas,
        SUM(CASE WHEN cnae_principal IS NULL THEN 1 ELSE 0 END) AS flag_nula
    FROM tcc_receita.silver_final.ponte_cnae
),
duplicadas AS (
    SELECT COUNT(*) AS qtd_pares_duplicados
    FROM (
        SELECT sk_grupo_cnae, nk_cnae 
        FROM tcc_receita.silver_final.ponte_cnae
        GROUP BY sk_grupo_cnae, nk_cnae HAVING COUNT(*) > 1
    )
),
orfaos_cnae AS (
    SELECT COUNT(*) AS qtd_orfaos_cnae
    FROM tcc_receita.silver_final.ponte_cnae p
    LEFT ANTI JOIN tcc_receita.silver_final.dim_cnae d ON p.sk_cnae = d.sk_cnae
),
orfaos_grupo AS (
    SELECT COUNT(*) AS qtd_orfaos_grupo
    FROM tcc_receita.silver_final.ponte_cnae p
    LEFT ANTI JOIN tcc_receita.silver_final.dim_grupo_cnae g ON p.sk_grupo_cnae = g.sk_grupo_cnae
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 1, 'ERRO DQ [ponte_cnae]: Tabela vazia!'),
    ASSERT_TRUE(m.qtd_ghost_row = 1, 'ERRO DQ [ponte_cnae]: Ghost row (-1, -1) ausente ou duplicada!'),
    ASSERT_TRUE(m.chaves_nulas = 0, CONCAT('ERRO DQ [ponte_cnae]: ', m.chaves_nulas, ' chaves nulas encontradas!')),
    ASSERT_TRUE(m.flag_nula = 0, 'ERRO DQ [ponte_cnae]: Coluna cnae_principal contém nulos!'),
    ASSERT_TRUE(d.qtd_pares_duplicados = 0, CONCAT('ERRO DQ [ponte_cnae]: ', d.qtd_pares_duplicados, ' pares (sk_grupo_cnae, nk_cnae) duplicados!')),
    ASSERT_TRUE(oc.qtd_orfaos_cnae = 0, CONCAT('ERRO DQ [ponte_cnae]: ', oc.qtd_orfaos_cnae, ' sk_cnae sem correspondência em dim_cnae!')),
    ASSERT_TRUE(og.qtd_orfaos_grupo = 0, CONCAT('ERRO DQ [ponte_cnae]: ', og.qtd_orfaos_grupo, ' sk_grupo_cnae sem correspondência em dim_grupo_cnae!'))
FROM metricas m 
CROSS JOIN duplicadas d 
CROSS JOIN orfaos_cnae oc 
CROSS JOIN orfaos_grupo og;