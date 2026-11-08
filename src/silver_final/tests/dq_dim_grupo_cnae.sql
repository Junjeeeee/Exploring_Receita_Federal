WITH metricas AS (
    SELECT 
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_grupo_cnae = -1 THEN 1 ELSE 0 END) AS qtd_ghost_row,
        SUM(CASE WHEN sk_grupo_cnae IS NULL OR nk_grupo_cnae IS NULL THEN 1 ELSE 0 END) AS chaves_nulas,
        SUM(CASE WHEN qtd_cnaes_secundarios < 0 THEN 1 ELSE 0 END) AS qtd_invalida
    FROM tcc_receita.silver_final.dim_grupo_cnae
),
colisoes_sk AS (
    SELECT COUNT(*) AS qtd_colisoes
    FROM (
        SELECT sk_grupo_cnae 
        FROM tcc_receita.silver_final.dim_grupo_cnae
        GROUP BY sk_grupo_cnae 
        HAVING COUNT(*) > 1 OR COUNT(DISTINCT nk_grupo_cnae) > 1
    )
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 1, 'ERRO DQ [dim_grupo_cnae]: Tabela vazia!'),
    ASSERT_TRUE(m.qtd_ghost_row = 1, 'ERRO DQ [dim_grupo_cnae]: Ghost row (-1) ausente ou duplicada!'),
    ASSERT_TRUE(m.chaves_nulas = 0, CONCAT('ERRO DQ [dim_grupo_cnae]: ', m.chaves_nulas, ' chaves nulas!')),
    ASSERT_TRUE(m.qtd_invalida = 0, 'ERRO DQ [dim_grupo_cnae]: Contagem negativa de CNAEs secundários!'),
    ASSERT_TRUE(c.qtd_colisoes = 0, CONCAT('ERRO DQ [dim_grupo_cnae]: ', c.qtd_colisoes, ' colisões ou duplicidades de sk_grupo_cnae!'))
FROM metricas m CROSS JOIN colisoes_sk c;