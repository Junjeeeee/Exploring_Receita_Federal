WITH metricas AS (
    SELECT 
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_mudanca_estado = -1 THEN 1 ELSE 0 END) AS qtd_ghost_row,
        SUM(CASE WHEN sk_mudanca_estado IS NULL OR nk_mudanca_estado IS NULL OR tipo_mudanca_estado IS NULL OR detalhe_mudanca_estado IS NULL THEN 1 ELSE 0 END) AS chaves_nulas
    FROM tcc_receita.silver_final.dim_mudanca_estado
),
colisoes_sk AS (
    SELECT COUNT(*) AS qtd_colisoes
    FROM (
        SELECT sk_mudanca_estado 
        FROM tcc_receita.silver_final.dim_mudanca_estado
        GROUP BY sk_mudanca_estado 
        HAVING COUNT(*) > 1 OR COUNT(DISTINCT nk_mudanca_estado) > 1
    )
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 1, 'ERRO DQ [dim_mudanca_estado]: Tabela sem eventos mapeados!'),
    ASSERT_TRUE(m.qtd_ghost_row = 1, 'ERRO DQ [dim_mudanca_estado]: Ghost row (-1) ausente ou duplicada!'),
    ASSERT_TRUE(m.chaves_nulas = 0, CONCAT('ERRO DQ [dim_mudanca_estado]: ', m.chaves_nulas, ' registos com chaves/tipos nulos!')),
    ASSERT_TRUE(c.qtd_colisoes = 0, CONCAT('ERRO DQ [dim_mudanca_estado]: ', c.qtd_colisoes, ' colisões ou duplicidades de sk_mudanca_estado!'))
FROM metricas m CROSS JOIN colisoes_sk c;