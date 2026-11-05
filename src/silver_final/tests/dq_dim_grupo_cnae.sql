WITH metricas AS (
    SELECT 
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_grupo_cnae = -1 THEN 1 ELSE 0 END) AS qtd_ghost_row,
        SUM(CASE WHEN sk_grupo_cnae IS NULL OR nk_grupo_cnae IS NULL THEN 1 ELSE 0 END) AS chaves_nulas,
        SUM(CASE WHEN registro_ativo = TRUE AND fim_vigencia IS NOT NULL THEN 1 ELSE 0 END) AS ativos_com_fim,
        SUM(CASE WHEN registro_ativo = FALSE AND sk_grupo_cnae != -1 AND (fim_vigencia IS NULL OR fim_vigencia <= inicio_vigencia) THEN 1 ELSE 0 END) AS inativos_invalidos
    FROM tcc_receita.silver_final.dim_grupo_cnae
),
duplicadas AS (
    SELECT COUNT(*) AS qtd_sk_duplicadas
    FROM (
        SELECT sk_grupo_cnae FROM tcc_receita.silver_final.dim_grupo_cnae
        GROUP BY sk_grupo_cnae HAVING COUNT(*) > 1
    )
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 1, 'ERRO DQ [dim_grupo_cnae]: Tabela vazia!'),
    ASSERT_TRUE(m.qtd_ghost_row = 1, 'ERRO DQ [dim_grupo_cnae]: Ghost row (-1) ausente ou duplicada!'),
    ASSERT_TRUE(m.chaves_nulas = 0, CONCAT('ERRO DQ [dim_grupo_cnae]: ', m.chaves_nulas, ' chaves nulas!')),
    ASSERT_TRUE(m.ativos_com_fim = 0, CONCAT('ERRO DQ [dim_grupo_cnae]: ', m.ativos_com_fim, ' grupos ativos com fim_vigencia preenchido!')),
    ASSERT_TRUE(m.inativos_invalidos = 0, CONCAT('ERRO DQ [dim_grupo_cnae]: ', m.inativos_invalidos, ' grupos inativos com datas inválidas!')),
    ASSERT_TRUE(d.qtd_sk_duplicadas = 0, CONCAT('ERRO DQ [dim_grupo_cnae]: ', d.qtd_sk_duplicadas, ' SKs duplicadas!'))
FROM metricas m CROSS JOIN duplicadas d;