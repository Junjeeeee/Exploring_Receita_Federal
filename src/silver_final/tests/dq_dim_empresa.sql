WITH metricas AS (
    SELECT 
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_empresa = -1 THEN 1 ELSE 0 END) AS qtd_ghost_row,
        SUM(CASE WHEN sk_empresa IS NULL OR nk_empresa IS NULL THEN 1 ELSE 0 END) AS chaves_nulas,
        SUM(CASE WHEN registro_ativo = TRUE AND fim_vigencia IS NOT NULL THEN 1 ELSE 0 END) AS ativos_com_fim,
        SUM(CASE WHEN registro_ativo = FALSE AND sk_empresa != -1 AND (fim_vigencia IS NULL OR fim_vigencia <= inicio_vigencia) THEN 1 ELSE 0 END) AS inativos_invalidos,
        SUM(CASE WHEN localidade_cep IS NOT NULL AND sk_empresa != -1 THEN 1 ELSE 0 END) AS enriquecidos_cep
    FROM tcc_receita.silver_final.dim_empresa
),
duplicadas_sk AS (
    SELECT COUNT(*) AS qtd_sk_duplicadas
    FROM (
        SELECT sk_empresa FROM tcc_receita.silver_final.dim_empresa
        GROUP BY sk_empresa HAVING COUNT(*) > 1
    )
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 1, 'ERRO DQ [dim_empresa]: Tabela vazia!'),
    ASSERT_TRUE(m.qtd_ghost_row = 1, 'ERRO DQ [dim_empresa]: Ghost row (-1) ausente ou duplicada!'),
    ASSERT_TRUE(m.chaves_nulas = 0, CONCAT('ERRO DQ [dim_empresa]: ', m.chaves_nulas, ' SKs/NKs nulas!')),
    ASSERT_TRUE(m.ativos_com_fim = 0, CONCAT('ERRO DQ [dim_empresa]: ', m.ativos_com_fim, ' registos ativos com fim_vigencia preenchido!')),
    ASSERT_TRUE(m.inativos_invalidos = 0, CONCAT('ERRO DQ [dim_empresa]: ', m.inativos_invalidos, ' registos inativos com intervalo inválido!')),
    ASSERT_TRUE(m.enriquecidos_cep > 0, 'ERRO DQ [dim_empresa]: Nenhum registo foi enriquecido pela tabela de CEPs!'),
    ASSERT_TRUE(d.qtd_sk_duplicadas = 0, CONCAT('ERRO DQ [dim_empresa]: ', d.qtd_sk_duplicadas, ' SKs duplicadas!'))
FROM metricas m CROSS JOIN duplicadas_sk d;