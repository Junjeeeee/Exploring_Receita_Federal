WITH metricas AS (
    SELECT 
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_cnae = -1 THEN 1 ELSE 0 END) AS qtd_ghost_row,
        SUM(CASE WHEN sk_cnae IS NULL OR codigo_cnae IS NULL THEN 1 ELSE 0 END) AS chaves_nulas,
        SUM(CASE WHEN descricao_cnae IS NULL OR nome_secao IS NULL OR grande_setor_economico IS NULL THEN 1 ELSE 0 END) AS atributos_nulos
    FROM tcc_receita.silver_final.dim_cnae
),
duplicadas AS (
    SELECT COUNT(*) AS qtd_sk_duplicadas
    FROM (
        SELECT sk_cnae FROM tcc_receita.silver_final.dim_cnae
        GROUP BY sk_cnae HAVING COUNT(*) > 1
    )
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 1300, CONCAT('ERRO DQ [dim_cnae]: Volumetria abaixo do esperado (', m.total_linhas, ' linhas)!')),
    ASSERT_TRUE(m.qtd_ghost_row = 1, 'ERRO DQ [dim_cnae]: Ghost row (-1) ausente ou duplicada!'),
    ASSERT_TRUE(m.chaves_nulas = 0, CONCAT('ERRO DQ [dim_cnae]: ', m.chaves_nulas, ' chaves nulas encontradas!')),
    ASSERT_TRUE(m.atributos_nulos = 0, CONCAT('ERRO DQ [dim_cnae]: ', m.atributos_nulos, ' registos com hierarquia IBGE nula!')),
    ASSERT_TRUE(d.qtd_sk_duplicadas = 0, CONCAT('ERRO DQ [dim_cnae]: ', d.qtd_sk_duplicadas, ' SKs duplicadas!'))
FROM metricas m CROSS JOIN duplicadas d;