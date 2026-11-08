WITH metricas AS (
    SELECT 
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_cnae = -1 THEN 1 ELSE 0 END) AS qtd_ghost_row,
        -- Sentinelas (-1, -2) não têm codigo_cnae por definição; só linhas reais exigem o código.
        SUM(CASE WHEN sk_cnae IS NULL OR nk_cnae IS NULL OR (sk_cnae >= 0 AND codigo_cnae IS NULL) THEN 1 ELSE 0 END) AS chaves_nulas,
        SUM(CASE WHEN sk_cnae >= 0 AND NOT REGEXP_LIKE(codigo_cnae, '^[0-9]{7}$') THEN 1 ELSE 0 END) AS formato_invalido,
        SUM(CASE WHEN descricao_cnae IS NULL OR nome_secao IS NULL OR grande_setor_economico IS NULL THEN 1 ELSE 0 END) AS atributos_nulos
    FROM tcc_receita.silver_final.dim_cnae
),
colisoes_sk AS (
    SELECT COUNT(*) AS qtd_colisoes
    FROM (
        SELECT sk_cnae 
        FROM tcc_receita.silver_final.dim_cnae
        GROUP BY sk_cnae 
        HAVING COUNT(*) > 1 OR COUNT(DISTINCT nk_cnae) > 1
    )
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 1300, CONCAT('ERRO DQ [dim_cnae]: Volumetria abaixo do esperado (', m.total_linhas, ' linhas)!')),
    ASSERT_TRUE(m.qtd_ghost_row = 1, 'ERRO DQ [dim_cnae]: Ghost row (-1) ausente ou duplicada!'),
    ASSERT_TRUE(m.chaves_nulas = 0, CONCAT('ERRO DQ [dim_cnae]: ', m.chaves_nulas, ' chaves nulas encontradas!')),
    ASSERT_TRUE(m.formato_invalido = 0, CONCAT('ERRO DQ [dim_cnae]: ', m.formato_invalido, ' códigos CNAE fora do padrão de 7 dígitos!')),
    ASSERT_TRUE(m.atributos_nulos = 0, CONCAT('ERRO DQ [dim_cnae]: ', m.atributos_nulos, ' registos com hierarquia IBGE nula!')),
    ASSERT_TRUE(c.qtd_colisoes = 0, CONCAT('ERRO DQ [dim_cnae]: ', c.qtd_colisoes, ' colisões ou duplicidades de sk_cnae detectadas!'))
FROM metricas m CROSS JOIN colisoes_sk c;