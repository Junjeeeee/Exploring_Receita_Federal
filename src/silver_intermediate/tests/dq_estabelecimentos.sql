WITH metricas AS (
    SELECT 
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN pk IS NULL OR TRIM(pk) = '' THEN 1 ELSE 0 END) AS pks_nulas,
        SUM(CASE WHEN change_hash IS NULL THEN 1 ELSE 0 END) AS hashes_nulos,
        SUM(CASE WHEN is_current = TRUE AND valid_until IS NOT NULL THEN 1 ELSE 0 END) AS ativos_com_fim,
        SUM(CASE WHEN is_current = FALSE AND (valid_until IS NULL OR valid_until <= valid_since) THEN 1 ELSE 0 END) AS inativos_invalidos
    FROM tcc_receita.silver_intermediate.estabelecimentos
),
duplicadas_ativas AS (
    SELECT COUNT(*) AS qtd_duplicadas
    FROM (
        SELECT pk FROM tcc_receita.silver_intermediate.estabelecimentos
        WHERE is_current = TRUE GROUP BY pk HAVING COUNT(*) > 1
    )
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 0, 'ERRO DQ [estabelecimentos]: Tabela vazia!'),
    ASSERT_TRUE(m.pks_nulas = 0, CONCAT('ERRO DQ [estabelecimentos]: ', m.pks_nulas, ' PKs nulas!')),
    ASSERT_TRUE(m.hashes_nulos = 0, CONCAT('ERRO DQ [estabelecimentos]: ', m.hashes_nulos, ' change_hash nulos!')),
    ASSERT_TRUE(m.ativos_com_fim = 0, CONCAT('ERRO DQ [estabelecimentos]: ', m.ativos_com_fim, ' registos ativos com valid_until não nulo!')),
    ASSERT_TRUE(m.inativos_invalidos = 0, CONCAT('ERRO DQ [estabelecimentos]: ', m.inativos_invalidos, ' registos inativos com intervalo inválido!')),
    ASSERT_TRUE(d.qtd_duplicadas = 0, CONCAT('ERRO DQ [estabelecimentos]: ', d.qtd_duplicadas, ' PKs com múltiplas versões ativas!'))
FROM metricas m CROSS JOIN duplicadas_ativas d;