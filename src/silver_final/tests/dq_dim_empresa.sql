WITH metricas AS (
    SELECT 
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_empresa = -1 THEN 1 ELSE 0 END) AS qtd_ghost_row,
        SUM(CASE WHEN sk_empresa IS NULL OR nk_empresa IS NULL OR sk_grupo_cnae IS NULL THEN 1 ELSE 0 END) AS chaves_nulas,
        SUM(CASE WHEN registro_ativo = TRUE AND fim_vigencia IS NOT NULL THEN 1 ELSE 0 END) AS ativos_com_fim,
        SUM(CASE WHEN registro_ativo = FALSE AND sk_empresa != -1 AND (fim_vigencia IS NULL OR fim_vigencia <= inicio_vigencia) THEN 1 ELSE 0 END) AS inativos_invalidos,
        SUM(CASE WHEN localidade_cep IS NOT NULL AND sk_empresa != -1 THEN 1 ELSE 0 END) AS enriquecidos_cep
    FROM tcc_receita.silver_final.dim_empresa
),
colisoes_sk AS (
    SELECT COUNT(*) AS qtd_colisoes,
           CONCAT_WS(', ', SLICE(COLLECT_LIST(CAST(sk_empresa AS STRING)), 1, 5)) AS amostra_colisoes
    FROM (
        SELECT sk_empresa 
        FROM tcc_receita.silver_final.dim_empresa
        GROUP BY sk_empresa 
        HAVING COUNT(*) > 1 OR COUNT(DISTINCT nk_empresa) > 1
    )
),
sobreposicoes AS (
    SELECT COUNT(*) AS qtd_sobreposicoes
    FROM (
        SELECT 
            cnpj_raiz,
            inicio_vigencia,
            COALESCE(fim_vigencia, '9999-12') AS fim_calc,
            LEAD(inicio_vigencia) OVER (PARTITION BY cnpj_raiz ORDER BY inicio_vigencia) AS prox_inicio
        FROM tcc_receita.silver_final.dim_empresa
        WHERE sk_empresa != -1
    )
    WHERE prox_inicio IS NOT NULL AND fim_calc > prox_inicio
),
multiplos_ativos AS (
    SELECT COUNT(*) AS qtd_cnpj_multi_ativo
    FROM (
        SELECT cnpj_raiz
        FROM tcc_receita.silver_final.dim_empresa
        WHERE sk_empresa != -1 AND registro_ativo = TRUE
        GROUP BY cnpj_raiz
        HAVING COUNT(*) > 1
    )
),
orfaos_grupo_cnae AS (
    SELECT COUNT(*) AS qtd_orfaos_cnae
    FROM tcc_receita.silver_final.dim_empresa e
    LEFT ANTI JOIN tcc_receita.silver_final.dim_grupo_cnae g ON e.sk_grupo_cnae = g.sk_grupo_cnae
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 1, 'ERRO DQ [dim_empresa]: Tabela vazia!'),
    ASSERT_TRUE(m.qtd_ghost_row = 1, 'ERRO DQ [dim_empresa]: Ghost row (-1) ausente ou duplicada!'),
    ASSERT_TRUE(m.chaves_nulas = 0, CONCAT('ERRO DQ [dim_empresa]: ', m.chaves_nulas, ' SKs/NKs nulas!')),
    ASSERT_TRUE(m.ativos_com_fim = 0, CONCAT('ERRO DQ [dim_empresa]: ', m.ativos_com_fim, ' registos ativos com fim_vigencia preenchido!')),
    ASSERT_TRUE(m.inativos_invalidos = 0, CONCAT('ERRO DQ [dim_empresa]: ', m.inativos_invalidos, ' registos inativos com intervalo inválido!')),
    ASSERT_TRUE(m.enriquecidos_cep > 0, 'ERRO DQ [dim_empresa]: Nenhum registo foi enriquecido pela tabela de CEPs!'),
    ASSERT_TRUE(c.qtd_colisoes = 0, CONCAT('ERRO DQ [dim_empresa]: ', c.qtd_colisoes, ' colisões ou duplicidades de sk_empresa! Amostra de sk: ', COALESCE(c.amostra_colisoes, ''))),
    ASSERT_TRUE(s.qtd_sobreposicoes = 0, CONCAT('ERRO DQ [dim_empresa]: ', s.qtd_sobreposicoes, ' sobreposições de intervalos temporais detectadas!')),
    ASSERT_TRUE(ma.qtd_cnpj_multi_ativo = 0, CONCAT('ERRO DQ [dim_empresa]: ', ma.qtd_cnpj_multi_ativo, ' CNPJs com mais de uma versão ativa simultânea!')),
    ASSERT_TRUE(og.qtd_orfaos_cnae = 0, CONCAT('ERRO DQ [dim_empresa]: ', og.qtd_orfaos_cnae, ' sk_grupo_cnae órfãs em relação à dim_grupo_cnae!'))
FROM metricas m 
CROSS JOIN colisoes_sk c 
CROSS JOIN sobreposicoes s
CROSS JOIN multiplos_ativos ma
CROSS JOIN orfaos_grupo_cnae og;