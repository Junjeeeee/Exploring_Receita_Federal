-- DQ da SCD2 estabelecimentos. Passa = nenhuma asserção dispara.
-- As três últimas checagens (sem_corrente, descontinuas, chaves_repetidas) protegem a linha do tempo:
-- a fase 2 da otimização grava o histórico com um MERGE que só toca as versões que mudam, então um erro
-- de lógica apareceria como buraco, sobreposição ou versão repetida, e não como tabela vazia.
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
),
-- Toda pk precisa de exatamente uma versão corrente (zero também é erro: a pk "sumiria" do estado atual).
sem_corrente AS (
    SELECT COUNT(*) AS qtd
    FROM (
        SELECT pk FROM tcc_receita.silver_intermediate.estabelecimentos
        GROUP BY pk HAVING SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) <> 1
    )
),
-- Continuidade: valid_until de cada versão = valid_since da próxima versão da mesma pk. IS DISTINCT FROM
-- também pega a versão fechada sem sucessora (valid_until preenchido e próxima NULL).
descontinuas AS (
    SELECT COUNT(*) AS qtd
    FROM (
        SELECT valid_until,
               LEAD(valid_since) OVER (PARTITION BY pk ORDER BY valid_since) AS prox_since
        FROM tcc_receita.silver_intermediate.estabelecimentos
    ) WHERE valid_until IS DISTINCT FROM prox_since
),
-- Unicidade de (pk, valid_since): no máximo uma versão por pk e mês de início.
chaves_repetidas AS (
    SELECT COUNT(*) AS qtd
    FROM (
        SELECT pk, valid_since FROM tcc_receita.silver_intermediate.estabelecimentos
        GROUP BY pk, valid_since HAVING COUNT(*) > 1
    )
)
SELECT
    ASSERT_TRUE(m.total_linhas > 0, 'ERRO DQ [estabelecimentos]: Tabela vazia!'),
    ASSERT_TRUE(m.pks_nulas = 0, CONCAT('ERRO DQ [estabelecimentos]: ', m.pks_nulas, ' PKs nulas!')),
    ASSERT_TRUE(m.hashes_nulos = 0, CONCAT('ERRO DQ [estabelecimentos]: ', m.hashes_nulos, ' change_hash nulos!')),
    ASSERT_TRUE(m.ativos_com_fim = 0, CONCAT('ERRO DQ [estabelecimentos]: ', m.ativos_com_fim, ' registos ativos com valid_until não nulo!')),
    ASSERT_TRUE(m.inativos_invalidos = 0, CONCAT('ERRO DQ [estabelecimentos]: ', m.inativos_invalidos, ' registos inativos com intervalo inválido!')),
    ASSERT_TRUE(d.qtd_duplicadas = 0, CONCAT('ERRO DQ [estabelecimentos]: ', d.qtd_duplicadas, ' PKs com múltiplas versões ativas!')),
    ASSERT_TRUE(sc.qtd = 0, CONCAT('ERRO DQ [estabelecimentos]: ', CAST(sc.qtd AS STRING), ' PKs sem exatamente uma versão corrente!')),
    ASSERT_TRUE(dc.qtd = 0, CONCAT('ERRO DQ [estabelecimentos]: ', CAST(dc.qtd AS STRING), ' versões com valid_until diferente do valid_since da próxima!')),
    ASSERT_TRUE(cr.qtd = 0, CONCAT('ERRO DQ [estabelecimentos]: ', CAST(cr.qtd AS STRING), ' pares (pk, valid_since) repetidos!'))
FROM metricas m
CROSS JOIN duplicadas_ativas d
CROSS JOIN sem_corrente sc
CROSS JOIN descontinuas dc
CROSS JOIN chaves_repetidas cr;
