WITH metricas AS (
    SELECT 
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN pk IS NULL OR TRIM(pk) = '' THEN 1 ELSE 0 END) AS pks_nulas,
        SUM(CASE WHEN change_hash IS NULL THEN 1 ELSE 0 END) AS hashes_nulos,
        SUM(CASE WHEN is_current IS NULL THEN 1 ELSE 0 END) AS is_current_nulos,
        SUM(CASE WHEN is_current = TRUE AND valid_until IS NOT NULL THEN 1 ELSE 0 END) AS ativos_com_fim,
        SUM(CASE WHEN is_current = FALSE AND (valid_until IS NULL OR valid_until <= valid_since) THEN 1 ELSE 0 END) AS inativos_invalidos
    FROM tcc_receita.silver_intermediate.socios
),
duplicadas_ativas AS (
    SELECT COUNT(*) AS qtd_duplicadas
    FROM (
        SELECT pk FROM tcc_receita.silver_intermediate.socios
        WHERE is_current = TRUE GROUP BY pk HAVING COUNT(*) > 1
    )
),
-- Toda pk precisa de exatamente uma versão corrente (zero também é erro).
sem_corrente AS (
    SELECT COUNT(*) AS qtd
    FROM (
        SELECT pk FROM tcc_receita.silver_intermediate.socios
        GROUP BY pk HAVING SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) <> 1
    )
),
-- A primeira versão de uma pk nunca pode ser inativa (pega reexecução que cria versão fantasma; C-05).
primeira_inativa AS (
    SELECT COUNT(*) AS qtd
    FROM (
        SELECT pk, MIN_BY(vinculo_ativo, valid_since) AS primeira
        FROM tcc_receita.silver_intermediate.socios GROUP BY pk
    ) WHERE primeira = FALSE
),
-- Continuidade: valid_until de cada versão = valid_since da próxima versão da mesma pk.
descontinuas AS (
    SELECT COUNT(*) AS qtd
    FROM (
        SELECT valid_until,
               LEAD(valid_since) OVER (PARTITION BY pk ORDER BY valid_since) AS prox_since
        FROM tcc_receita.silver_intermediate.socios
    ) WHERE valid_until IS DISTINCT FROM prox_since
),
-- Unicidade de (pk, valid_since): no máximo uma versão por pk e mês de início (premissa do MERGE da fase 2).
chaves_repetidas AS (
    SELECT COUNT(*) AS qtd
    FROM (
        SELECT pk, valid_since FROM tcc_receita.silver_intermediate.socios
        GROUP BY pk, valid_since HAVING COUNT(*) > 1
    )
),
-- Saídas em massa: por mês, versões que viram inativas / vínculos ativos no mês anterior (limite 5%).
-- Versão simples: conta pelo valid_since da versão inativa; "ativos no mês anterior" = versões ativas
-- cuja vigência [valid_since, valid_until) cobre o mês anterior. LEFT JOIN: mês com saídas e zero ativos também falha.
saidas_mes AS (
    SELECT valid_since AS mes,
           date_format(add_months(to_date(concat(valid_since, '-01')), -1), 'yyyy-MM') AS mes_anterior,
           COUNT(*) AS saidas
    FROM tcc_receita.silver_intermediate.socios
    WHERE vinculo_ativo = FALSE
    GROUP BY valid_since
),
saidas_proporcao AS (
    SELECT s.mes, s.saidas, COUNT(a.pk) AS ativos_anterior
    FROM saidas_mes s
    LEFT JOIN tcc_receita.silver_intermediate.socios a
      ON a.vinculo_ativo = TRUE
     AND a.valid_since <= s.mes_anterior
     AND (a.valid_until IS NULL OR a.valid_until > s.mes_anterior)
    GROUP BY s.mes, s.saidas
),
saidas_excessivas AS (
    SELECT COUNT(*) AS qtd, MAX(CONCAT(mes, ': ', saidas, '/', ativos_anterior)) AS exemplo
    FROM saidas_proporcao WHERE saidas > 0.05 * ativos_anterior
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 0, 'ERRO DQ [socios]: Tabela vazia!'),
    ASSERT_TRUE(m.pks_nulas = 0, CONCAT('ERRO DQ [socios]: ', m.pks_nulas, ' PKs nulas!')),
    ASSERT_TRUE(m.hashes_nulos = 0, CONCAT('ERRO DQ [socios]: ', m.hashes_nulos, ' change_hash nulos!')),
    ASSERT_TRUE(m.is_current_nulos = 0, CONCAT('ERRO DQ [socios]: ', m.is_current_nulos, ' versões com is_current nulo!')),
    ASSERT_TRUE(m.ativos_com_fim = 0, CONCAT('ERRO DQ [socios]: ', m.ativos_com_fim, ' registos ativos com valid_until não nulo!')),
    ASSERT_TRUE(m.inativos_invalidos = 0, CONCAT('ERRO DQ [socios]: ', m.inativos_invalidos, ' registos inativos com intervalo inválido!')),
    ASSERT_TRUE(d.qtd_duplicadas = 0, CONCAT('ERRO DQ [socios]: ', d.qtd_duplicadas, ' PKs com múltiplas versões ativas!')),
    ASSERT_TRUE(sc.qtd = 0, CONCAT('ERRO DQ [socios]: ', sc.qtd, ' PKs sem exatamente uma versão corrente!')),
    ASSERT_TRUE(pi.qtd = 0, CONCAT('ERRO DQ [socios]: ', pi.qtd, ' PKs cuja primeira versão é inativa (versão fantasma)!')),
    ASSERT_TRUE(dc.qtd = 0, CONCAT('ERRO DQ [socios]: ', dc.qtd, ' versões com valid_until diferente do valid_since da próxima!')),
    ASSERT_TRUE(cr.qtd = 0, CONCAT('ERRO DQ [socios]: ', CAST(cr.qtd AS STRING), ' pares (pk, valid_since) repetidos!')),
    ASSERT_TRUE(se.qtd = 0, CONCAT('ERRO DQ [socios]: ', se.qtd, ' meses com saídas > 5% dos vínculos ativos do mês anterior (ex.: ', se.exemplo, ')!'))
FROM metricas m
CROSS JOIN duplicadas_ativas d
CROSS JOIN sem_corrente sc
CROSS JOIN primeira_inativa pi
CROSS JOIN descontinuas dc
CROSS JOIN chaves_repetidas cr
CROSS JOIN saidas_excessivas se;
