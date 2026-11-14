-- DQ | silver_final.dim_empresa
-- Passa = nenhum ASSERT_TRUE dispara. Cada mensagem traz a contagem do defeito.
WITH metricas AS (
    SELECT
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_empresa = -1 THEN 1 ELSE 0 END) AS qtd_ghost_row,
        SUM(CASE WHEN sk_empresa = -1 AND uf = 'NA' THEN 1 ELSE 0 END) AS qtd_ghost_uf_ok,
        SUM(CASE WHEN localidade_cep IS NOT NULL AND sk_empresa != -1 THEN 1 ELSE 0 END) AS enriquecidos_cep,
        -- B24: UF tem de ser nula ou duas letras (pega 'RJ,SP' vindo da tabela de CEPs).
        SUM(CASE WHEN uf IS NOT NULL AND NOT (uf RLIKE '^[A-Z]{2}$') THEN 1 ELSE 0 END) AS uf_invalida
    FROM tcc_receita.silver_final.dim_empresa
),
colisoes_sk AS (
    -- Pega também fatia duplicada (mesmo CNPJ e início), sintoma de SCD2 de origem com sobreposição.
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
            COALESCE(fim_vigencia, '9999-12') AS fim_calc,
            LEAD(inicio_vigencia) OVER (PARTITION BY cnpj_raiz ORDER BY inicio_vigencia) AS prox_inicio
        FROM tcc_receita.silver_final.dim_empresa
        WHERE sk_empresa != -1
    )
    WHERE prox_inicio IS NOT NULL AND fim_calc > prox_inicio
),
-- Cobertura (C-02): a dimensão deve cobrir exatamente os meses da matriz canônica.
-- Mesma regra de matriz canônica do ingest: matriz vigente em cada período; em caso de empate, a menor ordem.
matriz_canonica AS (
    SELECT cnpj_basico, valid_since, COALESCE(valid_until, '9999-12') AS valid_until
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    -- A matriz pode mudar de estabelecimento ao longo do tempo (a Receita transfere a condição de matriz),
    -- e às vezes há mais de um marcado como matriz no mesmo período (B19). Regra da matriz canônica:
    -- em cada período vale o estabelecimento marcado como matriz; havendo mais de um ao mesmo tempo,
    -- fica o de menor ordem (0001 primeiro). A versão de ordem maior que se sobrepõe é descartada inteira.
    LEFT ANTI JOIN tcc_receita.silver_intermediate.estabelecimentos melhor
        ON melhor.cnpj_basico = e.cnpj_basico
       AND melhor.identificador_matriz_filial = '1'
       AND melhor.cnpj_ordem < e.cnpj_ordem
       AND melhor.valid_since < COALESCE(e.valid_until, '9999-12')
       AND COALESCE(melhor.valid_until, '9999-12') > e.valid_since
    WHERE e.identificador_matriz_filial = '1'
      AND e.cnpj_basico IS NOT NULL
      AND e.valid_since IS NOT NULL
      AND e.valid_since < COALESCE(e.valid_until, '9999-12')
),
-- Meses de um intervalo [a, b) = (ano_b*12 + mes_b) - (ano_a*12 + mes_a). '9999-12' = aberto; como os dois
-- lados usam o mesmo teto, a comparação continua exata.
cobertura_matriz AS (
    SELECT
        cnpj_basico AS cnpj_raiz,
        MIN(valid_since) AS primeiro_mes,
        MAX(valid_until) AS ultimo_fim,
        SUM((CAST(SUBSTR(valid_until, 1, 4) AS BIGINT) * 12 + CAST(SUBSTR(valid_until, 6, 2) AS BIGINT))
          - (CAST(SUBSTR(valid_since, 1, 4) AS BIGINT) * 12 + CAST(SUBSTR(valid_since, 6, 2) AS BIGINT))) AS qtd_meses,
        MAX(CASE WHEN valid_until = '9999-12' THEN 1 ELSE 0 END) AS tem_versao_corrente
    FROM matriz_canonica
    GROUP BY cnpj_basico
),
cobertura_dim AS (
    SELECT
        cnpj_raiz,
        MIN(inicio_vigencia) AS primeiro_mes,
        MAX(COALESCE(fim_vigencia, '9999-12')) AS ultimo_fim,
        SUM((CAST(SUBSTR(COALESCE(fim_vigencia, '9999-12'), 1, 4) AS BIGINT) * 12
             + CAST(SUBSTR(COALESCE(fim_vigencia, '9999-12'), 6, 2) AS BIGINT))
          - (CAST(SUBSTR(inicio_vigencia, 1, 4) AS BIGINT) * 12 + CAST(SUBSTR(inicio_vigencia, 6, 2) AS BIGINT))) AS qtd_meses,
        SUM(CASE WHEN registro_ativo THEN 1 ELSE 0 END) AS qtd_ativos
    FROM tcc_receita.silver_final.dim_empresa
    WHERE sk_empresa != -1
    GROUP BY cnpj_raiz
),
-- Sem sobreposição (teste acima), mesmo primeiro mês, mesmo último fim e mesma soma de meses
-- implicam cobertura idêntica: a dimensão não pode ter meses fora da matriz nem deixar de cobrir algum.
-- Por isso não há teste separado de continuidade: uma lacuna indevida na dim muda a soma de meses.
divergencias_cobertura AS (
    SELECT
        SUM(CASE WHEN d.cnpj_raiz IS NULL OR m.cnpj_raiz IS NULL
                   OR d.primeiro_mes <> m.primeiro_mes
                   OR d.ultimo_fim <> m.ultimo_fim
                   OR d.qtd_meses <> m.qtd_meses
                 THEN 1 ELSE 0 END) AS qtd_cnpj_cobertura_divergente,
        -- Exatamente uma versão ativa quando a matriz tem versão corrente; nenhuma caso contrário.
        SUM(CASE WHEN COALESCE(d.qtd_ativos, 0) <> COALESCE(m.tem_versao_corrente, 0)
                 THEN 1 ELSE 0 END) AS qtd_cnpj_ativos_divergentes,
        CONCAT_WS(', ', SLICE(COLLECT_LIST(CASE WHEN d.cnpj_raiz IS NULL OR m.cnpj_raiz IS NULL
                   OR d.primeiro_mes <> m.primeiro_mes
                   OR d.ultimo_fim <> m.ultimo_fim
                   OR d.qtd_meses <> m.qtd_meses
                 THEN COALESCE(d.cnpj_raiz, m.cnpj_raiz) END), 1, 5)) AS amostra_cobertura
    FROM cobertura_dim d
    FULL OUTER JOIN cobertura_matriz m ON d.cnpj_raiz = m.cnpj_raiz
),
orfaos_grupo_cnae AS (
    SELECT COUNT(*) AS qtd_orfaos_cnae
    FROM tcc_receita.silver_final.dim_empresa e
    LEFT ANTI JOIN tcc_receita.silver_final.dim_grupo_cnae g ON e.sk_grupo_cnae = g.sk_grupo_cnae
)
SELECT
    ASSERT_TRUE(m.total_linhas > 1, 'ERRO DQ [dim_empresa]: Tabela vazia!'),
    ASSERT_TRUE(m.qtd_ghost_row = 1, CONCAT('ERRO DQ [dim_empresa]: ', m.qtd_ghost_row, ' ghost rows (-1); esperado 1!')),
    ASSERT_TRUE(m.qtd_ghost_uf_ok = 1, 'ERRO DQ [dim_empresa]: ghost row (-1) sem uf = ''NA''!'),
    ASSERT_TRUE(m.enriquecidos_cep > 0, 'ERRO DQ [dim_empresa]: Nenhum registro foi enriquecido pela tabela de CEPs!'),
    ASSERT_TRUE(m.uf_invalida = 0, CONCAT('ERRO DQ [dim_empresa]: ', m.uf_invalida, ' linhas com uf fora do padrão de duas letras (B24)!')),
    ASSERT_TRUE(c.qtd_colisoes = 0, CONCAT('ERRO DQ [dim_empresa]: ', c.qtd_colisoes, ' colisões ou duplicidades de sk_empresa! Amostra de sk: ', COALESCE(c.amostra_colisoes, ''))),
    ASSERT_TRUE(s.qtd_sobreposicoes = 0, CONCAT('ERRO DQ [dim_empresa]: ', s.qtd_sobreposicoes, ' sobreposições de intervalos temporais detectadas!')),
    ASSERT_TRUE(dc.qtd_cnpj_cobertura_divergente = 0, CONCAT('ERRO DQ [dim_empresa]: ', dc.qtd_cnpj_cobertura_divergente, ' CNPJs com meses cobertos diferentes da matriz canônica (C-02)! Amostra: ', COALESCE(dc.amostra_cobertura, ''))),
    ASSERT_TRUE(dc.qtd_cnpj_ativos_divergentes = 0, CONCAT('ERRO DQ [dim_empresa]: ', dc.qtd_cnpj_ativos_divergentes, ' CNPJs sem exatamente uma versão ativa quando a matriz tem versão corrente (ou com ativa sem matriz corrente)!')),
    ASSERT_TRUE(og.qtd_orfaos_cnae = 0, CONCAT('ERRO DQ [dim_empresa]: ', og.qtd_orfaos_cnae, ' sk_grupo_cnae órfãs em relação à dim_grupo_cnae!'))
FROM metricas m
CROSS JOIN colisoes_sk c
CROSS JOIN sobreposicoes s
CROSS JOIN divergencias_cobertura dc
CROSS JOIN orfaos_grupo_cnae og;
