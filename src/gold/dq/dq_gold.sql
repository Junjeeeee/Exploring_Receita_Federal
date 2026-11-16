-- Gold | DQ (roda depois de todas as tabelas gold).
-- Padrão: SELECT ASSERT_TRUE(cond, 'ERRO DQ [tabela]: mensagem com números'). Passa = nenhum erro.
-- Janela = parâmetros do Job (obrigatórios, sem default "mês anterior").
-- Spec: docs/specs/features/2026-11-14-semantica-eventos-e-fluxos.md; critérios em docs/critica.md § 5.
-- Números nas mensagens vão com CAST(... AS STRING) para funcionar também com ANSI ligado (SQL warehouse).

-- 0. Parâmetros obrigatórios.
SELECT ASSERT_TRUE(
    '{{mes_referencia_inicial}}' RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
    AND '{{mes_referencia_final}}' RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
    AND '{{mes_referencia_inicial}}' <= '{{mes_referencia_final}}',
    'ERRO DQ [gold]: informe mes_referencia_inicial e mes_referencia_final no formato YYYY-MM, com inicial <= final (recebido: [{{mes_referencia_inicial}}] a [{{mes_referencia_final}}]).'
);

-- 1. Reconciliação entre camadas: a OBT tem exatamente uma linha por linha de fato_mudanca_estado.
WITH params AS (
    SELECT '{{mes_referencia_inicial}}' AS mes_ini, '{{mes_referencia_final}}' AS mes_fim
),
contagem_fato AS (
    SELECT COUNT(*) AS qtd_fato
    FROM tcc_receita.silver_final.fato_mudanca_estado f
    CROSS JOIN params p
    WHERE f.mes_referencia >= p.mes_ini AND f.mes_referencia <= p.mes_fim
),
contagem_obt AS (
    SELECT COUNT(*) AS qtd_obt
    FROM tcc_receita.gold.obt_dashboard_eventos o
    CROSS JOIN params p
    WHERE o.mes_referencia >= p.mes_ini AND o.mes_referencia <= p.mes_fim
)
SELECT
    ASSERT_TRUE(cf.qtd_fato = co.qtd_obt,
        CONCAT('ERRO DQ [obt_dashboard_eventos]: ', CAST(co.qtd_obt AS STRING), ' linhas na OBT x ',
               CAST(cf.qtd_fato AS STRING), ' em fato_mudanca_estado na janela.'))
FROM contagem_fato cf
CROSS JOIN contagem_obt co;

-- 2. fato_empresa_mes: identidade e continuidade do estoque, unicidade, coerência das flags, baseline, UF
--    e aberturas = primeiras aparições na dim_empresa.
WITH params AS (
    SELECT
        '{{mes_referencia_inicial}}' AS mes_ini,
        '{{mes_referencia_final}}' AS mes_fim,
        DATE_FORMAT(ADD_MONTHS(TO_DATE(CONCAT('{{mes_referencia_inicial}}', '-01')), -1), 'yyyy-MM') AS mes_anterior_janela
),
limites_dim AS (
    SELECT MIN(inicio_vigencia) AS mes_baseline
    FROM tcc_receita.silver_final.dim_empresa
    WHERE sk_empresa != -1
),
metricas_mes AS (
    -- Totais por mês, da janela e do mês anterior a ela (para a continuidade do primeiro mês).
    SELECT
        f.mes_referencia,
        COUNT(*) AS linhas,
        COUNT_IF(f.ativa_inicio_mes) AS estoque_inicial,
        COUNT_IF(f.ativa_fim_mes) AS estoque_final,
        COUNT_IF(f.entrou_estoque_mes) AS entradas,
        COUNT_IF(f.saiu_estoque_mes) AS saidas,
        COUNT_IF(f.teve_abertura_mes) AS aberturas,
        COUNT_IF(f.teve_baixa_mes AND f.situacao_cadastral <> 'BAIXADA') AS baixas_incoerentes,
        -- D-17: primeira aparição já BAIXADA/INAPTA/SUSPENSA tem abertura E o evento da situação; e evento de
        -- situação sem mês anterior só existe na primeira aparição.
        COUNT_IF(f.teve_abertura_mes
                 AND ((f.situacao_cadastral = 'BAIXADA'  AND NOT f.teve_baixa_mes)
                   OR (f.situacao_cadastral = 'INAPTA'   AND NOT f.teve_inaptidao_mes)
                   OR (f.situacao_cadastral = 'SUSPENSA' AND NOT f.teve_suspensao_mes))) AS primeira_aparicao_sem_evento,
        COUNT_IF((f.teve_baixa_mes OR f.teve_inaptidao_mes OR f.teve_suspensao_mes)
                 AND f.situacao_inicio_mes IS NULL AND NOT f.teve_abertura_mes) AS evento_sem_mes_anterior,
        COUNT_IF(f.ativa_inicio_mes OR f.teve_abertura_mes OR f.teve_reativacao_mes OR f.entrou_estoque_mes
                 OR f.saiu_estoque_mes OR f.teve_baixa_mes OR f.teve_falencia_mes OR f.teve_inaptidao_mes
                 OR f.teve_suspensao_mes) AS linhas_com_fluxo,
        COUNT_IF(NOT COALESCE(f.uf RLIKE '^[A-Z]{2}$', FALSE)) AS uf_invalida,
        -- Idade na baixa (dashboard v2): faixa só em baixa, idade nunca negativa, "sem data" medida.
        COUNT_IF(f.teve_baixa_mes) AS baixas,
        COUNT_IF((f.faixa_idade_baixa IS NOT NULL) <> f.teve_baixa_mes) AS faixa_idade_incoerente,
        COUNT_IF(f.idade_na_baixa_meses < 0) AS idade_negativa,
        COUNT_IF(f.faixa_idade_baixa = 'sem data') AS baixas_sem_data
    FROM tcc_receita.gold.fato_empresa_mes f
    CROSS JOIN params p
    WHERE f.mes_referencia >= p.mes_anterior_janela AND f.mes_referencia <= p.mes_fim
    GROUP BY f.mes_referencia
),
primeiras_aparicoes AS (
    -- Quantas empresas aparecem pela primeira vez na dim_empresa em cada mês.
    SELECT mes_primeira_aparicao AS mes_referencia, COUNT(*) AS qtd
    FROM (
        SELECT cnpj_raiz, MIN(inicio_vigencia) AS mes_primeira_aparicao
        FROM tcc_receita.silver_final.dim_empresa
        WHERE sk_empresa != -1
        GROUP BY cnpj_raiz
    )
    GROUP BY mes_primeira_aparicao
),
janela AS (
    -- Meses da janela com o estoque final de M−1 (NULL se M−1 não está na tabela: continuidade não se aplica).
    SELECT
        m.*,
        (m.mes_referencia = l.mes_baseline) AS eh_baseline,
        ant.estoque_final AS estoque_final_mes_anterior,
        CASE WHEN m.mes_referencia = l.mes_baseline THEN 0 ELSE COALESCE(pa.qtd, 0) END AS aberturas_esperadas
    FROM metricas_mes m
    CROSS JOIN params p
    CROSS JOIN limites_dim l
    LEFT JOIN metricas_mes ant
        ON ant.mes_referencia = DATE_FORMAT(ADD_MONTHS(TO_DATE(CONCAT(m.mes_referencia, '-01')), -1), 'yyyy-MM')
    LEFT JOIN primeiras_aparicoes pa
        ON pa.mes_referencia = m.mes_referencia
    WHERE m.mes_referencia >= p.mes_ini
),
resumo AS (
    SELECT
        COALESCE(SUM(linhas), 0) AS total_linhas,
        -- (1) Identidade: final = inicial + entradas − saídas (fora do baseline, que é o estoque de abertura).
        COUNT_IF(NOT eh_baseline AND estoque_final <> estoque_inicial + entradas - saidas) AS meses_sem_identidade,
        MIN(CASE WHEN NOT eh_baseline AND estoque_final <> estoque_inicial + entradas - saidas
                 THEN CONCAT(mes_referencia, ' final=', CAST(estoque_final AS STRING),
                             ' inicial=', CAST(estoque_inicial AS STRING),
                             ' entradas=', CAST(entradas AS STRING), ' saidas=', CAST(saidas AS STRING)) END) AS exemplo_identidade,
        -- (2) Continuidade: inicial(M) = final(M−1), quando M−1 existe na tabela.
        COUNT_IF(estoque_final_mes_anterior IS NOT NULL AND estoque_inicial <> estoque_final_mes_anterior) AS meses_sem_continuidade,
        MIN(CASE WHEN estoque_final_mes_anterior IS NOT NULL AND estoque_inicial <> estoque_final_mes_anterior
                 THEN CONCAT(mes_referencia, ' inicial=', CAST(estoque_inicial AS STRING),
                             ' final(M-1)=', CAST(estoque_final_mes_anterior AS STRING)) END) AS exemplo_continuidade,
        -- (4) Baixa só com situação final BAIXADA.
        COALESCE(SUM(baixas_incoerentes), 0) AS baixas_incoerentes,
        COALESCE(SUM(primeira_aparicao_sem_evento), 0) AS primeira_aparicao_sem_evento,
        COALESCE(SUM(evento_sem_mes_anterior), 0) AS evento_sem_mes_anterior,
        -- (5) Baseline sem fluxo nenhum (nem ativa_inicio).
        COALESCE(SUM(CASE WHEN eh_baseline THEN linhas_com_fluxo ELSE 0 END), 0) AS linhas_com_fluxo_no_baseline,
        -- (7) UF com 2 letras (o rótulo de desconhecido NI também tem 2 letras).
        COALESCE(SUM(uf_invalida), 0) AS uf_invalida,
        COALESCE(SUM(faixa_idade_incoerente), 0) AS faixa_idade_incoerente,
        COALESCE(SUM(idade_negativa), 0) AS idade_negativa,
        COALESCE(SUM(baixas), 0) AS baixas,
        COALESCE(SUM(baixas_sem_data), 0) AS baixas_sem_data,
        -- Critério de aceite da spec: aberturas = matrizes cuja primeira aparição é naquele mês.
        COUNT_IF(aberturas <> aberturas_esperadas) AS meses_aberturas_divergentes,
        MIN(CASE WHEN aberturas <> aberturas_esperadas
                 THEN CONCAT(mes_referencia, ' aberturas=', CAST(aberturas AS STRING),
                             ' primeiras_aparicoes=', CAST(aberturas_esperadas AS STRING)) END) AS exemplo_aberturas
    FROM janela
),
duplicados AS (
    -- (3) Unicidade do grão (cnpj_raiz, mes_referencia).
    SELECT COUNT(*) AS qtd_chaves_duplicadas
    FROM (
        SELECT f.cnpj_raiz, f.mes_referencia
        FROM tcc_receita.gold.fato_empresa_mes f
        CROSS JOIN params p
        WHERE f.mes_referencia >= p.mes_ini AND f.mes_referencia <= p.mes_fim
        GROUP BY f.cnpj_raiz, f.mes_referencia
        HAVING COUNT(*) > 1
    )
)
SELECT
    ASSERT_TRUE(r.total_linhas > 0,
        'ERRO DQ [fato_empresa_mes]: nenhuma linha na janela.'),
    ASSERT_TRUE(r.meses_sem_identidade = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: identidade de estoque falhou em ', CAST(r.meses_sem_identidade AS STRING),
               ' mes(es); ex.: ', COALESCE(r.exemplo_identidade, '?'))),
    ASSERT_TRUE(r.meses_sem_continuidade = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: estoque inicial difere do final do mes anterior em ',
               CAST(r.meses_sem_continuidade AS STRING), ' mes(es); ex.: ', COALESCE(r.exemplo_continuidade, '?'))),
    ASSERT_TRUE(d.qtd_chaves_duplicadas = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: ', CAST(d.qtd_chaves_duplicadas AS STRING),
               ' chaves (cnpj_raiz, mes_referencia) duplicadas.')),
    ASSERT_TRUE(r.baixas_incoerentes = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: ', CAST(r.baixas_incoerentes AS STRING),
               ' linhas com teve_baixa_mes e situacao_cadastral diferente de BAIXADA.')),
    ASSERT_TRUE(r.primeira_aparicao_sem_evento = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: ', CAST(r.primeira_aparicao_sem_evento AS STRING),
               ' aberturas ja BAIXADA/INAPTA/SUSPENSA sem a flag do evento correspondente (D-17).')),
    ASSERT_TRUE(r.evento_sem_mes_anterior = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: ', CAST(r.evento_sem_mes_anterior AS STRING),
               ' baixas/inaptidoes/suspensoes sem mes anterior fora da primeira aparicao.')),
    ASSERT_TRUE(r.linhas_com_fluxo_no_baseline = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: ', CAST(r.linhas_com_fluxo_no_baseline AS STRING),
               ' linhas do baseline com flag de fluxo ou ativa_inicio_mes TRUE.')),
    ASSERT_TRUE(r.uf_invalida = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: ', CAST(r.uf_invalida AS STRING), ' linhas com UF fora do padrao de 2 letras.')),
    ASSERT_TRUE(r.meses_aberturas_divergentes = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: aberturas diferem das primeiras aparicoes da dim_empresa em ',
               CAST(r.meses_aberturas_divergentes AS STRING), ' mes(es); ex.: ', COALESCE(r.exemplo_aberturas, '?'))),
    ASSERT_TRUE(r.faixa_idade_incoerente = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: ', CAST(r.faixa_idade_incoerente AS STRING),
               ' linhas com faixa_idade_baixa preenchida sem baixa (ou baixa sem faixa).')),
    ASSERT_TRUE(r.idade_negativa = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: ', CAST(r.idade_negativa AS STRING), ' baixas com idade negativa.')),
    -- Limiar [padrão] de 5%: acima disso a mediana e as faixas de idade deixam de representar as baixas.
    ASSERT_TRUE(r.baixas_sem_data <= 0.05 * r.baixas,
        CONCAT('ERRO DQ [fato_empresa_mes]: ', CAST(r.baixas_sem_data AS STRING), ' de ', CAST(r.baixas AS STRING),
               ' baixas sem idade (data de inicio ou da baixa ausente, ou baixa antes do inicio); limite 5%.'))
FROM resumo r
CROSS JOIN duplicados d;

-- 2b. Baixas: fato_empresa_mes × OBT. O dashboard mostra o total de baixas pelo fato e o motivo pela OBT
--     (dataset baixas_motivo: matriz que termina BAIXADA sem estar BAIXADA antes, inclusive a primeira aparição,
--     que na OBT vem com situação anterior INEXISTENTE). Os dois totais têm de fechar. Na amostra fecham com
--     diferença zero; o limite de 0,5% por mês [padrão] tolera casos de borda das duas origens (evento por
--     estabelecimento marcado matriz × matriz canônica da dim_empresa; buraco na linha do tempo).
WITH params AS (
    SELECT '{{mes_referencia_inicial}}' AS mes_ini, '{{mes_referencia_final}}' AS mes_fim
),
baixas_fato AS (
    SELECT f.mes_referencia, COUNT_IF(f.teve_baixa_mes) AS qtd
    FROM tcc_receita.gold.fato_empresa_mes f
    CROSS JOIN params p
    WHERE f.mes_referencia >= p.mes_ini AND f.mes_referencia <= p.mes_fim
    GROUP BY f.mes_referencia
),
baixas_obt AS (
    SELECT o.mes_referencia, COUNT(DISTINCT o.cnpj_raiz) AS qtd
    FROM tcc_receita.gold.obt_dashboard_eventos o
    CROSS JOIN params p
    WHERE o.mes_referencia >= p.mes_ini AND o.mes_referencia <= p.mes_fim
      AND o.tipo_evento = 'MUDOU ESTADO CADASTRAL'
      AND o.eh_matriz = TRUE
      AND o.situacao_cadastral_posterior = 'BAIXADA'
      AND o.situacao_cadastral_anterior <> 'BAIXADA'
    GROUP BY o.mes_referencia
),
comparacao AS (
    SELECT
        COALESCE(f.mes_referencia, o.mes_referencia) AS mes_referencia,
        COALESCE(f.qtd, 0) AS qtd_fato,
        COALESCE(o.qtd, 0) AS qtd_obt
    FROM baixas_fato f
    FULL OUTER JOIN baixas_obt o ON f.mes_referencia = o.mes_referencia
),
resumo AS (
    SELECT
        COUNT_IF(ABS(qtd_fato - qtd_obt) > 0.005 * GREATEST(qtd_fato, qtd_obt)) AS meses_divergentes,
        MIN(CASE WHEN ABS(qtd_fato - qtd_obt) > 0.005 * GREATEST(qtd_fato, qtd_obt)
                 THEN CONCAT(mes_referencia, ' fato=', CAST(qtd_fato AS STRING), ' obt=', CAST(qtd_obt AS STRING)) END)
            AS exemplo
    FROM comparacao
)
SELECT
    ASSERT_TRUE(r.meses_divergentes = 0,
        CONCAT('ERRO DQ [fato_empresa_mes x obt_dashboard_eventos]: total de baixas de matriz difere mais de 0,5% em ',
               CAST(r.meses_divergentes AS STRING), ' mes(es); ex.: ', COALESCE(r.exemplo, '?')))
FROM resumo r;

-- 2c. Porte da baixa na primeira aparição já BAIXADA (D-17b), recalculado aqui de forma independente:
--     (a) quem tem, no Simples, opção pelo MEI <= data da baixa e exclusão nula ou >= data da baixa tem de estar
--         com porte MEI e opcao_pelo_mei TRUE;
--     (b) plausibilidade [padrão]: com 100+ dessas baixas na janela, pelo menos 80% são MEI (base real: ~98% das
--         marcadas como ME tinham as datas de MEI; amostra: 256 de 257).
WITH params AS (
    SELECT '{{mes_referencia_inicial}}' AS mes_ini, '{{mes_referencia_final}}' AS mes_fim
),
nascidas_baixadas AS (
    SELECT f.mes_referencia, f.cnpj_raiz, f.porte_empresa, f.opcao_pelo_mei, d.data_situacao_cadastral AS data_baixa
    FROM tcc_receita.gold.fato_empresa_mes f
    CROSS JOIN params p
    JOIN tcc_receita.silver_final.dim_empresa d ON d.sk_empresa = f.sk_empresa
    WHERE f.mes_referencia >= p.mes_ini AND f.mes_referencia <= p.mes_fim
      AND f.teve_abertura_mes AND f.teve_baixa_mes
),
pela_regra AS (
    SELECT /*+ BROADCAST(n) */ n.mes_referencia, n.cnpj_raiz
    FROM nascidas_baixadas n
    JOIN tcc_receita.silver_intermediate.dados_simples s
      ON s.cnpj_basico = n.cnpj_raiz
     AND s.valid_since <= n.mes_referencia
    WHERE CAST(TRY_TO_TIMESTAMP(s.data_opcao_mei, 'yyyyMMdd') AS DATE) <= n.data_baixa
      AND (CAST(TRY_TO_TIMESTAMP(s.data_exclusao_mei, 'yyyyMMdd') AS DATE) IS NULL
           OR CAST(TRY_TO_TIMESTAMP(s.data_exclusao_mei, 'yyyyMMdd') AS DATE) >= n.data_baixa)
    GROUP BY n.mes_referencia, n.cnpj_raiz
),
resumo AS (
    SELECT
        COUNT(*) AS qtd,
        COUNT_IF(n.porte_empresa = 'MEI') AS qtd_mei,
        COUNT_IF(r.cnpj_raiz IS NOT NULL
                 AND (n.porte_empresa <> 'MEI' OR NOT COALESCE(n.opcao_pelo_mei, FALSE))) AS violacoes
    FROM nascidas_baixadas n
    LEFT JOIN pela_regra r ON r.mes_referencia = n.mes_referencia AND r.cnpj_raiz = n.cnpj_raiz
)
SELECT
    ASSERT_TRUE(r.violacoes = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: ', CAST(r.violacoes AS STRING),
               ' baixas de primeira aparicao com datas de MEI no Simples e porte diferente de MEI (D-17b).')),
    ASSERT_TRUE(r.qtd < 100 OR r.qtd_mei >= 0.8 * r.qtd,
        CONCAT('ERRO DQ [fato_empresa_mes]: so ', CAST(r.qtd_mei AS STRING), ' de ', CAST(r.qtd AS STRING),
               ' baixas de primeira aparicao sao MEI; esperado >= 80% (D-17b).'))
FROM resumo r;

-- 3. fato_indicadores_setoriais_mes: o painel soma o mesmo estoque que o fato e as taxas são proporções válidas.
WITH params AS (
    SELECT '{{mes_referencia_inicial}}' AS mes_ini, '{{mes_referencia_final}}' AS mes_fim
),
fato_mes AS (
    SELECT f.mes_referencia, COUNT_IF(f.ativa_fim_mes) AS estoque_final
    FROM tcc_receita.gold.fato_empresa_mes f
    CROSS JOIN params p
    WHERE f.mes_referencia >= p.mes_ini AND f.mes_referencia <= p.mes_fim
    GROUP BY f.mes_referencia
),
painel_mes AS (
    SELECT
        s.mes_referencia,
        SUM(s.estoque_final_ativas) AS estoque_final,
        COUNT_IF(s.taxa_fechamento_por_mil > 1000) AS taxas_acima_de_mil
    FROM tcc_receita.gold.fato_indicadores_setoriais_mes s
    CROSS JOIN params p
    WHERE s.mes_referencia >= p.mes_ini AND s.mes_referencia <= p.mes_fim
    GROUP BY s.mes_referencia
),
resumo AS (
    SELECT
        COUNT_IF(COALESCE(f.estoque_final, 0) <> COALESCE(s.estoque_final, 0)) AS meses_divergentes,
        MIN(CASE WHEN COALESCE(f.estoque_final, 0) <> COALESCE(s.estoque_final, 0)
                 THEN CONCAT(COALESCE(f.mes_referencia, s.mes_referencia),
                             ' fato=', CAST(COALESCE(f.estoque_final, 0) AS STRING),
                             ' painel=', CAST(COALESCE(s.estoque_final, 0) AS STRING)) END) AS exemplo,
        COALESCE(SUM(s.taxas_acima_de_mil), 0) AS taxas_acima_de_mil
    FROM fato_mes f
    FULL OUTER JOIN painel_mes s ON f.mes_referencia = s.mes_referencia
)
SELECT
    ASSERT_TRUE(r.meses_divergentes = 0,
        CONCAT('ERRO DQ [fato_indicadores_setoriais_mes]: estoque_final_ativas difere do fato_empresa_mes em ',
               CAST(r.meses_divergentes AS STRING), ' mes(es); ex.: ', COALESCE(r.exemplo, '?'))),
    ASSERT_TRUE(r.taxas_acima_de_mil = 0,
        CONCAT('ERRO DQ [fato_indicadores_setoriais_mes]: ', CAST(r.taxas_acima_de_mil AS STRING),
               ' linhas com taxa_fechamento_por_mil acima de 1000.'))
FROM resumo r;

-- 3b. Cobertura de meses do painel (fase 3): o painel é carregado só com os meses da janela (REPLACE WHERE), então
--     pode ficar com buraco se a carga completa da migração for pulada — e os testes acima, que olham só a janela,
--     não perceberiam. Aqui se compara o CONJUNTO de meses do painel com o do fato, do início do histórico até o
--     fim da janela (meses depois da janela ainda podem não ter sido processados numa carga em blocos). Lê só a
--     coluna de partição das duas tabelas.
WITH params AS (
    SELECT '{{mes_referencia_final}}' AS mes_fim
),
meses_fato AS (
    SELECT DISTINCT f.mes_referencia FROM tcc_receita.gold.fato_empresa_mes f CROSS JOIN params p
    WHERE f.mes_referencia <= p.mes_fim
),
meses_painel AS (
    SELECT DISTINCT s.mes_referencia FROM tcc_receita.gold.fato_indicadores_setoriais_mes s CROSS JOIN params p
    WHERE s.mes_referencia <= p.mes_fim
),
resumo AS (
    SELECT
        (SELECT COUNT(*) FROM (SELECT * FROM meses_fato EXCEPT SELECT * FROM meses_painel)) AS faltando,
        (SELECT COUNT(*) FROM (SELECT * FROM meses_painel EXCEPT SELECT * FROM meses_fato)) AS sobrando,
        (SELECT MIN(mes_referencia) FROM (SELECT * FROM meses_fato EXCEPT SELECT * FROM meses_painel)) AS exemplo
)
SELECT
    ASSERT_TRUE(r.faltando = 0 AND r.sobrando = 0,
        CONCAT('ERRO DQ [fato_indicadores_setoriais_mes]: meses do painel diferem dos do fato (', CAST(r.faltando AS STRING),
               ' faltando, ex.: ', COALESCE(r.exemplo, '?'), '; ', CAST(r.sobrando AS STRING),
               ' sobrando). Carga completa da migracao pulada?'))
FROM resumo r;

-- 4. dashboard_mercado_ativo = ativas do último mês do fato. Só se aplica quando o último mês do fato é o
--    último mês da dim_empresa (a fotografia do mercado é sempre a versão corrente); senão o teste é pulado.
WITH ultimo_fato AS (
    SELECT MAX(mes_referencia) AS mes FROM tcc_receita.gold.fato_empresa_mes
),
ultimo_dim AS (
    SELECT MAX(inicio_vigencia) AS mes FROM tcc_receita.silver_final.dim_empresa WHERE sk_empresa != -1
),
ativas_fato AS (
    SELECT COUNT_IF(f.ativa_fim_mes) AS qtd
    FROM tcc_receita.gold.fato_empresa_mes f
    WHERE f.mes_referencia = (SELECT mes FROM ultimo_fato)
),
ativas_mercado AS (
    SELECT COUNT(*) AS qtd FROM tcc_receita.gold.dashboard_mercado_ativo
)
SELECT
    ASSERT_TRUE(NOT COALESCE(ulf.mes = uld.mes, FALSE) OR af.qtd = am.qtd,
        CONCAT('ERRO DQ [dashboard_mercado_ativo]: ', CAST(am.qtd AS STRING), ' empresas no mercado ativo x ',
               CAST(af.qtd AS STRING), ' ativas em fato_empresa_mes no mes ', COALESCE(ulf.mes, '?'), '.'))
FROM ultimo_fato ulf
CROSS JOIN ultimo_dim uld
CROSS JOIN ativas_fato af
CROSS JOIN ativas_mercado am;

-- 5. Tabelas do dashboard (dash_*, fase 3): batem com as fontes e cobrem todos os meses.
--    (a) Na janela, por mês: linhas de dash_fluxos_mes = aberturas + baixas do fato; baixas em dash_baixas_idade_mes
--        e em dash_baixas_motivo_mes = baixas do fato; eventos em dash_eventos_mes = linhas da OBT.
--    (b) Cobertura, do início do histórico até o fim da janela (independente da janela, como o 3b): os meses das
--        dash_*_mes têm de ser os meses do fato depois do primeiro mês (no primeiro, a carga inicial, não há abertura
--        nem baixa) e, em dash_eventos_mes, os meses da OBT. Pega a migração seguida de uma janela parcial.
--    Só colunas de mês e contagens: as dash_* são pequenas e o fato é lido uma vez, na janela.
WITH params AS (
    SELECT '{{mes_referencia_inicial}}' AS mes_ini, '{{mes_referencia_final}}' AS mes_fim
),
fato_janela AS (
    SELECT f.mes_referencia, COUNT_IF(f.teve_abertura_mes) + COUNT_IF(f.teve_baixa_mes) AS linhas_fluxo,
           COUNT_IF(f.teve_baixa_mes) AS baixas
    FROM tcc_receita.gold.fato_empresa_mes f CROSS JOIN params p
    WHERE f.mes_referencia >= p.mes_ini AND f.mes_referencia <= p.mes_fim
    GROUP BY f.mes_referencia
),
obt_janela AS (
    SELECT o.mes_referencia, COUNT(*) AS eventos
    FROM tcc_receita.gold.obt_dashboard_eventos o CROSS JOIN params p
    WHERE o.mes_referencia >= p.mes_ini AND o.mes_referencia <= p.mes_fim
    GROUP BY o.mes_referencia
),
dash_fluxos AS (
    SELECT d.mes_referencia, COUNT(*) AS linhas FROM tcc_receita.gold.dash_fluxos_mes d CROSS JOIN params p
    WHERE d.mes_referencia >= p.mes_ini AND d.mes_referencia <= p.mes_fim GROUP BY d.mes_referencia
),
dash_idade AS (
    SELECT d.mes_referencia, SUM(d.qtd_baixas) AS baixas FROM tcc_receita.gold.dash_baixas_idade_mes d CROSS JOIN params p
    WHERE d.mes_referencia >= p.mes_ini AND d.mes_referencia <= p.mes_fim GROUP BY d.mes_referencia
),
dash_motivo AS (
    SELECT d.mes_referencia, SUM(d.qtd_empresas) AS baixas FROM tcc_receita.gold.dash_baixas_motivo_mes d CROSS JOIN params p
    WHERE d.mes_referencia >= p.mes_ini AND d.mes_referencia <= p.mes_fim GROUP BY d.mes_referencia
),
dash_eventos AS (
    SELECT d.mes_referencia, SUM(d.qtd_eventos) AS eventos FROM tcc_receita.gold.dash_eventos_mes d CROSS JOIN params p
    WHERE d.mes_referencia >= p.mes_ini AND d.mes_referencia <= p.mes_fim GROUP BY d.mes_referencia
),
conferencia AS (
    -- (a) por mês da janela; mês ausente de um lado conta como 0.
    SELECT
        COUNT_IF(COALESCE(f.linhas_fluxo, 0) <> COALESCE(df.linhas, 0)
              OR COALESCE(f.baixas, 0) <> COALESCE(di.baixas, 0)
              OR COALESCE(f.baixas, 0) <> COALESCE(dm.baixas, 0)) AS meses_fato_divergentes,
        MIN(CASE WHEN COALESCE(f.linhas_fluxo, 0) <> COALESCE(df.linhas, 0)
                   OR COALESCE(f.baixas, 0) <> COALESCE(di.baixas, 0)
                   OR COALESCE(f.baixas, 0) <> COALESCE(dm.baixas, 0)
                 THEN CONCAT(f.mes_referencia, ' fato=', CAST(f.linhas_fluxo AS STRING), '/', CAST(f.baixas AS STRING),
                             ' dash=', CAST(COALESCE(df.linhas, 0) AS STRING), '/', CAST(COALESCE(di.baixas, 0) AS STRING),
                             '/', CAST(COALESCE(dm.baixas, 0) AS STRING)) END) AS exemplo_fato
    FROM fato_janela f
    LEFT JOIN dash_fluxos df ON df.mes_referencia = f.mes_referencia
    LEFT JOIN dash_idade di ON di.mes_referencia = f.mes_referencia
    LEFT JOIN dash_motivo dm ON dm.mes_referencia = f.mes_referencia
),
conferencia_eventos AS (
    SELECT COUNT_IF(COALESCE(o.eventos, 0) <> COALESCE(d.eventos, 0)) AS meses_eventos_divergentes
    FROM obt_janela o
    FULL OUTER JOIN dash_eventos d ON d.mes_referencia = o.mes_referencia
),
-- (b) cobertura de meses até o fim da janela.
meses_fato AS (
    SELECT DISTINCT f.mes_referencia FROM tcc_receita.gold.fato_empresa_mes f CROSS JOIN params p
    WHERE f.mes_referencia <= p.mes_fim
      AND f.mes_referencia > (SELECT MIN(mes_referencia) FROM tcc_receita.gold.fato_empresa_mes)
),
meses_obt AS (
    SELECT DISTINCT o.mes_referencia FROM tcc_receita.gold.obt_dashboard_eventos o CROSS JOIN params p
    WHERE o.mes_referencia <= p.mes_fim
),
meses_dash AS (
    SELECT 'dash_fluxos_mes' AS tabela, d.mes_referencia FROM tcc_receita.gold.dash_fluxos_mes d
    UNION SELECT 'dash_baixas_idade_mes', d.mes_referencia FROM tcc_receita.gold.dash_baixas_idade_mes d
    UNION SELECT 'dash_baixas_motivo_mes', d.mes_referencia FROM tcc_receita.gold.dash_baixas_motivo_mes d
    UNION SELECT 'dash_eventos_mes', d.mes_referencia FROM tcc_receita.gold.dash_eventos_mes d
),
esperado AS (
    SELECT t.tabela, m.mes_referencia
    FROM (SELECT 'dash_fluxos_mes' AS tabela UNION ALL SELECT 'dash_baixas_idade_mes'
          UNION ALL SELECT 'dash_baixas_motivo_mes') t
    CROSS JOIN meses_fato m
    UNION ALL
    SELECT 'dash_eventos_mes', mes_referencia FROM meses_obt
),
cobertura AS (
    SELECT
        (SELECT COUNT(*) FROM (SELECT * FROM esperado
                               EXCEPT SELECT d.* FROM meses_dash d CROSS JOIN params p WHERE d.mes_referencia <= p.mes_fim)) AS faltando,
        (SELECT COUNT(*) FROM (SELECT d.* FROM meses_dash d CROSS JOIN params p WHERE d.mes_referencia <= p.mes_fim
                               EXCEPT SELECT * FROM esperado)) AS sobrando,
        (SELECT MIN(CONCAT(tabela, ' ', mes_referencia)) FROM (SELECT * FROM esperado
                               EXCEPT SELECT d.* FROM meses_dash d CROSS JOIN params p WHERE d.mes_referencia <= p.mes_fim)) AS exemplo
),
-- (c) mercado: a soma de dash_mercado é o número de empresas do mercado ativo (mesma fotografia).
mercado AS (
    SELECT (SELECT SUM(empresas_ativas) FROM tcc_receita.gold.dash_mercado) AS soma_dash,
           (SELECT COUNT(*) FROM tcc_receita.gold.dashboard_mercado_ativo) AS qtd_mercado
)
SELECT
    ASSERT_TRUE(c.meses_fato_divergentes = 0,
        CONCAT('ERRO DQ [dash_fluxos_mes/dash_baixas_*]: contagens diferem do fato_empresa_mes em ',
               CAST(c.meses_fato_divergentes AS STRING), ' mes(es); ex.: ', COALESCE(c.exemplo_fato, '?'))),
    ASSERT_TRUE(ce.meses_eventos_divergentes = 0,
        CONCAT('ERRO DQ [dash_eventos_mes]: soma de eventos difere da OBT em ',
               CAST(ce.meses_eventos_divergentes AS STRING), ' mes(es) da janela.')),
    ASSERT_TRUE(cob.faltando = 0 AND cob.sobrando = 0,
        CONCAT('ERRO DQ [dash_*_mes]: meses diferem dos da fonte (', CAST(cob.faltando AS STRING), ' faltando, ex.: ',
               COALESCE(cob.exemplo, '?'), '; ', CAST(cob.sobrando AS STRING), ' sobrando). Carga completa da migracao pulada?')),
    ASSERT_TRUE(COALESCE(m.soma_dash, 0) = m.qtd_mercado,
        CONCAT('ERRO DQ [dash_mercado]: soma de empresas_ativas = ', CAST(COALESCE(m.soma_dash, 0) AS STRING),
               ' x ', CAST(m.qtd_mercado AS STRING), ' linhas em dashboard_mercado_ativo.'))
FROM conferencia c
CROSS JOIN conferencia_eventos ce
CROSS JOIN cobertura cob
CROSS JOIN mercado m;

-- 6. Ponte "aberturas e baixas → crescimento líquido" (quadro da página 1 do dashboard). Por mês da janela, no fato:
--      aberturas − baixas + reativações + baixas de quem já não estava ativa − aberturas que não entraram como ativas
--      − saídas que não são baixa = crescimento líquido (entradas − saídas)
--    com reativações = entrou_estoque_mes AND NOT teve_abertura_mes. O quadro é montado a partir do painel, que
--    guarda teve_reativacao_mes e qtd_baixas_de_ativas; por isso também se exige, linha a linha agregada:
--      (i)  entrou AND NOT abertura = teve_reativacao_mes (não há outra forma de entrar nas ativas);
--      (ii) baixa AND saiu = baixa AND ativa_inicio_mes (toda baixa de quem estava ativa é uma saída).
--    Base real, ago/2023 (conferência do autor): 397.214 − 200.882 + 38.556 + 36.373 − 10.533 − 2.090 = 258.638.
WITH params AS (
    SELECT '{{mes_referencia_inicial}}' AS mes_ini, '{{mes_referencia_final}}' AS mes_fim
),
ponte AS (
    SELECT
        f.mes_referencia,
        COUNT_IF(f.teve_abertura_mes) AS aberturas,
        COUNT_IF(f.teve_baixa_mes) AS baixas,
        COUNT_IF(f.entrou_estoque_mes AND NOT f.teve_abertura_mes) AS reativacoes,
        COUNT_IF(f.teve_baixa_mes AND NOT f.saiu_estoque_mes) AS baixas_sem_saida,
        COUNT_IF(f.teve_abertura_mes AND NOT f.entrou_estoque_mes) AS aberturas_sem_entrada,
        COUNT_IF(f.saiu_estoque_mes AND NOT f.teve_baixa_mes) AS saidas_sem_baixa,
        COUNT_IF(f.entrou_estoque_mes) - COUNT_IF(f.saiu_estoque_mes) AS crescimento,
        COUNT_IF(f.teve_reativacao_mes) AS reativacoes_flag,
        COUNT_IF(f.teve_baixa_mes AND f.saiu_estoque_mes) AS baixas_com_saida,
        COUNT_IF(f.teve_baixa_mes AND f.ativa_inicio_mes) AS baixas_de_ativas
    FROM tcc_receita.gold.fato_empresa_mes f
    CROSS JOIN params p
    WHERE f.mes_referencia >= p.mes_ini AND f.mes_referencia <= p.mes_fim
    GROUP BY f.mes_referencia
),
resumo AS (
    SELECT
        COUNT_IF(aberturas - baixas + reativacoes + baixas_sem_saida - aberturas_sem_entrada - saidas_sem_baixa
                 <> crescimento) AS meses_sem_identidade,
        MIN(CASE WHEN aberturas - baixas + reativacoes + baixas_sem_saida - aberturas_sem_entrada - saidas_sem_baixa
                      <> crescimento
                 THEN CONCAT(mes_referencia, ' ', CAST(aberturas AS STRING), ' - ', CAST(baixas AS STRING), ' + ',
                             CAST(reativacoes AS STRING), ' + ', CAST(baixas_sem_saida AS STRING), ' - ',
                             CAST(aberturas_sem_entrada AS STRING), ' - ', CAST(saidas_sem_baixa AS STRING),
                             ' <> ', CAST(crescimento AS STRING)) END) AS exemplo,
        COUNT_IF(reativacoes <> reativacoes_flag OR baixas_com_saida <> baixas_de_ativas) AS meses_painel_incoerente
    FROM ponte
)
SELECT
    ASSERT_TRUE(r.meses_sem_identidade = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: ponte aberturas/baixas -> crescimento liquido nao fecha em ',
               CAST(r.meses_sem_identidade AS STRING), ' mes(es); ex.: ', COALESCE(r.exemplo, '?'))),
    ASSERT_TRUE(r.meses_painel_incoerente = 0,
        CONCAT('ERRO DQ [fato_empresa_mes]: em ', CAST(r.meses_painel_incoerente AS STRING),
               ' mes(es), entradas sem abertura diferem das reativacoes ou baixas com saida diferem das baixas de ativas',
               ' (o quadro da pagina 1 usaria numeros errados).'))
FROM resumo r;
