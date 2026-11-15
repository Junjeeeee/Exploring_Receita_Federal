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
