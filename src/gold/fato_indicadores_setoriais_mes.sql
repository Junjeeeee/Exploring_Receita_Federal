-- Gold | fato_indicadores_setoriais_mes
-- Grão: mês × UF × porte × CNAE principal (com classe, divisão e grande área).
-- Fonte: gold.fato_empresa_mes (empresa × mês) + gold.fato_indicadores_macro_mes.
-- Spec: docs/specs/features/2026-11-14-semantica-eventos-e-fluxos.md (seção 2.4).
--
-- Estoque e fluxos fecham por construção em cada linha deste painel:
--     estoque_final_ativas = estoque_inicial_ativas + qtd_entradas_estoque − qtd_saidas_estoque
-- (no baseline não há fluxo: o estoque final é o estoque de abertura).
-- Atenção: estoque_inicial(M) = estoque_final(M−1) vale no TOTAL do mês, não por grupo — uma empresa que
-- muda de UF/CNAE/porte entre M−1 e M é contada no grupo de M.
--
-- Carga INCREMENTAL (fase 3, U5; spec 2026-11-16-otimizacao-log-fato-dashboard.md §3.3): cada mês do painel
-- depende só do mesmo mês do fato, então a execução lê e substitui só os meses da janela (REPLACE WHERE), em
-- vez de recalcular o fato inteiro. Consequências:
--   * revisão de Selic/IPCA de um mês antigo só entra ao reprocessar aquele mês;
--   * a tabela só tem os meses que já foram processados: na migração (src/jobs/migracoes/2026-11-16_fase3.sql)
--     é obrigatória uma carga com a janela inteira; o DQ de cobertura (dq_gold.sql) falha se faltar mês.

CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE TABLE IF NOT EXISTS tcc_receita.gold.fato_indicadores_setoriais_mes (
    mes_referencia STRING COMMENT 'Mês do snapshot da Receita (YYYY-MM), mês de detecção. Partição.',
    data_referencia DATE COMMENT 'Primeiro dia do mês de referência (YYYY-MM-01).',
    uf STRING COMMENT 'UF da matriz no mês; NI = não identificada.',
    porte_empresa STRING COMMENT 'Porte analítico (MEI, ME, EPP, DEMAIS, NAO IDENTIFICADO...), como em fato_empresa_mes.',
    grande_area_cnae STRING COMMENT 'Grande setor econômico (agrupamento analítico) no mês.',
    divisao_cnae STRING COMMENT 'Divisão CNAE do CNAE principal no mês.',
    classe_cnae STRING COMMENT 'Classe CNAE do CNAE principal no mês.',
    codigo_cnae STRING COMMENT 'CNAE principal da matriz no mês (0000000 = não informado).',
    nome_cnae STRING COMMENT 'Descrição do CNAE principal no mês.',
    estoque_inicial_ativas BIGINT COMMENT 'Matrizes ATIVAS no início do mês (= no mês anterior).',
    estoque_final_ativas BIGINT COMMENT 'Matrizes ATIVAS no fim do mês.',
    qtd_aberturas BIGINT COMMENT 'Primeiras aparições (inclui as que já aparecem fora de ATIVA, D-17).',
    qtd_baixas BIGINT COMMENT 'Todas as baixas do mês (inclui primeira aparição já BAIXADA).',
    qtd_falencias BIGINT COMMENT 'Baixas com motivo 05, 06 ou 10.',
    qtd_inaptidoes BIGINT COMMENT 'Passagens para INAPTA no mês.',
    idade_media_ativas_meses DOUBLE COMMENT 'Idade média (meses) das matrizes ativas no fim do mês.',
    crescimento_liquido BIGINT COMMENT 'qtd_entradas_estoque − qtd_saidas_estoque.',
    taxa_abertura_por_mil DECIMAL(30,4) COMMENT 'Aberturas por 1.000 ativas no início do mês.',
    taxa_fechamento_por_mil DECIMAL(30,4) COMMENT 'Baixas de ativas por 1.000 ativas no início do mês.',
    taxa_falencia_por_mil DECIMAL(30,4) COMMENT 'Falências de ativas por 1.000 ativas no início do mês.',
    taxa_inaptidao_por_mil DECIMAL(30,4) COMMENT 'Inaptidões de ativas por 1.000 ativas no início do mês.',
    selic_mensal_pct DOUBLE COMMENT 'Taxa Selic efetiva acumulada no mês (% a.m. - Série BCB SGS 4390).',
    selic_anualizada_media_pct DOUBLE COMMENT 'Taxa Selic média mensal anualizada base 252 (% a.a. - Série BCB SGS 4189).',
    ipca_mensal_pct DOUBLE COMMENT 'Variação percentual mensal do IPCA (% a.m. - Série BCB SGS 433).',
    ipca_acumulado_12m_pct DOUBLE COMMENT 'IPCA acumulado nos últimos 12 meses (% a.a.).',
    juro_real_ex_post_anual_pct DOUBLE COMMENT 'Taxa de juro real ex-post anualizada aproximada ((1 + Selic_aa)/(1 + IPCA_12m) - 1).',
    selic_anualizada_lag_1m DOUBLE COMMENT 'Selic média anualizada defasada em 1 mês (t-1).',
    selic_anualizada_lag_3m DOUBLE COMMENT 'Selic média anualizada defasada em 3 meses (t-3).',
    selic_anualizada_lag_6m DOUBLE COMMENT 'Selic média anualizada defasada em 6 meses (t-6).',
    selic_anualizada_lag_12m DOUBLE COMMENT 'Selic média anualizada defasada em 12 meses (t-12).',
    data_carga_etl TIMESTAMP COMMENT 'Momento do processamento na camada Gold.',
    qtd_reativacoes BIGINT COMMENT 'Existia em M−1 sem estar ATIVA e está ATIVA em M.',
    qtd_entradas_estoque BIGINT COMMENT 'Passaram a estar ativas (aberturas e reativações).',
    qtd_saidas_estoque BIGINT COMMENT 'Deixaram de estar ativas (baixas, inaptidões, suspensões...).',
    qtd_baixas_de_ativas BIGINT COMMENT 'Baixas de empresas ativas no início do mês (numerador da taxa de fechamento).',
    qtd_suspensoes BIGINT COMMENT 'Passagens para SUSPENSA no mês.',
    qtd_falencias_de_ativas BIGINT COMMENT 'Falências de empresas ativas no início do mês.',
    qtd_inaptidoes_de_ativas BIGINT COMMENT 'Inaptidões de empresas ativas no início do mês.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Gold: painel mensal (mês × UF × porte × CNAE) com estoque de ativas, fluxos de entrada/saída, eventos de situação, taxas por 1.000 ativas no início do mês e indicadores macroeconômicos. Carga incremental por mês (REPLACE WHERE da janela).';

-- Parâmetros obrigatórios: o REPLACE WHERE abaixo precisa da janela explícita (parâmetro vazio falha aqui).
SELECT ASSERT_TRUE(
    '{{mes_referencia_inicial}}' RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
    AND '{{mes_referencia_final}}' RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
    AND '{{mes_referencia_inicial}}' <= '{{mes_referencia_final}}',
    'ERRO [fato_indicadores_setoriais_mes]: informe mes_referencia_inicial e mes_referencia_final no formato YYYY-MM, com inicial <= final (recebido: [{{mes_referencia_inicial}}] a [{{mes_referencia_final}}]).'
);

-- Só os meses da janela são lidos do fato e substituídos aqui; os outros meses do painel ficam intactos.
INSERT INTO tcc_receita.gold.fato_indicadores_setoriais_mes
REPLACE WHERE mes_referencia >= '{{mes_referencia_inicial}}'
          AND mes_referencia <= '{{mes_referencia_final}}'
WITH agregado_setorial AS (
    -- Contagens por grupo. "_de_ativas" = só empresas que estavam ATIVAS no início do mês: são subconjunto
    -- de estoque_inicial_ativas e por isso servem de numerador das taxas (nunca passam de 1.000‰).
    SELECT
        f.mes_referencia,
        f.data_referencia,
        f.uf,
        f.porte_empresa,
        f.grande_area_cnae,
        f.divisao_cnae,
        f.classe_cnae,
        f.codigo_cnae,
        f.nome_cnae,
        COUNT_IF(f.ativa_inicio_mes) AS estoque_inicial_ativas,
        COUNT_IF(f.ativa_fim_mes) AS estoque_final_ativas,
        COUNT_IF(f.teve_abertura_mes) AS qtd_aberturas,
        COUNT_IF(f.teve_baixa_mes) AS qtd_baixas,
        COUNT_IF(f.teve_falencia_mes) AS qtd_falencias,
        COUNT_IF(f.teve_inaptidao_mes) AS qtd_inaptidoes,
        AVG(CASE WHEN f.ativa_fim_mes THEN CAST(f.idade_meses AS DOUBLE) END) AS idade_media_ativas_meses,
        COUNT_IF(f.teve_reativacao_mes) AS qtd_reativacoes,
        COUNT_IF(f.entrou_estoque_mes) AS qtd_entradas_estoque,
        COUNT_IF(f.saiu_estoque_mes) AS qtd_saidas_estoque,
        COUNT_IF(f.teve_baixa_mes AND f.ativa_inicio_mes) AS qtd_baixas_de_ativas,
        COUNT_IF(f.teve_suspensao_mes) AS qtd_suspensoes,
        COUNT_IF(f.teve_falencia_mes AND f.ativa_inicio_mes) AS qtd_falencias_de_ativas,
        COUNT_IF(f.teve_inaptidao_mes AND f.ativa_inicio_mes) AS qtd_inaptidoes_de_ativas
    FROM tcc_receita.gold.fato_empresa_mes f
    WHERE f.mes_referencia >= '{{mes_referencia_inicial}}'
      AND f.mes_referencia <= '{{mes_referencia_final}}'
    GROUP BY
        f.mes_referencia, f.data_referencia, f.uf, f.porte_empresa,
        f.grande_area_cnae, f.divisao_cnae, f.classe_cnae, f.codigo_cnae, f.nome_cnae
)
-- Inserção posicional: a ordem do SELECT segue a ordem das colunas do CREATE.
SELECT /*+ BROADCAST(m) */
    a.mes_referencia,
    a.data_referencia,
    a.uf,
    a.porte_empresa,
    a.grande_area_cnae,
    a.divisao_cnae,
    a.classe_cnae,
    a.codigo_cnae,
    a.nome_cnae,
    a.estoque_inicial_ativas,
    a.estoque_final_ativas,
    a.qtd_aberturas,
    -- Todas as baixas do mês: inclui quem já estava INAPTA/SUSPENSA e quem já aparece BAIXADA na primeira
    -- aparição (abriu e fechou entre dois snapshots, D-17; conta também em qtd_aberturas). Taxa de fechamento usa
    -- qtd_baixas_de_ativas: essas empresas nunca estiveram na população em risco (ativas no início do mês).
    a.qtd_baixas,
    a.qtd_falencias,
    a.qtd_inaptidoes,
    a.idade_media_ativas_meses,
    -- Saldo do estoque de ativas: fecha com estoque_final − estoque_inicial por construção.
    (a.qtd_entradas_estoque - a.qtd_saidas_estoque) AS crescimento_liquido,
    -- Taxas por 1.000 empresas ativas no início do mês (população em risco). CAST explícito para o tipo da coluna.
    CAST(ROUND(1000.0 * a.qtd_aberturas            / NULLIF(a.estoque_inicial_ativas, 0), 4) AS DECIMAL(30,4)) AS taxa_abertura_por_mil,
    CAST(ROUND(1000.0 * a.qtd_baixas_de_ativas     / NULLIF(a.estoque_inicial_ativas, 0), 4) AS DECIMAL(30,4)) AS taxa_fechamento_por_mil,
    CAST(ROUND(1000.0 * a.qtd_falencias_de_ativas  / NULLIF(a.estoque_inicial_ativas, 0), 4) AS DECIMAL(30,4)) AS taxa_falencia_por_mil,
    CAST(ROUND(1000.0 * a.qtd_inaptidoes_de_ativas / NULLIF(a.estoque_inicial_ativas, 0), 4) AS DECIMAL(30,4)) AS taxa_inaptidao_por_mil,
    m.selic_mensal_pct,
    m.selic_anualizada_media_pct,
    m.ipca_mensal_pct,
    m.ipca_acumulado_12m_pct,
    m.juro_real_ex_post_anual_pct,
    m.selic_anualizada_lag_1m,
    m.selic_anualizada_lag_3m,
    m.selic_anualizada_lag_6m,
    m.selic_anualizada_lag_12m,
    NOW() AS data_carga_etl,
    -- Colunas novas (2026-11-14), no fim para não deslocar as existentes.
    a.qtd_reativacoes,
    a.qtd_entradas_estoque,
    a.qtd_saidas_estoque,
    a.qtd_baixas_de_ativas,
    a.qtd_suspensoes,
    a.qtd_falencias_de_ativas,
    a.qtd_inaptidoes_de_ativas
FROM agregado_setorial a
LEFT JOIN tcc_receita.gold.fato_indicadores_macro_mes m
    ON a.mes_referencia = m.mes_referencia;
