CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE TABLE IF NOT EXISTS tcc_receita.gold.fato_empresa_mes (
    mes_referencia STRING COMMENT 'Partição mensal (YYYY-MM).',
    data_referencia DATE COMMENT 'Primeiro dia do mês (YYYY-MM-01).',
    cnpj_raiz STRING COMMENT 'Radical de 8 dígitos do CNPJ.',
    sk_empresa BIGINT COMMENT 'FK da versão vigente da empresa no mês.',
    sk_grupo_cnae BIGINT COMMENT 'FK do grupo de CNAEs vigente no mês.',
    codigo_cnae STRING COMMENT 'Código do CNAE principal no mês.',
    nome_cnae STRING COMMENT 'Descrição do CNAE principal no mês.',
    classe_cnae STRING COMMENT 'Classe CNAE no mês.',
    divisao_cnae STRING COMMENT 'Divisão CNAE no mês.',
    grande_area_cnae STRING COMMENT 'Grande setor econômico no mês.',
    porte_empresa STRING COMMENT 'Enquadramento de porte analítico no mês (MEI, ME, EPP, DEMAIS, NAO IDENTIFICADO).',
    opcao_pelo_mei BOOLEAN COMMENT 'Optante pelo MEI no mês.',
    opcao_pelo_simples BOOLEAN COMMENT 'Optante pelo Simples Nacional no mês.',
    uf STRING COMMENT 'UF da sede matriz no mês.',
    municipio STRING COMMENT 'Município da sede matriz no mês.',
    codigo_ibge_municipio STRING COMMENT 'Código IBGE do município no mês.',
    bairro STRING COMMENT 'Localidade/bairro analítico no mês.',
    situacao_cadastral STRING COMMENT 'Situação cadastral da matriz no mês (ATIVA, BAIXADA, INAPTA, SUSPENSA, NULA).',
    idade_meses INT COMMENT 'Idade da empresa em meses completos até o mês de referência.',
    ativa_inicio_mes BOOLEAN COMMENT 'TRUE se a empresa já estava ATIVA no mês imediatamente anterior (População em Risco no início do mês).',
    ativa_fim_mes BOOLEAN COMMENT 'TRUE se a empresa terminou o mês de referência com situação ATIVA.',
    teve_abertura_mes BOOLEAN COMMENT 'TRUE se a empresa nasceu neste mês de referência.',
    teve_baixa_mes BOOLEAN COMMENT 'TRUE se a empresa sofreu baixa cadastral neste mês.',
    teve_falencia_mes BOOLEAN COMMENT 'TRUE se a empresa teve evento de falência/liquidação neste mês.',
    teve_inaptidao_mes BOOLEAN COMMENT 'TRUE se a empresa tornou-se inapta neste mês.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na camada Gold.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Gold: Estoque mensal de empresas (grão: cnpj_raiz x mes_referencia) para cálculo de população em risco, taxas de mortalidade e concorrência.';

-- REPLACE WHERE só aceita INSERT INTO e predicado com literais: o Job sempre passa YYYY-MM.
INSERT INTO tcc_receita.gold.fato_empresa_mes
REPLACE WHERE mes_referencia >= '{{mes_referencia_inicial}}'
          AND mes_referencia <= '{{mes_referencia_final}}'
WITH params AS (
    SELECT 
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_fim,
        -- Recua 1 mês na leitura para calcular corretamente ativa_inicio_mes via LAG na fronteira inicial
        DATE_FORMAT(ADD_MONTHS(TO_DATE(CONCAT(COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')), '-01'), 'yyyy-MM-dd'), -1), 'yyyy-MM') AS mes_ini_lookback
),
meses_alvo AS (
    SELECT t.mes_referencia, t.data_referencia
    FROM tcc_receita.silver_final.dim_tempo t
    CROSS JOIN params p
    WHERE t.sk_tempo != -1
      AND t.mes_referencia >= p.mes_ini_lookback
      AND t.mes_referencia <= p.mes_fim
),
ponte_primaria AS (
    SELECT sk_grupo_cnae, sk_cnae 
    FROM (
        SELECT sk_grupo_cnae, sk_cnae, ROW_NUMBER() OVER (PARTITION BY sk_grupo_cnae ORDER BY nk_cnae ASC) AS rn
        FROM tcc_receita.silver_final.ponte_cnae 
        WHERE cnae_principal = TRUE AND sk_grupo_cnae != -1
    )
    WHERE rn = 1
),
eventos_mes AS (
    SELECT 
        mes_referencia,
        cnpj_raiz,
        MAX(CASE WHEN categoria_macro_evento = 'ABERTURA' THEN TRUE ELSE FALSE END) AS teve_abertura_mes,
        MAX(CASE WHEN situacao_cadastral_posterior = 'BAIXADA' OR categoria_macro_evento IN ('BAIXA VOLUNTARIA / OUTRAS', 'FECHAMENTO POR FALENCIA', 'EXTINCAO POR OBITO (MEI)', 'EXTINCAO POR LIQUIDACAO') THEN TRUE ELSE FALSE END) AS teve_baixa_mes,
        MAX(CASE WHEN categoria_macro_evento IN ('FECHAMENTO POR FALENCIA', 'EXTINCAO POR LIQUIDACAO') THEN TRUE ELSE FALSE END) AS teve_falencia_mes,
        MAX(CASE WHEN situacao_cadastral_posterior = 'INAPTA' OR categoria_macro_evento LIKE 'INAPTIDAO%' THEN TRUE ELSE FALSE END) AS teve_inaptidao_mes
    FROM tcc_receita.gold.obt_dashboard_eventos
    CROSS JOIN params p
    WHERE mes_referencia >= p.mes_ini AND mes_referencia <= p.mes_fim
    GROUP BY mes_referencia, cnpj_raiz
),
expansao_mensal AS (
    SELECT /*+ BROADCAST(m, cnae) */
        m.mes_referencia,
        m.data_referencia,
        e.cnpj_raiz,
        e.sk_empresa,
        e.sk_grupo_cnae,
        COALESCE(cnae.codigo_cnae, '0000000') AS codigo_cnae,
        COALESCE(cnae.descricao_cnae, 'NAO INFORMADO / NAO APLICAVEL') AS nome_cnae,
        COALESCE(cnae.nome_classe, 'NAO ESPECIFICADO') AS classe_cnae,
        COALESCE(cnae.nome_divisao, 'NAO ESPECIFICADO') AS divisao_cnae,
        COALESCE(cnae.grande_setor_economico, 'OUTROS / NAO ESPECIFICADO') AS grande_area_cnae,
        COALESCE(NULLIF(e.enquadramento_porte_analitico, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS porte_empresa,
        COALESCE(e.opcao_pelo_mei, FALSE) AS opcao_pelo_mei,
        COALESCE(e.opcao_pelo_simples, FALSE) AS opcao_pelo_simples,
        COALESCE(NULLIF(e.uf, 'NA'), 'NI') AS uf,
        COALESCE(NULLIF(e.municipio, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS municipio,
        e.codigo_ibge_municipio,
        COALESCE(NULLIF(e.localidade_analitica, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS bairro,
        COALESCE(e.situacao_cadastral_matriz, 'NAO IDENTIFICADO') AS situacao_cadastral,
        CASE 
            WHEN e.data_inicio_atividade IS NOT NULL 
            THEN CAST(FLOOR(MONTHS_BETWEEN(m.data_referencia, TRUNC(e.data_inicio_atividade, 'MM'))) AS INT)
            ELSE NULL 
        END AS idade_meses,
        (e.situacao_cadastral_matriz = 'ATIVA') AS ativa_fim_mes,
        LAG(e.situacao_cadastral_matriz = 'ATIVA', 1, FALSE) OVER (
            PARTITION BY e.cnpj_raiz ORDER BY m.mes_referencia ASC
        ) AS ativa_inicio_mes
    FROM tcc_receita.silver_final.dim_empresa e
    INNER JOIN meses_alvo m
        ON m.mes_referencia >= e.inicio_vigencia
       AND m.mes_referencia < COALESCE(e.fim_vigencia, '9999-12')
    LEFT JOIN ponte_primaria b ON e.sk_grupo_cnae = b.sk_grupo_cnae
    LEFT JOIN tcc_receita.silver_final.dim_cnae cnae ON COALESCE(b.sk_cnae, -1) = cnae.sk_cnae
    WHERE e.sk_empresa != -1
)
SELECT 
    em.mes_referencia,
    em.data_referencia,
    em.cnpj_raiz,
    em.sk_empresa,
    em.sk_grupo_cnae,
    em.codigo_cnae,
    em.nome_cnae,
    em.classe_cnae,
    em.divisao_cnae,
    em.grande_area_cnae,
    em.porte_empresa,
    em.opcao_pelo_mei,
    em.opcao_pelo_simples,
    em.uf,
    em.municipio,
    em.codigo_ibge_municipio,
    em.bairro,
    em.situacao_cadastral,
    em.idade_meses,
    em.ativa_inicio_mes,
    em.ativa_fim_mes,
    COALESCE(ev.teve_abertura_mes, FALSE) AS teve_abertura_mes,
    COALESCE(ev.teve_baixa_mes, FALSE) AS teve_baixa_mes,
    COALESCE(ev.teve_falencia_mes, FALSE) AS teve_falencia_mes,
    COALESCE(ev.teve_inaptidao_mes, FALSE) AS teve_inaptidao_mes,
    NOW() AS data_carga_etl
FROM expansao_mensal em
CROSS JOIN params p
LEFT JOIN eventos_mes ev
    ON em.mes_referencia = ev.mes_referencia
   AND em.cnpj_raiz = ev.cnpj_raiz
WHERE em.mes_referencia >= p.mes_ini
  AND em.mes_referencia <= p.mes_fim;