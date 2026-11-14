-- Gold | fato_empresa_mes
-- Grão: uma linha por empresa (cnpj_raiz, representada pela matriz canônica) × mês de referência.
-- Fonte única: a linha do tempo da empresa em silver_final.dim_empresa. NÃO lê a OBT de eventos.
-- Spec: docs/specs/features/2026-11-14-semantica-eventos-e-fluxos.md (seção 2.4).
--
-- Relógio: mes_referencia = snapshot da Receita (mês de detecção). "Mês anterior" = mês calendário M−1;
-- se a dimensão não tem versão cobrindo M−1, a empresa não existia em M−1.
-- Baseline = primeiro mês da dimensão (carga inicial): estoque de abertura, nenhuma flag de fluxo.
-- Linhas guardadas: ativa no início ou no fim do mês, abertura, ou mudança de situação.
-- Identidade (vale linha a linha, portanto em qualquer soma, fora do baseline):
--     ativa_fim_mes = ativa_inicio_mes + entrou_estoque_mes − saiu_estoque_mes

CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

-- Schema novo (2026-11-14): a tabela antiga é removida pelo script de migração antes do Job.
CREATE TABLE IF NOT EXISTS tcc_receita.gold.fato_empresa_mes (
    mes_referencia STRING COMMENT 'Mês do snapshot da Receita (YYYY-MM), mês de detecção. Partição.',
    data_referencia DATE COMMENT 'Primeiro dia do mês de referência (YYYY-MM-01).',
    cnpj_raiz STRING COMMENT 'Radical de 8 dígitos do CNPJ (empresa).',
    sk_empresa BIGINT COMMENT 'FK para dim_empresa: versão da empresa vigente no mês.',
    sk_grupo_cnae BIGINT COMMENT 'FK para dim_grupo_cnae: grupo de CNAEs da matriz no mês (-1 = sem CNAE).',
    codigo_cnae STRING COMMENT 'CNAE principal da matriz no mês (0000000 = não informado).',
    nome_cnae STRING COMMENT 'Descrição do CNAE principal no mês.',
    classe_cnae STRING COMMENT 'Classe CNAE do CNAE principal no mês.',
    divisao_cnae STRING COMMENT 'Divisão CNAE do CNAE principal no mês.',
    grande_area_cnae STRING COMMENT 'Grande setor econômico (agrupamento analítico) no mês.',
    porte_empresa STRING COMMENT 'Porte analítico (MEI, ME, EPP, DEMAIS, NAO IDENTIFICADO...). Do mês anterior quando saiu_estoque_mes (B23); senão do mês.',
    opcao_pelo_mei BOOLEAN COMMENT 'Optante pelo MEI; do mês anterior quando saiu_estoque_mes (B23). NULL = sem informação (não é FALSE).',
    opcao_pelo_simples BOOLEAN COMMENT 'Optante pelo Simples; do mês anterior quando saiu_estoque_mes (B23). NULL = sem informação (não é FALSE).',
    uf STRING COMMENT 'UF da matriz no mês; NI = não identificada.',
    municipio STRING COMMENT 'Município da matriz no mês; NAO IDENTIFICADO quando ausente.',
    codigo_ibge_municipio STRING COMMENT 'Código IBGE (7 dígitos) do município no mês, quando conhecido.',
    bairro STRING COMMENT 'Localidade/bairro analítico da matriz no mês.',
    situacao_cadastral STRING COMMENT 'Situação cadastral da matriz no fim do mês (ATIVA, BAIXADA, INAPTA, SUSPENSA, NULA, NAO IDENTIFICADO).',
    idade_meses INT COMMENT 'Meses completos entre o mês de início de atividade e o mês de referência; NULL se início ausente ou posterior ao mês.',
    ativa_inicio_mes BOOLEAN COMMENT 'Matriz ATIVA no mês calendário anterior (população em risco). FALSE no baseline e se a empresa não existia em M−1.',
    ativa_fim_mes BOOLEAN COMMENT 'Matriz ATIVA no mês de referência.',
    teve_abertura_mes BOOLEAN COMMENT 'Primeira aparição da empresa na dimensão, em mês posterior ao baseline.',
    teve_baixa_mes BOOLEAN COMMENT 'Existia em M−1 sem estar BAIXADA e está BAIXADA em M.',
    teve_falencia_mes BOOLEAN COMMENT 'teve_baixa_mes com motivo 05, 06 ou 10 (falência / liquidação judicial).',
    teve_inaptidao_mes BOOLEAN COMMENT 'Existia em M−1 sem estar INAPTA e está INAPTA em M.',
    data_carga_etl TIMESTAMP COMMENT 'Momento do processamento na camada Gold.',
    situacao_inicio_mes STRING COMMENT 'Situação cadastral da matriz no mês calendário anterior; NULL se a empresa não existia em M−1.',
    teve_reativacao_mes BOOLEAN COMMENT 'Existia em M−1 sem estar ATIVA e está ATIVA em M.',
    entrou_estoque_mes BOOLEAN COMMENT 'NOT ativa_inicio_mes AND ativa_fim_mes (abertura ou reativação). FALSE no baseline.',
    saiu_estoque_mes BOOLEAN COMMENT 'ativa_inicio_mes AND NOT ativa_fim_mes (baixa, inaptidão, suspensão, nulidade...).',
    teve_suspensao_mes BOOLEAN COMMENT 'Existia em M−1 sem estar SUSPENSA e está SUSPENSA em M.',
    idade_na_baixa_meses INT COMMENT 'Só quando teve_baixa_mes: meses completos entre data_inicio_atividade e a data real da baixa (data_situacao_cadastral). NULL se falta data ou se a baixa é anterior ao início.',
    faixa_idade_baixa STRING COMMENT 'Só quando teve_baixa_mes: 00-01 ano, 01-02 anos, 02-05 anos, 05-10 anos, 10+ anos ou sem data (idade NULL). NULL quando não houve baixa.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Gold: empresa × mês (matriz canônica) com estoque de ativas e fluxos derivados só da linha do tempo da dim_empresa. Guarda apenas linhas ativas no início/fim do mês, aberturas e mudanças de situação.';

-- Parâmetros obrigatórios: o REPLACE WHERE abaixo precisa da janela explícita. Parâmetro vazio NÃO vira
-- "mês anterior" (isso faria o Job rodar verde sobre outro mês): falha aqui, antes de gravar qualquer coisa.
SELECT ASSERT_TRUE(
    '{{mes_referencia_inicial}}' RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
    AND '{{mes_referencia_final}}' RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
    AND '{{mes_referencia_inicial}}' <= '{{mes_referencia_final}}',
    'ERRO [fato_empresa_mes]: informe mes_referencia_inicial e mes_referencia_final no formato YYYY-MM, com inicial <= final (recebido: [{{mes_referencia_inicial}}] a [{{mes_referencia_final}}]).'
);

-- REPLACE WHERE só aceita INSERT INTO e predicado com literais. Reprocessar a janela substitui só os
-- meses da janela; meses anteriores e posteriores ficam intactos.
INSERT INTO tcc_receita.gold.fato_empresa_mes
REPLACE WHERE mes_referencia >= '{{mes_referencia_inicial}}'
          AND mes_referencia <= '{{mes_referencia_final}}'
WITH params AS (
    -- Janela do Job e o mês anterior a ela (necessário para saber a situação em M−1 do primeiro mês).
    SELECT
        '{{mes_referencia_inicial}}' AS mes_ini,
        '{{mes_referencia_final}}' AS mes_fim,
        TO_DATE(CONCAT('{{mes_referencia_inicial}}', '-01')) AS data_ini,
        TO_DATE(CONCAT('{{mes_referencia_final}}', '-01')) AS data_fim,
        DATE_FORMAT(ADD_MONTHS(TO_DATE(CONCAT('{{mes_referencia_inicial}}', '-01')), -1), 'yyyy-MM') AS mes_anterior_janela
),
limites_dim AS (
    -- Baseline = primeiro mês da dimensão (carga inicial, sem eventos).
    -- Último mês = último snapshot carregado: a versão corrente (fim_vigencia NULL) não é projetada
    -- para meses que ainda não existem.
    SELECT
        MIN(inicio_vigencia) AS mes_baseline,
        TO_DATE(CONCAT(MAX(inicio_vigencia), '-01')) AS data_ultimo_mes_dim
    FROM tcc_receita.silver_final.dim_empresa
    WHERE sk_empresa != -1
),
primeira_aparicao AS (
    -- Abertura = mês da primeira versão da empresa na dimensão inteira (não só na janela).
    -- Agregação estreita (2 colunas); só sobram as empresas que aparecem pela primeira vez na janela.
    SELECT cnpj_raiz, MIN(inicio_vigencia) AS mes_primeira_aparicao
    FROM tcc_receita.silver_final.dim_empresa
    WHERE sk_empresa != -1
    GROUP BY cnpj_raiz
    HAVING MIN(inicio_vigencia) >= (SELECT mes_ini FROM params)
       AND MIN(inicio_vigencia) <= (SELECT mes_fim FROM params)
),
versoes_janela AS (
    -- Só versões que intersectam [mês anterior à janela, fim da janela]. A versão que cobre M−1 do
    -- primeiro mês tem fim_vigencia >= mes_ini, então entra; versões encerradas antes disso não importam.
    SELECT
        e.cnpj_raiz,
        e.sk_empresa,
        e.sk_grupo_cnae,
        e.inicio_vigencia,
        e.fim_vigencia,
        e.data_inicio_vigencia,
        e.data_fim_vigencia,
        e.enquadramento_porte_analitico,
        e.opcao_pelo_mei,
        e.opcao_pelo_simples,
        e.uf,
        e.municipio,
        e.codigo_ibge_municipio,
        e.localidade_analitica,
        COALESCE(e.situacao_cadastral_matriz, 'NAO IDENTIFICADO') AS situacao_cadastral,
        -- Código normalizado com 2 dígitos ('5' e '05' são o mesmo motivo).
        LPAD(TRIM(e.codigo_motivo_situacao_cadastral), 2, '0') AS codigo_motivo,
        e.data_inicio_atividade,
        e.data_situacao_cadastral
    FROM tcc_receita.silver_final.dim_empresa e
    CROSS JOIN params p
    WHERE e.sk_empresa != -1
      AND e.inicio_vigencia <= p.mes_fim
      AND COALESCE(e.fim_vigencia, '9999-12') > p.mes_anterior_janela
),
versoes_com_anterior AS (
    -- Para cada versão, a versão imediatamente anterior da mesma empresa (LAG no nível de VERSÃO, não de
    -- mês: ~1 linha por empresa em vez de uma por empresa×mês). Ela só vale como "mês anterior" se
    -- terminar exatamente onde esta começa (fim_vigencia_anterior = inicio_vigencia); senão há um buraco.
    SELECT
        v.*,
        (pa.cnpj_raiz IS NOT NULL) AS eh_primeira_versao,
        LAG(v.fim_vigencia) OVER w AS fim_vigencia_anterior,
        LAG(v.situacao_cadastral) OVER w AS situacao_versao_anterior,
        LAG(v.enquadramento_porte_analitico) OVER w AS porte_versao_anterior,
        LAG(v.opcao_pelo_mei) OVER w AS mei_versao_anterior,
        LAG(v.opcao_pelo_simples) OVER w AS simples_versao_anterior
    FROM versoes_janela v
    LEFT JOIN primeira_aparicao pa
        ON pa.cnpj_raiz = v.cnpj_raiz
       AND pa.mes_primeira_aparicao = v.inicio_vigencia
    WINDOW w AS (PARTITION BY v.cnpj_raiz ORDER BY v.inicio_vigencia)
),
ponte_primaria AS (
    -- CNAE principal de cada grupo de CNAEs (um por grupo).
    SELECT sk_grupo_cnae, sk_cnae
    FROM (
        SELECT sk_grupo_cnae, sk_cnae, ROW_NUMBER() OVER (PARTITION BY sk_grupo_cnae ORDER BY nk_cnae ASC) AS rn
        FROM tcc_receita.silver_final.ponte_cnae
        WHERE cnae_principal = TRUE AND sk_grupo_cnae != -1
    )
    WHERE rn = 1
),
versoes_a_expandir AS (
    -- Versão que começou antes da janela e não está ATIVA (ex.: baixada há anos) não gera linha: em todos
    -- os meses da janela ela não está no estoque e não muda de situação. Descartá-la ANTES de expandir
    -- em meses evita multiplicar ~30 milhões de empresas mortas pelo número de meses.
    -- Intervalo de meses a gerar: [início da versão, fim exclusivo) ∩ [mes_ini, mes_fim] ∩ (até o último snapshot).
    SELECT /*+ BROADCAST(cnae) */
        v.*,
        COALESCE(cnae.codigo_cnae, '0000000') AS codigo_cnae,
        COALESCE(cnae.descricao_cnae, 'NAO INFORMADO / NAO APLICAVEL') AS nome_cnae,
        COALESCE(cnae.nome_classe, 'NAO ESPECIFICADO') AS classe_cnae,
        COALESCE(cnae.nome_divisao, 'NAO ESPECIFICADO') AS divisao_cnae,
        COALESCE(cnae.grande_setor_economico, 'OUTROS / NAO ESPECIFICADO') AS grande_area_cnae,
        GREATEST(v.data_inicio_vigencia, p.data_ini) AS data_primeiro_mes,
        LEAST(COALESCE(ADD_MONTHS(v.data_fim_vigencia, -1), p.data_fim), p.data_fim, l.data_ultimo_mes_dim) AS data_ultimo_mes
    FROM versoes_com_anterior v
    CROSS JOIN params p
    CROSS JOIN limites_dim l
    LEFT JOIN ponte_primaria b ON v.sk_grupo_cnae = b.sk_grupo_cnae
    LEFT JOIN tcc_receita.silver_final.dim_cnae cnae ON COALESCE(b.sk_cnae, -1) = cnae.sk_cnae
    WHERE v.situacao_cadastral = 'ATIVA'
       OR v.inicio_vigencia >= p.mes_ini
),
meses_empresa AS (
    -- Uma linha por empresa × mês coberto pela versão (EXPLODE de uma sequência curta, sem join de intervalo).
    -- O WHERE vem antes do EXPLODE: SEQUENCE com início > fim daria erro.
    SELECT
        v.*,
        EXPLODE(SEQUENCE(v.data_primeiro_mes, v.data_ultimo_mes, INTERVAL 1 MONTH)) AS data_referencia
    FROM versoes_a_expandir v
    WHERE v.data_primeiro_mes <= v.data_ultimo_mes
),
estado_mes AS (
    -- Situação em M−1 (mês calendário):
    --   M depois do início da versão → a própria versão cobria M−1;
    --   M = início da versão → só existia se a versão anterior terminar exatamente em M.
    SELECT
        m.*,
        DATE_FORMAT(m.data_referencia, 'yyyy-MM') AS mes_referencia,
        (DATE_FORMAT(m.data_referencia, 'yyyy-MM') = l.mes_baseline) AS eh_baseline,
        (DATE_FORMAT(m.data_referencia, 'yyyy-MM') = m.inicio_vigencia) AS eh_mes_inicio_versao,
        CASE
            WHEN DATE_FORMAT(m.data_referencia, 'yyyy-MM') > m.inicio_vigencia THEN m.situacao_cadastral
            WHEN m.fim_vigencia_anterior = m.inicio_vigencia THEN m.situacao_versao_anterior
        END AS situacao_inicio_mes
    FROM meses_empresa m
    CROSS JOIN limites_dim l
),
estoque_mes AS (
    -- Estoque: ATIVA no início (= em M−1) e no fim do mês. NULL de situação conta como "não ativa".
    SELECT
        s.*,
        (s.situacao_inicio_mes IS NOT NULL) AS existia_mes_anterior,
        COALESCE(s.situacao_inicio_mes = 'ATIVA', FALSE) AS ativa_inicio_mes,
        (s.situacao_cadastral = 'ATIVA') AS ativa_fim_mes
    FROM estado_mes s
),
fluxos_mes AS (
    -- Fluxos (todos FALSE no baseline: lá não existe M−1, e entrada/abertura são forçadas a FALSE).
    -- Uma transição de situação só existe se a empresa existia em M−1.
    SELECT
        e.*,
        (NOT e.eh_baseline AND e.eh_primeira_versao AND e.eh_mes_inicio_versao) AS teve_abertura_mes,
        (e.existia_mes_anterior AND e.situacao_inicio_mes <> 'ATIVA'    AND e.situacao_cadastral = 'ATIVA')    AS teve_reativacao_mes,
        (NOT e.eh_baseline AND NOT e.ativa_inicio_mes AND e.ativa_fim_mes) AS entrou_estoque_mes,
        (e.ativa_inicio_mes AND NOT e.ativa_fim_mes) AS saiu_estoque_mes,
        (e.existia_mes_anterior AND e.situacao_inicio_mes <> 'BAIXADA'  AND e.situacao_cadastral = 'BAIXADA')  AS teve_baixa_mes,
        (e.existia_mes_anterior AND e.situacao_inicio_mes <> 'INAPTA'   AND e.situacao_cadastral = 'INAPTA')   AS teve_inaptidao_mes,
        (e.existia_mes_anterior AND e.situacao_inicio_mes <> 'SUSPENSA' AND e.situacao_cadastral = 'SUSPENSA') AS teve_suspensao_mes
    FROM estoque_mes e
),
idade_baixa AS (
    -- Idade na baixa = data real da baixa − início de atividade (relógio da data do evento, não do mês de
    -- detecção). Na versão BAIXADA da matriz, data_situacao_cadastral é a data da baixa declarada pela Receita.
    -- Data ausente ou baixa antes do início → NULL (vira a faixa 'sem data', medida no DQ).
    SELECT
        f.*,
        CASE
            WHEN f.teve_baixa_mes
             AND f.data_inicio_atividade IS NOT NULL
             AND f.data_situacao_cadastral >= f.data_inicio_atividade
            THEN CAST(FLOOR(MONTHS_BETWEEN(f.data_situacao_cadastral, f.data_inicio_atividade)) AS INT)
        END AS idade_na_baixa_meses
    FROM fluxos_mes f
)
SELECT
    f.mes_referencia,
    f.data_referencia,
    f.cnpj_raiz,
    f.sk_empresa,
    f.sk_grupo_cnae,
    f.codigo_cnae,
    f.nome_cnae,
    f.classe_cnae,
    f.divisao_cnae,
    f.grande_area_cnae,
    -- B23: ao sair do estoque (ex.: MEI baixado vira "ME" na Receita) vale o atributo de M−1. Saída só
    -- acontece no mês de início de uma versão, então M−1 = a versão anterior (LAG).
    COALESCE(NULLIF(
        CASE WHEN f.saiu_estoque_mes THEN f.porte_versao_anterior ELSE f.enquadramento_porte_analitico END,
        'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS porte_empresa,
    CASE WHEN f.saiu_estoque_mes THEN f.mei_versao_anterior ELSE f.opcao_pelo_mei END AS opcao_pelo_mei,
    CASE WHEN f.saiu_estoque_mes THEN f.simples_versao_anterior ELSE f.opcao_pelo_simples END AS opcao_pelo_simples,
    COALESCE(NULLIF(f.uf, 'NA'), 'NI') AS uf,
    COALESCE(NULLIF(f.municipio, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS municipio,
    f.codigo_ibge_municipio,
    COALESCE(NULLIF(f.localidade_analitica, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS bairro,
    f.situacao_cadastral,
    -- Idade nunca negativa: início de atividade depois do mês de referência → NULL.
    CASE
        WHEN f.data_inicio_atividade IS NOT NULL
         AND TRUNC(f.data_inicio_atividade, 'MM') <= f.data_referencia
        THEN CAST(FLOOR(MONTHS_BETWEEN(f.data_referencia, TRUNC(f.data_inicio_atividade, 'MM'))) AS INT)
    END AS idade_meses,
    f.ativa_inicio_mes,
    f.ativa_fim_mes,
    f.teve_abertura_mes,
    f.teve_baixa_mes,
    -- Falência/liquidação judicial por código de motivo da Receita (05, 06, 10), não por texto.
    (f.teve_baixa_mes AND COALESCE(f.codigo_motivo IN ('05', '06', '10'), FALSE)) AS teve_falencia_mes,
    f.teve_inaptidao_mes,
    NOW() AS data_carga_etl,
    f.situacao_inicio_mes,
    f.teve_reativacao_mes,
    f.entrou_estoque_mes,
    f.saiu_estoque_mes,
    f.teve_suspensao_mes,
    f.idade_na_baixa_meses,
    -- Faixas com dois dígitos: a ordem alfabética já é a ordem de idade.
    CASE
        WHEN NOT f.teve_baixa_mes THEN NULL
        WHEN f.idade_na_baixa_meses IS NULL THEN 'sem data'
        WHEN f.idade_na_baixa_meses < 12 THEN '00-01 ano'
        WHEN f.idade_na_baixa_meses < 24 THEN '01-02 anos'
        WHEN f.idade_na_baixa_meses < 60 THEN '02-05 anos'
        WHEN f.idade_na_baixa_meses < 120 THEN '05-10 anos'
        ELSE '10+ anos'
    END AS faixa_idade_baixa
FROM idade_baixa f
-- Filtro DEPOIS das flags: empresas fora do estoque e sem mudança (ex.: baixadas há anos) não geram linha.
WHERE f.ativa_inicio_mes
   OR f.ativa_fim_mes
   OR f.teve_abertura_mes
   OR (f.existia_mes_anterior AND f.situacao_inicio_mes <> f.situacao_cadastral);
