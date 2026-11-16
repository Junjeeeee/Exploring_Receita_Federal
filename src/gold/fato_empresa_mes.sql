-- Gold | fato_empresa_mes
-- Grão: uma linha por empresa (cnpj_raiz, representada pela matriz canônica) × mês de referência.
-- Fonte: a linha do tempo da empresa em silver_final.dim_empresa. NÃO lê a OBT de eventos. Exceção estreita:
-- silver_intermediate.dados_simples, só para o porte da baixa na primeira aparição já BAIXADA (D-17b).
-- Spec: docs/specs/features/2026-11-14-semantica-eventos-e-fluxos.md (seção 2.4).
--
-- Relógio: mes_referencia = snapshot da Receita (mês de detecção). "Mês anterior" = mês calendário M−1;
-- se a dimensão não tem versão cobrindo M−1, a empresa não existia em M−1.
-- Baseline = primeiro mês da dimensão (carga inicial): estoque de abertura, nenhuma flag de fluxo.
-- Primeira aparição pós-baseline já fora de ATIVA (abriu e fechou entre dois snapshots) = abertura E o evento
-- da situação em que apareceu (baixa, inaptidão ou suspensão) no mesmo mês (D-17).
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
    porte_empresa STRING COMMENT 'Porte analítico (MEI, ME, EPP, DEMAIS, NAO IDENTIFICADO...). Do mês anterior quando saiu_estoque_mes (B23); MEI na baixa de primeira aparição quando as datas do Simples mostram MEI na data da baixa (D-17b); senão do mês.',
    opcao_pelo_mei BOOLEAN COMMENT 'Optante pelo MEI; do mês anterior quando saiu_estoque_mes (B23); TRUE na baixa de primeira aparição com MEI na data da baixa (D-17b). NULL = sem informação (não é FALSE).',
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
    teve_baixa_mes BOOLEAN COMMENT 'Está BAIXADA em M e: existia em M−1 sem estar BAIXADA, ou é a primeira aparição pós-baseline (abertura e baixa no mesmo mês, D-17).',
    teve_falencia_mes BOOLEAN COMMENT 'teve_baixa_mes com motivo 05, 06 ou 10 (falência / liquidação judicial).',
    teve_inaptidao_mes BOOLEAN COMMENT 'Está INAPTA em M e: existia em M−1 sem estar INAPTA, ou é a primeira aparição pós-baseline (D-17).',
    data_carga_etl TIMESTAMP COMMENT 'Momento do processamento na camada Gold.',
    situacao_inicio_mes STRING COMMENT 'Situação cadastral da matriz no mês calendário anterior; NULL se a empresa não existia em M−1.',
    teve_reativacao_mes BOOLEAN COMMENT 'Existia em M−1 sem estar ATIVA e está ATIVA em M.',
    entrou_estoque_mes BOOLEAN COMMENT 'NOT ativa_inicio_mes AND ativa_fim_mes (abertura ou reativação). FALSE no baseline.',
    saiu_estoque_mes BOOLEAN COMMENT 'ativa_inicio_mes AND NOT ativa_fim_mes (baixa, inaptidão, suspensão, nulidade...).',
    teve_suspensao_mes BOOLEAN COMMENT 'Está SUSPENSA em M e: existia em M−1 sem estar SUSPENSA, ou é a primeira aparição pós-baseline (D-17).',
    idade_na_baixa_meses INT COMMENT 'Só quando teve_baixa_mes: meses completos entre data_inicio_atividade e a data real da baixa (data_situacao_cadastral). NULL se falta data ou se a baixa é anterior ao início.',
    faixa_idade_baixa STRING COMMENT 'Só quando teve_baixa_mes: 00-01 ano, 01-02 anos, 02-05 anos, 05-10 anos, 10+ anos ou sem data (idade NULL). NULL quando não houve baixa.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Gold: empresa × mês (matriz canônica) com estoque de ativas e fluxos derivados só da linha do tempo da dim_empresa. Guarda apenas linhas ativas no início/fim do mês, aberturas e mudanças de situação.';

-- D-17/D-17b mudaram regras de colunas sem mudar o schema: CREATE TABLE IF NOT EXISTS não atualiza comentário de
-- tabela que já existe, então os comentários dessas colunas são reescritos aqui (idempotente).
ALTER TABLE tcc_receita.gold.fato_empresa_mes ALTER COLUMN teve_baixa_mes
    COMMENT 'Está BAIXADA em M e: existia em M−1 sem estar BAIXADA, ou é a primeira aparição pós-baseline (abertura e baixa no mesmo mês, D-17).';
ALTER TABLE tcc_receita.gold.fato_empresa_mes ALTER COLUMN teve_inaptidao_mes
    COMMENT 'Está INAPTA em M e: existia em M−1 sem estar INAPTA, ou é a primeira aparição pós-baseline (D-17).';
ALTER TABLE tcc_receita.gold.fato_empresa_mes ALTER COLUMN teve_suspensao_mes
    COMMENT 'Está SUSPENSA em M e: existia em M−1 sem estar SUSPENSA, ou é a primeira aparição pós-baseline (D-17).';
ALTER TABLE tcc_receita.gold.fato_empresa_mes ALTER COLUMN porte_empresa
    COMMENT 'Porte analítico (MEI, ME, EPP, DEMAIS, NAO IDENTIFICADO...). Do mês anterior quando saiu_estoque_mes (B23); MEI na baixa de primeira aparição quando as datas do Simples mostram MEI na data da baixa (D-17b); senão do mês.';
ALTER TABLE tcc_receita.gold.fato_empresa_mes ALTER COLUMN opcao_pelo_mei
    COMMENT 'Optante pelo MEI; do mês anterior quando saiu_estoque_mes (B23); TRUE na baixa de primeira aparição com MEI na data da baixa (D-17b). NULL = sem informação (não é FALSE).';

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
mei_na_baixa AS (
    -- Porte da baixa na primeira aparição já BAIXADA (D-17b). Não há versão anterior (B23 não se aplica) e a
    -- Receita tira a marca de MEI na baixa: o MEI aparece como ME. Decide-se pelas DATAS do Simples, nunca
    -- pela natureza jurídica: era MEI na data da baixa se tem opção pelo MEI válida <= data da baixa e a
    -- exclusão do MEI é nula ou não anterior à data da baixa (tolerância 0 dia: na amostra, a exclusão é
    -- igual à data da baixa em 100% dos casos). Data inválida/ausente = sem informação (não vira MEI).
    -- Só as empresas de primeira aparição já BAIXADA entram no join com dados_simples (tabela grande); só
    -- versões do Simples conhecidas até o mês da aparição (sem olhar o futuro).
    SELECT /*+ BROADCAST(nb) */ nb.cnpj_raiz
    FROM (
        SELECT e.cnpj_raiz, e.inicio_vigencia, e.data_situacao_cadastral AS data_baixa
        FROM tcc_receita.silver_final.dim_empresa e
        JOIN primeira_aparicao pa
          ON pa.cnpj_raiz = e.cnpj_raiz
         AND pa.mes_primeira_aparicao = e.inicio_vigencia
        CROSS JOIN limites_dim l
        WHERE e.sk_empresa != -1
          AND e.situacao_cadastral_matriz = 'BAIXADA'
          AND e.data_situacao_cadastral IS NOT NULL
          AND e.inicio_vigencia > l.mes_baseline
    ) nb
    JOIN tcc_receita.silver_intermediate.dados_simples s
      ON s.cnpj_basico = nb.cnpj_raiz
     AND s.valid_since <= nb.inicio_vigencia
    WHERE CAST(TRY_TO_TIMESTAMP(s.data_opcao_mei, 'yyyyMMdd') AS DATE) <= nb.data_baixa
      AND (CAST(TRY_TO_TIMESTAMP(s.data_exclusao_mei, 'yyyyMMdd') AS DATE) IS NULL
           OR CAST(TRY_TO_TIMESTAMP(s.data_exclusao_mei, 'yyyyMMdd') AS DATE) >= nb.data_baixa)
    GROUP BY nb.cnpj_raiz
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
-- Dois caminhos (spec 2026-11-16-otimizacao-log-fato-dashboard.md, §3.3). O "mês anterior" de uma versão só
-- importa no mês em que ela começa: nos meses seguintes, M−1 é coberto pela própria versão. Por isso:
--   * versões NOVAS (começam na janela, depois do baseline) buscam a versão anterior por join de igualdade
--     (a anterior termina exatamente onde a nova começa) — caminho pequeno, só o que mudou;
--   * versões ANTIGAS (começaram antes da janela, ou no baseline) não precisam de anterior: no baseline não
--     existe M−1, e antes da janela o mês de início não é gerado. Elas são a maioria (~todas as ativas) e não
--     passam por ordenação nenhuma.
-- Corte A: versão não ATIVA só gera linha no mês em que começa (nos meses seguintes ela não está ativa nem
-- muda de situação, e o WHERE final a descartaria). Então versão antiga não ATIVA não gera nada e é tirada já
-- aqui, antes do desdobramento em meses (ex.: dezenas de milhões de empresas baixadas há anos).
novas AS (
    SELECT v.*
    FROM versoes_janela v
    CROSS JOIN params p
    CROSS JOIN limites_dim l
    WHERE v.inicio_vigencia BETWEEN p.mes_ini AND p.mes_fim
      AND v.inicio_vigencia > l.mes_baseline
),
anteriores AS (
    -- Candidatas a versão anterior de uma nova: terminam dentro da janela. Só colunas estreitas.
    SELECT
        v.cnpj_raiz,
        v.fim_vigencia,
        v.situacao_cadastral AS situacao_versao_anterior,
        v.enquadramento_porte_analitico AS porte_versao_anterior,
        v.opcao_pelo_mei AS mei_versao_anterior,
        v.opcao_pelo_simples AS simples_versao_anterior
    FROM versoes_janela v
    CROSS JOIN params p
    WHERE v.fim_vigencia BETWEEN p.mes_ini AND p.mes_fim
),
novas_com_anterior AS (
    -- Versão anterior contígua (fim = início da nova). Sem ela (buraco, ou primeira versão), as colunas ficam
    -- NULL e a empresa "não existia em M−1". Versões da dim não se sobrepõem (DQ de SK), então casa no máximo uma.
    -- Corte C (D-17b): a marca de "era MEI na baixa" entra aqui, no nível de versão, ANTES do desdobramento em
    -- meses; só vale para a primeira versão da empresa (mesma condição do join de antes).
    SELECT
        n.*,
        a.fim_vigencia AS fim_vigencia_anterior,
        a.situacao_versao_anterior,
        a.porte_versao_anterior,
        a.mei_versao_anterior,
        a.simples_versao_anterior,
        (pa.cnpj_raiz IS NOT NULL) AS eh_primeira_versao,
        (pa.cnpj_raiz IS NOT NULL AND mb.cnpj_raiz IS NOT NULL) AS eh_mei_na_baixa
    FROM novas n
    LEFT JOIN anteriores a
        ON a.cnpj_raiz = n.cnpj_raiz
       AND a.fim_vigencia = n.inicio_vigencia
    LEFT JOIN primeira_aparicao pa
        ON pa.cnpj_raiz = n.cnpj_raiz
       AND pa.mes_primeira_aparicao = n.inicio_vigencia
    LEFT JOIN mei_na_baixa mb
        ON mb.cnpj_raiz = n.cnpj_raiz
),
antigas AS (
    -- Começaram antes da janela ou no baseline: as colunas de "anterior" nunca são usadas (ficam NULL), não
    -- são abertura e não são MEI na baixa. Corte A: só as ATIVA geram linha.
    SELECT
        v.*,
        CAST(NULL AS STRING) AS fim_vigencia_anterior,
        CAST(NULL AS STRING) AS situacao_versao_anterior,
        CAST(NULL AS STRING) AS porte_versao_anterior,
        CAST(NULL AS BOOLEAN) AS mei_versao_anterior,
        CAST(NULL AS BOOLEAN) AS simples_versao_anterior,
        FALSE AS eh_primeira_versao,
        FALSE AS eh_mei_na_baixa
    FROM versoes_janela v
    CROSS JOIN params p
    CROSS JOIN limites_dim l
    WHERE (v.inicio_vigencia < p.mes_ini OR v.inicio_vigencia = l.mes_baseline)
      AND v.situacao_cadastral = 'ATIVA'
),
versoes_com_anterior AS (
    -- Mesmas colunas, na mesma ordem, nos dois caminhos.
    SELECT * FROM novas_com_anterior
    UNION ALL
    SELECT * FROM antigas
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
    -- Intervalo de meses a gerar: [início da versão, fim exclusivo) ∩ [mes_ini, mes_fim] ∩ (até o último snapshot).
    -- Versão não ATIVA (só chega aqui se for nova, Corte A): um mês só, o de início.
    SELECT /*+ BROADCAST(cnae) */
        v.*,
        COALESCE(cnae.codigo_cnae, '0000000') AS codigo_cnae,
        COALESCE(cnae.descricao_cnae, 'NAO INFORMADO / NAO APLICAVEL') AS nome_cnae,
        COALESCE(cnae.nome_classe, 'NAO ESPECIFICADO') AS classe_cnae,
        COALESCE(cnae.nome_divisao, 'NAO ESPECIFICADO') AS divisao_cnae,
        COALESCE(cnae.grande_setor_economico, 'OUTROS / NAO ESPECIFICADO') AS grande_area_cnae,
        GREATEST(v.data_inicio_vigencia, p.data_ini) AS data_primeiro_mes,
        CASE
            WHEN v.situacao_cadastral = 'ATIVA'
            THEN LEAST(COALESCE(ADD_MONTHS(v.data_fim_vigencia, -1), p.data_fim), p.data_fim, l.data_ultimo_mes_dim)
            ELSE v.data_inicio_vigencia
        END AS data_ultimo_mes
    FROM versoes_com_anterior v
    CROSS JOIN params p
    CROSS JOIN limites_dim l
    LEFT JOIN ponte_primaria b ON v.sk_grupo_cnae = b.sk_grupo_cnae
    LEFT JOIN tcc_receita.silver_final.dim_cnae cnae ON COALESCE(b.sk_cnae, -1) = cnae.sk_cnae
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
        (s.situacao_cadastral = 'ATIVA') AS ativa_fim_mes,
        -- Primeira aparição da empresa na dimensão, depois do baseline (= abertura).
        (NOT s.eh_baseline AND s.eh_primeira_versao AND s.eh_mes_inicio_versao) AS eh_primeira_aparicao
    FROM estado_mes s
),
fluxos_mes AS (
    -- Fluxos (todos FALSE no baseline: lá não existe M−1, e entrada/abertura são forçadas a FALSE).
    -- Transição de situação: a empresa existia em M−1 numa situação diferente, OU é a primeira aparição
    -- (pós-baseline) já nessa situação (D-17): quem abre e fecha entre dois snapshots aparece já BAIXADA e
    -- conta como abertura E baixa no mesmo mês. Ela nunca esteve ATIVA num snapshot, então não entra nem sai
    -- do estoque (a identidade de ativas não muda) e não é "baixa de ativa" (fica fora das taxas por mil).
    SELECT
        e.*,
        e.eh_primeira_aparicao AS teve_abertura_mes,
        (e.existia_mes_anterior AND e.situacao_inicio_mes <> 'ATIVA'    AND e.situacao_cadastral = 'ATIVA')    AS teve_reativacao_mes,
        (NOT e.eh_baseline AND NOT e.ativa_inicio_mes AND e.ativa_fim_mes) AS entrou_estoque_mes,
        (e.ativa_inicio_mes AND NOT e.ativa_fim_mes) AS saiu_estoque_mes,
        (e.situacao_cadastral = 'BAIXADA'
         AND ((e.existia_mes_anterior AND e.situacao_inicio_mes <> 'BAIXADA')  OR e.eh_primeira_aparicao)) AS teve_baixa_mes,
        (e.situacao_cadastral = 'INAPTA'
         AND ((e.existia_mes_anterior AND e.situacao_inicio_mes <> 'INAPTA')   OR e.eh_primeira_aparicao)) AS teve_inaptidao_mes,
        (e.situacao_cadastral = 'SUSPENSA'
         AND ((e.existia_mes_anterior AND e.situacao_inicio_mes <> 'SUSPENSA') OR e.eh_primeira_aparicao)) AS teve_suspensao_mes
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
    -- acontece no mês de início de uma versão, então M−1 = a versão anterior (LAG). Na primeira aparição já
    -- BAIXADA (D-17) não há M−1: vale 'MEI' se as datas do Simples mostram que era MEI na data da baixa
    -- (eh_mei_na_baixa, D-17b, marcado na versão); senão, o porte da própria versão.
    COALESCE(NULLIF(
        CASE
            WHEN f.saiu_estoque_mes THEN f.porte_versao_anterior
            WHEN f.eh_mei_na_baixa AND f.teve_abertura_mes AND f.teve_baixa_mes THEN 'MEI'
            ELSE f.enquadramento_porte_analitico
        END,
        'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS porte_empresa,
    CASE
        WHEN f.saiu_estoque_mes THEN f.mei_versao_anterior
        WHEN f.eh_mei_na_baixa AND f.teve_abertura_mes AND f.teve_baixa_mes THEN TRUE
        ELSE f.opcao_pelo_mei
    END AS opcao_pelo_mei,
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
