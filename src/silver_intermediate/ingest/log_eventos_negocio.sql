-- Silver Intermediate: log consolidado de eventos de negócio.
-- Spec: docs/specs/features/2026-11-14-semantica-eventos-e-fluxos.md (seções 2.1, 2.2 e 2.4).
--
-- IMPORTANTE: substitua os dois parâmetros por literais YYYY-MM antes da execução.
-- Exemplo: '{{mes_referencia_inicial}}' -> '2026-01'. Não deixe os parâmetros vazios.
-- O predicado REPLACE WHERE do Databricks aceita apenas expressões restritas e valores literais.
-- Para o padrão do projeto, resolva ambos para o mês anterior quando nenhum intervalo for informado.
--
-- Dois relógios (D-13):
--   * mes_referencia = MÊS DE DETECÇÃO: primeiro snapshot em que o fato aparece (valid_since da versão).
--     É o relógio dos fluxos mensais e sempre cai dentro da janela [inicial, final].
--   * data_evento    = data declarada pela Receita, quando a fonte tem (pode ser NULL). Serve a
--     tempo-até-evento; nunca é usada para somar fluxo mensal.
-- Baseline: o menor valid_since de cada tabela SCD2 é a carga inicial (2023-05). Nele tudo aparece pela
-- primeira vez, então nenhuma "primeira aparição" vira evento ali (e não há versão anterior para transição).
--
-- Como as transições são detectadas: por tabela SCD2, LAG(...) OVER (PARTITION BY pk ORDER BY valid_since)
-- traz os atributos da versão anterior; "não tem anterior" (LAG de valid_since nulo) diz que a versão é a
-- primeira da pk. SUPOSIÇÃO: as versões de uma pk são contíguas (valid_until da anterior = valid_since da
-- seguinte). Vale por construção nas SCD2 deste projeto (valid_until = LEAD(valid_since)) e é verificada pelo
-- DQ de contiguidade de cada SCD2.
--
-- Desempenho (spec 2026-11-16-otimizacao-log-fato-dashboard.md, §3.1): o LAG NÃO roda sobre o histórico
-- inteiro. Antes dele, cada SCD2 é reduzida a:
--   (a) versões que começam na janela, depois do baseline (são as que podem gerar evento), e
--   (b) versões que TERMINAM na janela (valid_until dentro dela) — a anterior de uma versão (a) termina
--       exatamente onde ela começa, então está aqui.
-- Sobre esse conjunto, o LAG de cada versão (a) devolve a mesma anterior que devolveria na tabela inteira;
-- e se o LAG não acha anterior, a versão é mesmo a primeira da pk (se houvesse anterior, ela terminaria na
-- janela e estaria no conjunto). Versões que começam no baseline nunca geram evento, por isso ficam de fora.
--
-- cnpj_basico: cada evento grava o cnpj_basico da PRÓPRIA linha da SCD2 de origem (não é extraído da pk).
-- O fato_mudanca_estado usa essa coluna em vez de reler as quatro SCD2 para descobrir o CNPJ.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.log_eventos_negocio (
    pk STRING COMMENT 'Chave lógica da entidade ou da relação, conforme a granularidade da tabela de origem.',
    entidade_tipo STRING COMMENT 'Granularidade da chave: EMPRESA, ESTABELECIMENTO, SOCIO ou TRIBUTARIO.',
    evento STRING COMMENT 'Tipo do evento de negócio detectado.',
    valor_antigo STRING COMMENT 'Estado do atributo antes da transição; NULL na primeira aparição da entidade após o baseline.',
    valor_novo STRING COMMENT 'Estado do atributo após a transição. Em MUDOU ESTADO CADASTRAL, só a descrição da situação (sem o motivo).',
    informacao_adicional STRING COMMENT 'Metadado complementar: descrição do motivo (estado cadastral) ou qualificação (saída de sócio). Nunca contém documentos.',
    mes_referencia STRING COMMENT 'Mês de DETECÇÃO do evento (YYYY-MM): primeiro snapshot em que o fato aparece. Relógio dos fluxos mensais.',
    gerado_em TIMESTAMP COMMENT 'Timestamp de processamento do evento.',
    data_evento DATE COMMENT 'Data declarada pela Receita (início de atividade, situação cadastral, opção/exclusão MEI/Simples, entrada do sócio); NULL quando a fonte não tem. Não usar para somar fluxo mensal.',
    codigo_motivo STRING COMMENT 'Código do motivo da situação cadastral da versão nova; só em MUDOU ESTADO CADASTRAL (NULL nos demais).',
    eh_matriz BOOLEAN COMMENT 'Entidade ESTABELECIMENTO: identificador_matriz_filial = 1 na versão nova. EMPRESA NASCEU: TRUE. Demais (EMPRESA, TRIBUTARIO, SOCIO): NULL, não se aplica.',
    cnpj_basico STRING COMMENT 'CNPJ raiz (8 dígitos) copiado da coluna cnpj_basico da linha da SCD2 de origem (nunca extraído da pk). Coluna nova (fase 3): tabela antiga sem ela é removida pela migração.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Silver Intermediate: log de transições cadastrais, societárias e tributárias. mes_referencia = mês de detecção; data_evento = data declarada pela Receita. Ordem das colunas: pk, entidade_tipo, evento, valor_antigo, valor_novo, informacao_adicional, mes_referencia, gerado_em, data_evento, codigo_motivo, eh_matriz, cnpj_basico.';

WITH params AS (
    SELECT
        CAST('{{mes_referencia_inicial}}' AS STRING) AS mes_ini,
        CAST('{{mes_referencia_final}}' AS STRING) AS mes_fim,
        -- Baseline (carga inicial) de cada SCD2 = o menor valid_since da própria tabela (leitura de uma coluna).
        (SELECT MIN(valid_since) FROM tcc_receita.silver_intermediate.estabelecimentos) AS baseline_est,
        (SELECT MIN(valid_since) FROM tcc_receita.silver_intermediate.empresas) AS baseline_emp,
        (SELECT MIN(valid_since) FROM tcc_receita.silver_intermediate.dados_simples) AS baseline_simples,
        (SELECT MIN(valid_since) FROM tcc_receita.silver_intermediate.socios) AS baseline_socios
),
-- Domínio de motivos da situação cadastral (código -> descrição), para informacao_adicional.
dom_motivos AS (
    SELECT
        TRIM(codigo) AS cod_motivo,
        MAX(UPPER(TRIM(descricao))) AS desc_motivo
    FROM tcc_receita.bronze.motivos_situacao
    GROUP BY TRIM(codigo)
),

-- =====================================================================================================
-- ESTABELECIMENTOS: atributos da versão anterior, só para as versões que interessam à janela.
-- =====================================================================================================
est_versoes AS (
    SELECT
        e.pk,
        e.cnpj_basico,
        e.identificador_matriz_filial,
        e.situacao_cadastral_desc,
        e.motivo_situacao_cadastral,
        e.data_situacao_cadastral,
        e.cnae_fiscal_principal,
        e.cnae_fiscal_secundaria,
        e.valid_since,
        LAG(e.valid_since) OVER w IS NULL     AS eh_primeira_versao,
        LAG(e.situacao_cadastral_desc)   OVER w AS ant_situacao_desc,
        LAG(e.motivo_situacao_cadastral) OVER w AS ant_motivo,
        LAG(e.cnae_fiscal_principal)     OVER w AS ant_cnae_principal,
        LAG(e.cnae_fiscal_secundaria)    OVER w AS ant_cnae_secundaria
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    CROSS JOIN params p
    -- Filtro ANTES do LAG (ver cabeçalho): (a) começa na janela depois do baseline OU (b) termina na janela.
    WHERE (e.valid_since BETWEEN p.mes_ini AND p.mes_fim AND e.valid_since > p.baseline_est)
       OR  e.valid_until BETWEEN p.mes_ini AND p.mes_fim
    WINDOW w AS (PARTITION BY e.pk ORDER BY e.valid_since)
),
-- Só as versões novas (a); as que só terminam na janela (b) serviram apenas de "versão anterior" no LAG.
-- eh_matriz (nos eventos abaixo): identificador '1' = matriz; qualquer outro valor, inclusive NULL, conta
-- como não-matriz (FALSE), porque só matriz confirmada entra nos indicadores de empresa.
est_janela AS (
    SELECT v.*, p.baseline_est
    FROM est_versoes v
    CROSS JOIN params p
    WHERE v.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND v.valid_since > p.baseline_est
),
-- 1a. Mudança de situação cadastral OU de motivo entre duas versões consecutivas.
--     valor_antigo/valor_novo = só a descrição da situação; o motivo vai em codigo_motivo.
--     Situação sem descrição (código fora do domínio) vira 'NAO INFORMADA' para não gerar valor_novo nulo.
ev_estado_cadastral AS (
    SELECT /*+ BROADCAST(m) */
        e.pk,
        'ESTABELECIMENTO' AS entidade_tipo,
        'MUDOU ESTADO CADASTRAL' AS evento,
        COALESCE(e.ant_situacao_desc, 'NAO INFORMADA') AS valor_antigo,
        COALESCE(e.situacao_cadastral_desc, 'NAO INFORMADA') AS valor_novo,
        m.desc_motivo AS informacao_adicional,
        e.valid_since AS mes_referencia,
        CAST(try_to_timestamp(e.data_situacao_cadastral, 'yyyyMMdd') AS DATE) AS data_evento,
        e.motivo_situacao_cadastral AS codigo_motivo,
        COALESCE(e.identificador_matriz_filial = '1', FALSE) AS eh_matriz,
        e.cnpj_basico
    FROM est_janela e
    LEFT JOIN dom_motivos m
        ON e.motivo_situacao_cadastral = m.cod_motivo
    WHERE NOT e.eh_primeira_versao
      AND (
             e.ant_situacao_desc IS DISTINCT FROM e.situacao_cadastral_desc
          OR e.ant_motivo IS DISTINCT FROM e.motivo_situacao_cadastral
      )
),
-- 1b. Estabelecimento que aparece pela primeira vez depois do baseline já fora de ATIVA (C-16):
--     mesmo evento, com valor_antigo NULL. Situação desconhecida (NULL) não gera evento (NULL ≠ "não ativa").
ev_estado_cadastral_primeira AS (
    SELECT /*+ BROADCAST(m) */
        e.pk,
        'ESTABELECIMENTO' AS entidade_tipo,
        'MUDOU ESTADO CADASTRAL' AS evento,
        CAST(NULL AS STRING) AS valor_antigo,
        e.situacao_cadastral_desc AS valor_novo,
        m.desc_motivo AS informacao_adicional,
        e.valid_since AS mes_referencia,
        CAST(try_to_timestamp(e.data_situacao_cadastral, 'yyyyMMdd') AS DATE) AS data_evento,
        e.motivo_situacao_cadastral AS codigo_motivo,
        COALESCE(e.identificador_matriz_filial = '1', FALSE) AS eh_matriz,
        e.cnpj_basico
    FROM est_janela e
    LEFT JOIN dom_motivos m
        ON e.motivo_situacao_cadastral = m.cod_motivo
    WHERE e.eh_primeira_versao
      AND e.valid_since > e.baseline_est
      AND e.situacao_cadastral_desc IS NOT NULL
      AND e.situacao_cadastral_desc <> 'ATIVA'
),
-- 2. Mudança de CNAE principal.
ev_cnae_principal AS (
    SELECT
        e.pk,
        'ESTABELECIMENTO' AS entidade_tipo,
        'MUDOU CNAE PRINCIPAL' AS evento,
        e.ant_cnae_principal AS valor_antigo,
        e.cnae_fiscal_principal AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        e.valid_since AS mes_referencia,
        CAST(NULL AS DATE) AS data_evento,
        CAST(NULL AS STRING) AS codigo_motivo,
        COALESCE(e.identificador_matriz_filial = '1', FALSE) AS eh_matriz,
        e.cnpj_basico
    FROM est_janela e
    WHERE NOT e.eh_primeira_versao
      AND e.ant_cnae_principal IS DISTINCT FROM e.cnae_fiscal_principal
),
-- 3. Mudança na lista de CNAEs secundários (valor_novo pode ser NULL: a lista foi esvaziada, B18).
ev_cnae_secundario AS (
    SELECT
        e.pk,
        'ESTABELECIMENTO' AS entidade_tipo,
        'MUDOU LISTA CNAE SECUNDARIO' AS evento,
        e.ant_cnae_secundaria AS valor_antigo,
        e.cnae_fiscal_secundaria AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        e.valid_since AS mes_referencia,
        CAST(NULL AS DATE) AS data_evento,
        CAST(NULL AS STRING) AS codigo_motivo,
        COALESCE(e.identificador_matriz_filial = '1', FALSE) AS eh_matriz,
        e.cnpj_basico
    FROM est_janela e
    WHERE NOT e.eh_primeira_versao
      AND e.ant_cnae_secundaria IS DISTINCT FROM e.cnae_fiscal_secundaria
),

-- =====================================================================================================
-- EMPRESA NASCEU: primeira aparição da MATRIZ CANÔNICA do CNPJ raiz (mesma regra da dim_empresa).
-- =====================================================================================================
-- Desempenho (spec §3.1, Ideia 3): só pode nascer na janela um CNPJ que tenha alguma versão de MATRIZ
-- começando na janela (depois do baseline) — e essas versões já estão em est_janela. A regra da matriz
-- canônica roda só para esses candidatos, em vez de comparar todas as matrizes do histórico entre si.
candidatos_nascimento AS (
    SELECT DISTINCT cnpj_basico
    FROM est_janela
    WHERE identificador_matriz_filial = '1'
      AND cnpj_basico IS NOT NULL
),
-- Histórico COMPLETO de matriz dos candidatos (sem filtro de datas), só com as colunas estreitas que a regra usa.
hist_matriz AS (
    SELECT e.pk, e.cnpj_basico, e.cnpj_ordem, e.data_inicio_atividade, e.valid_since, e.valid_until
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    LEFT SEMI JOIN candidatos_nascimento c
        ON c.cnpj_basico = e.cnpj_basico
    WHERE e.identificador_matriz_filial = '1'
),
-- Matriz canônica (mesma regra da CTE est_matriz da dim_empresa): em cada período, o estabelecimento marcado
-- como matriz; havendo mais de um ao mesmo tempo, o de menor ordem (B19).
matriz_canonica AS (
    SELECT
        h.cnpj_basico,
        h.pk,
        h.data_inicio_atividade,
        h.valid_since
    FROM hist_matriz h
    -- A matriz pode mudar de estabelecimento ao longo do tempo (a Receita transfere a condição de matriz),
    -- e às vezes há mais de um marcado como matriz no mesmo período (B19). Regra da matriz canônica:
    -- em cada período vale o estabelecimento marcado como matriz; havendo mais de um ao mesmo tempo,
    -- fica o de menor ordem (0001 primeiro). A versão de ordem maior que se sobrepõe é descartada inteira.
    -- O lado `melhor` continua sem filtro de datas (igual à regra sobre a tabela inteira).
    LEFT ANTI JOIN hist_matriz melhor
        ON melhor.cnpj_basico = h.cnpj_basico
       AND melhor.cnpj_ordem < h.cnpj_ordem
       AND melhor.valid_since < COALESCE(h.valid_until, '9999-12')
       AND COALESCE(melhor.valid_until, '9999-12') > h.valid_since
    WHERE h.valid_since IS NOT NULL
      AND h.valid_since < COALESCE(h.valid_until, '9999-12')
),
-- Primeira versão da matriz canônica de cada CNPJ raiz (desempate por pk só por determinismo).
primeira_versao_matriz AS (
    SELECT cnpj_basico, data_inicio_atividade, valid_since
    FROM matriz_canonica
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cnpj_basico ORDER BY valid_since, pk) = 1
),
-- Uma pk de empresa por CNPJ raiz (a pk vem da tabela empresas; nunca é montada a partir do CNPJ).
-- Só para os candidatos, não para a tabela empresas inteira.
pk_empresa AS (
    SELECT emp.cnpj_basico, MIN(emp.pk) AS pk
    FROM tcc_receita.silver_intermediate.empresas emp
    LEFT SEMI JOIN primeira_versao_matriz mat
        ON mat.cnpj_basico = emp.cnpj_basico
    WHERE emp.cnpj_basico IS NOT NULL
    GROUP BY emp.cnpj_basico
),
-- 4. Nascimento: primeira aparição da matriz depois do baseline, detectada na janela.
--    Empresa sem cadastro em `empresas` fica de fora (INNER JOIN): não há pk de empresa para ela.
ev_empresa_nasceu AS (
    SELECT
        emp.pk,
        'EMPRESA' AS entidade_tipo,
        'EMPRESA NASCEU' AS evento,
        CAST(NULL AS STRING) AS valor_antigo,
        'ATIVIDADE INICIADA' AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        mat.valid_since AS mes_referencia,
        CAST(try_to_timestamp(mat.data_inicio_atividade, 'yyyyMMdd') AS DATE) AS data_evento,
        CAST(NULL AS STRING) AS codigo_motivo,
        TRUE AS eh_matriz,
        mat.cnpj_basico
    FROM primeira_versao_matriz mat
    INNER JOIN pk_empresa emp
        ON emp.cnpj_basico = mat.cnpj_basico
    CROSS JOIN params p
    WHERE mat.valid_since > p.baseline_est
      AND mat.valid_since BETWEEN p.mes_ini AND p.mes_fim
),

-- =====================================================================================================
-- EMPRESAS: mudança de natureza jurídica.
-- =====================================================================================================
emp_versoes AS (
    SELECT
        e.pk,
        e.cnpj_basico,
        e.natureza_juridica,
        e.valid_since,
        LAG(e.valid_since)       OVER w IS NULL AS eh_primeira_versao,
        LAG(e.natureza_juridica) OVER w AS ant_natureza_juridica
    FROM tcc_receita.silver_intermediate.empresas e
    CROSS JOIN params p
    -- Mesmo filtro antes do LAG dos estabelecimentos, com o baseline da própria tabela.
    WHERE (e.valid_since BETWEEN p.mes_ini AND p.mes_fim AND e.valid_since > p.baseline_emp)
       OR  e.valid_until BETWEEN p.mes_ini AND p.mes_fim
    WINDOW w AS (PARTITION BY e.pk ORDER BY e.valid_since)
),
-- 5. Mudança de natureza jurídica entre duas versões consecutivas, detectada na janela.
ev_natureza_juridica AS (
    SELECT
        e.pk,
        'EMPRESA' AS entidade_tipo,
        'MUDOU NATUREZA JURIDICA' AS evento,
        e.ant_natureza_juridica AS valor_antigo,
        e.natureza_juridica AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        e.valid_since AS mes_referencia,
        CAST(NULL AS DATE) AS data_evento,
        CAST(NULL AS STRING) AS codigo_motivo,
        CAST(NULL AS BOOLEAN) AS eh_matriz,
        e.cnpj_basico
    FROM emp_versoes e
    CROSS JOIN params p
    WHERE e.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND e.valid_since > p.baseline_emp
      AND NOT e.eh_primeira_versao
      AND e.ant_natureza_juridica IS DISTINCT FROM e.natureza_juridica
),

-- =====================================================================================================
-- DADOS_SIMPLES: entradas e saídas do MEI e do Simples. NULL ≠ FALSE: flag desconhecida não gera evento.
-- =====================================================================================================
sim_versoes AS (
    SELECT
        s.pk,
        s.cnpj_basico,
        s.opcao_pelo_mei_bool,
        s.opcao_pelo_simples_bool,
        s.data_opcao_mei,
        s.data_exclusao_mei,
        s.data_opcao_simples,
        s.data_exclusao_simples,
        s.valid_since,
        LAG(s.valid_since)             OVER w IS NULL AS eh_primeira_versao,
        LAG(s.opcao_pelo_mei_bool)     OVER w AS ant_mei,
        LAG(s.opcao_pelo_simples_bool) OVER w AS ant_simples
    FROM tcc_receita.silver_intermediate.dados_simples s
    CROSS JOIN params p
    -- Mesmo filtro antes do LAG dos estabelecimentos, com o baseline da própria tabela.
    WHERE (s.valid_since BETWEEN p.mes_ini AND p.mes_fim AND s.valid_since > p.baseline_simples)
       OR  s.valid_until BETWEEN p.mes_ini AND p.mes_fim
    WINDOW w AS (PARTITION BY s.pk ORDER BY s.valid_since)
),
-- Só as versões novas (a); as demais serviram apenas de "versão anterior" no LAG.
sim_janela AS (
    SELECT v.*, p.baseline_simples
    FROM sim_versoes v
    CROSS JOIN params p
    WHERE v.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND v.valid_since > p.baseline_simples
),
-- 6. Transições de flag (FALSE→TRUE = entrou; TRUE→FALSE = saiu) e primeira aparição pós-baseline
--    já optante (C-16: NULL → 'true'). data_evento = data de opção (entrada) ou de exclusão (saída).
ev_tributario AS (
    -- Entrou no MEI (transição)
    SELECT pk, cnpj_basico, 'ENTROU NO MEI' AS evento,
           CAST(ant_mei AS STRING) AS valor_antigo, CAST(opcao_pelo_mei_bool AS STRING) AS valor_novo,
           valid_since, data_opcao_mei AS data_texto
    FROM sim_janela
    WHERE ant_mei = FALSE AND opcao_pelo_mei_bool = TRUE
    UNION ALL
    -- Entrou no MEI (primeira aparição pós-baseline já como MEI)
    SELECT pk, cnpj_basico, 'ENTROU NO MEI',
           CAST(NULL AS STRING), CAST(opcao_pelo_mei_bool AS STRING),
           valid_since, data_opcao_mei
    FROM sim_janela
    WHERE eh_primeira_versao AND valid_since > baseline_simples AND opcao_pelo_mei_bool = TRUE
    UNION ALL
    -- Saiu do MEI
    SELECT pk, cnpj_basico, 'SAIU DO MEI',
           CAST(ant_mei AS STRING), CAST(opcao_pelo_mei_bool AS STRING),
           valid_since, data_exclusao_mei
    FROM sim_janela
    WHERE ant_mei = TRUE AND opcao_pelo_mei_bool = FALSE
    UNION ALL
    -- Entrou no Simples (transição)
    SELECT pk, cnpj_basico, 'ENTROU NO SIMPLES',
           CAST(ant_simples AS STRING), CAST(opcao_pelo_simples_bool AS STRING),
           valid_since, data_opcao_simples
    FROM sim_janela
    WHERE ant_simples = FALSE AND opcao_pelo_simples_bool = TRUE
    UNION ALL
    -- Entrou no Simples (primeira aparição pós-baseline já optante)
    SELECT pk, cnpj_basico, 'ENTROU NO SIMPLES',
           CAST(NULL AS STRING), CAST(opcao_pelo_simples_bool AS STRING),
           valid_since, data_opcao_simples
    FROM sim_janela
    WHERE eh_primeira_versao AND valid_since > baseline_simples AND opcao_pelo_simples_bool = TRUE
    UNION ALL
    -- Saiu do Simples
    SELECT pk, cnpj_basico, 'SAIU DO SIMPLES',
           CAST(ant_simples AS STRING), CAST(opcao_pelo_simples_bool AS STRING),
           valid_since, data_exclusao_simples
    FROM sim_janela
    WHERE ant_simples = TRUE AND opcao_pelo_simples_bool = FALSE
),
ev_tributario_final AS (
    SELECT
        pk,
        'TRIBUTARIO' AS entidade_tipo,
        evento,
        valor_antigo,
        valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        valid_since AS mes_referencia,
        CAST(try_to_timestamp(data_texto, 'yyyyMMdd') AS DATE) AS data_evento,
        CAST(NULL AS STRING) AS codigo_motivo,
        CAST(NULL AS BOOLEAN) AS eh_matriz,
        cnpj_basico
    FROM ev_tributario
),

-- =====================================================================================================
-- SOCIOS: entrada, saída, retorno e mudança de qualificação no QSA.
-- =====================================================================================================
soc_versoes AS (
    SELECT
        s.pk,
        s.cnpj_basico,
        s.vinculo_ativo,
        s.qualificacao_socio,
        s.data_entrada_sociedade,
        s.valid_since,
        LAG(s.valid_since)        OVER w IS NULL AS eh_primeira_versao,
        LAG(s.vinculo_ativo)      OVER w AS ant_vinculo_ativo,
        LAG(s.qualificacao_socio) OVER w AS ant_qualificacao
    FROM tcc_receita.silver_intermediate.socios s
    CROSS JOIN params p
    -- Mesmo filtro antes do LAG dos estabelecimentos, com o baseline da própria tabela.
    WHERE (s.valid_since BETWEEN p.mes_ini AND p.mes_fim AND s.valid_since > p.baseline_socios)
       OR  s.valid_until BETWEEN p.mes_ini AND p.mes_fim
    WINDOW w AS (PARTITION BY s.pk ORDER BY s.valid_since)
),
-- Só as versões novas (a); as demais serviram apenas de "versão anterior" no LAG.
soc_janela AS (
    SELECT v.*, p.baseline_socios
    FROM soc_versoes v
    CROSS JOIN params p
    WHERE v.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND v.valid_since > p.baseline_socios
),
-- 7. Eventos do QSA. Nenhum leva documento ou CNPJ em informacao_adicional.
ev_socios AS (
    -- Entrou: primeira versão da relação, com vínculo ativo, depois do baseline (mês de detecção).
    SELECT pk, cnpj_basico, 'SOCIO ENTROU NO QSA' AS evento,
           CAST(NULL AS STRING) AS valor_antigo, 'ATIVO' AS valor_novo,
           CAST(NULL AS STRING) AS informacao_adicional,
           valid_since, data_entrada_sociedade AS data_texto
    FROM soc_janela
    WHERE eh_primeira_versao AND valid_since > baseline_socios AND vinculo_ativo = TRUE
    UNION ALL
    -- Saiu: primeiro snapshot completo em que a relação não aparece. Info = qualificação (como antes).
    SELECT pk, cnpj_basico, 'SOCIO SAIU DO QSA',
           'ATIVO', 'INATIVO',
           qualificacao_socio,
           valid_since, CAST(NULL AS STRING)
    FROM soc_janela
    WHERE ant_vinculo_ativo = TRUE AND vinculo_ativo = FALSE
    UNION ALL
    -- Retornou: relação marcada como inativa volta a aparecer.
    SELECT pk, cnpj_basico, 'SOCIO RETORNOU AO QSA',
           'INATIVO', 'ATIVO',
           CAST(NULL AS STRING),
           valid_since, data_entrada_sociedade
    FROM soc_janela
    WHERE ant_vinculo_ativo = FALSE AND vinculo_ativo = TRUE
    UNION ALL
    -- Mudou qualificação: só entre duas versões com vínculo ativo.
    SELECT pk, cnpj_basico, 'MUDOU QUALIFICACAO SOCIO',
           ant_qualificacao, qualificacao_socio,
           CAST(NULL AS STRING),
           valid_since, CAST(NULL AS STRING)
    FROM soc_janela
    WHERE ant_vinculo_ativo = TRUE AND vinculo_ativo = TRUE
      AND ant_qualificacao IS DISTINCT FROM qualificacao_socio
),
ev_socios_final AS (
    SELECT
        pk,
        'SOCIO' AS entidade_tipo,
        evento,
        valor_antigo,
        valor_novo,
        informacao_adicional,
        valid_since AS mes_referencia,
        CAST(try_to_timestamp(data_texto, 'yyyyMMdd') AS DATE) AS data_evento,
        CAST(NULL AS STRING) AS codigo_motivo,
        CAST(NULL AS BOOLEAN) AS eh_matriz,
        cnpj_basico
    FROM ev_socios
),

-- Todas as CTEs acima têm as mesmas 11 colunas, na mesma ordem (cnpj_basico por último).
todos_eventos AS (
    SELECT * FROM ev_empresa_nasceu
    UNION ALL SELECT * FROM ev_estado_cadastral
    UNION ALL SELECT * FROM ev_estado_cadastral_primeira
    UNION ALL SELECT * FROM ev_cnae_principal
    UNION ALL SELECT * FROM ev_cnae_secundario
    UNION ALL SELECT * FROM ev_natureza_juridica
    UNION ALL SELECT * FROM ev_tributario_final
    UNION ALL SELECT * FROM ev_socios_final
)

-- Inserção posicional: a ordem do SELECT segue a ordem das colunas do DDL.
INSERT INTO tcc_receita.silver_intermediate.log_eventos_negocio
REPLACE WHERE mes_referencia >= '{{mes_referencia_inicial}}'
              AND mes_referencia <= '{{mes_referencia_final}}'
SELECT
    pk,
    entidade_tipo,
    evento,
    valor_antigo,
    valor_novo,
    informacao_adicional,
    mes_referencia,
    current_timestamp() AS gerado_em,
    data_evento,
    codigo_motivo,
    eh_matriz,
    cnpj_basico
FROM todos_eventos;
