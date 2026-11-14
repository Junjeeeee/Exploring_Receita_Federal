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
-- Como as transições são detectadas: UMA passada por tabela SCD2 com LAG(...) OVER (PARTITION BY pk
-- ORDER BY valid_since), que traz os atributos da versão anterior, e ROW_NUMBER, que diz se a versão é a
-- primeira da pk. SUPOSIÇÃO: as versões de uma pk são contíguas (valid_until da anterior = valid_since da
-- seguinte). Isso vale por construção nas SCD2 deste projeto (valid_until = LEAD(valid_since)), então a
-- "versão anterior pelo LAG" equivale ao antigo self-join antigo.valid_until = novo.valid_since.
-- O filtro da janela é aplicado DEPOIS do LAG (em CTEs separadas), para que a versão anterior, que pode
-- estar fora da janela, seja enxergada.

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
    eh_matriz BOOLEAN COMMENT 'Entidade ESTABELECIMENTO: identificador_matriz_filial = 1 na versão nova. EMPRESA NASCEU: TRUE. Demais (EMPRESA, TRIBUTARIO, SOCIO): NULL, não se aplica.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Silver Intermediate: log de transições cadastrais, societárias e tributárias. mes_referencia = mês de detecção; data_evento = data declarada pela Receita. Ordem das colunas: pk, entidade_tipo, evento, valor_antigo, valor_novo, informacao_adicional, mes_referencia, gerado_em, data_evento, codigo_motivo, eh_matriz.';

WITH params AS (
    SELECT
        CAST('{{mes_referencia_inicial}}' AS STRING) AS mes_ini,
        CAST('{{mes_referencia_final}}' AS STRING) AS mes_fim,
        -- Baseline (carga inicial) de cada SCD2 = o menor valid_since da própria tabela.
        (SELECT MIN(valid_since) FROM tcc_receita.silver_intermediate.estabelecimentos) AS baseline_est,
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
-- ESTABELECIMENTOS: uma passada com os atributos da versão anterior.
-- =====================================================================================================
est_versoes AS (
    SELECT
        pk,
        identificador_matriz_filial,
        situacao_cadastral_desc,
        motivo_situacao_cadastral,
        data_situacao_cadastral,
        cnae_fiscal_principal,
        cnae_fiscal_secundaria,
        valid_since,
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY valid_since) AS ordem_versao,
        LAG(situacao_cadastral_desc)   OVER (PARTITION BY pk ORDER BY valid_since) AS ant_situacao_desc,
        LAG(motivo_situacao_cadastral) OVER (PARTITION BY pk ORDER BY valid_since) AS ant_motivo,
        LAG(cnae_fiscal_principal)     OVER (PARTITION BY pk ORDER BY valid_since) AS ant_cnae_principal,
        LAG(cnae_fiscal_secundaria)    OVER (PARTITION BY pk ORDER BY valid_since) AS ant_cnae_secundaria
    FROM tcc_receita.silver_intermediate.estabelecimentos
),
-- Só as versões que começam na janela (filtro depois do LAG).
-- eh_matriz (nos eventos abaixo): identificador '1' = matriz; qualquer outro valor, inclusive NULL, conta
-- como não-matriz (FALSE), porque só matriz confirmada entra nos indicadores de empresa.
est_janela AS (
    SELECT v.*, p.baseline_est
    FROM est_versoes v
    CROSS JOIN params p
    WHERE v.valid_since BETWEEN p.mes_ini AND p.mes_fim
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
        COALESCE(e.identificador_matriz_filial = '1', FALSE) AS eh_matriz
    FROM est_janela e
    LEFT JOIN dom_motivos m
        ON e.motivo_situacao_cadastral = m.cod_motivo
    WHERE e.ordem_versao > 1
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
        COALESCE(e.identificador_matriz_filial = '1', FALSE) AS eh_matriz
    FROM est_janela e
    LEFT JOIN dom_motivos m
        ON e.motivo_situacao_cadastral = m.cod_motivo
    WHERE e.ordem_versao = 1
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
        COALESCE(e.identificador_matriz_filial = '1', FALSE) AS eh_matriz
    FROM est_janela e
    WHERE e.ordem_versao > 1
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
        COALESCE(e.identificador_matriz_filial = '1', FALSE) AS eh_matriz
    FROM est_janela e
    WHERE e.ordem_versao > 1
      AND e.ant_cnae_secundaria IS DISTINCT FROM e.cnae_fiscal_secundaria
),

-- =====================================================================================================
-- EMPRESA NASCEU: primeira aparição da MATRIZ CANÔNICA do CNPJ raiz (mesma regra da dim_empresa).
-- =====================================================================================================
-- Matriz canônica (mesma regra da CTE est_matriz da dim_empresa): em cada período, o estabelecimento marcado
-- como matriz; havendo mais de um ao mesmo tempo, o de menor ordem (B19).
matriz_canonica AS (
    SELECT
        cnpj_basico,
        pk,
        data_inicio_atividade,
        valid_since
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
-- Primeira versão da matriz canônica de cada CNPJ raiz (desempate por pk só por determinismo).
primeira_versao_matriz AS (
    SELECT cnpj_basico, data_inicio_atividade, valid_since
    FROM matriz_canonica
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cnpj_basico ORDER BY valid_since, pk) = 1
),
-- Uma pk de empresa por CNPJ raiz (a pk vem da tabela empresas; nunca é montada a partir do CNPJ).
pk_empresa AS (
    SELECT cnpj_basico, MIN(pk) AS pk
    FROM tcc_receita.silver_intermediate.empresas
    WHERE cnpj_basico IS NOT NULL
    GROUP BY cnpj_basico
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
        TRUE AS eh_matriz
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
        pk,
        natureza_juridica,
        valid_since,
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY valid_since) AS ordem_versao,
        LAG(natureza_juridica) OVER (PARTITION BY pk ORDER BY valid_since) AS ant_natureza_juridica
    FROM tcc_receita.silver_intermediate.empresas
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
        CAST(NULL AS BOOLEAN) AS eh_matriz
    FROM emp_versoes e
    CROSS JOIN params p
    WHERE e.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND e.ordem_versao > 1
      AND e.ant_natureza_juridica IS DISTINCT FROM e.natureza_juridica
),

-- =====================================================================================================
-- DADOS_SIMPLES: entradas e saídas do MEI e do Simples. NULL ≠ FALSE: flag desconhecida não gera evento.
-- =====================================================================================================
sim_versoes AS (
    SELECT
        pk,
        opcao_pelo_mei_bool,
        opcao_pelo_simples_bool,
        data_opcao_mei,
        data_exclusao_mei,
        data_opcao_simples,
        data_exclusao_simples,
        valid_since,
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY valid_since) AS ordem_versao,
        LAG(opcao_pelo_mei_bool)     OVER (PARTITION BY pk ORDER BY valid_since) AS ant_mei,
        LAG(opcao_pelo_simples_bool) OVER (PARTITION BY pk ORDER BY valid_since) AS ant_simples
    FROM tcc_receita.silver_intermediate.dados_simples
),
sim_janela AS (
    SELECT v.*, p.baseline_simples
    FROM sim_versoes v
    CROSS JOIN params p
    WHERE v.valid_since BETWEEN p.mes_ini AND p.mes_fim
),
-- 6. Transições de flag (FALSE→TRUE = entrou; TRUE→FALSE = saiu) e primeira aparição pós-baseline
--    já optante (C-16: NULL → 'true'). data_evento = data de opção (entrada) ou de exclusão (saída).
ev_tributario AS (
    -- Entrou no MEI (transição)
    SELECT pk, 'ENTROU NO MEI' AS evento,
           CAST(ant_mei AS STRING) AS valor_antigo, CAST(opcao_pelo_mei_bool AS STRING) AS valor_novo,
           valid_since, data_opcao_mei AS data_texto
    FROM sim_janela
    WHERE ant_mei = FALSE AND opcao_pelo_mei_bool = TRUE
    UNION ALL
    -- Entrou no MEI (primeira aparição pós-baseline já como MEI)
    SELECT pk, 'ENTROU NO MEI',
           CAST(NULL AS STRING), CAST(opcao_pelo_mei_bool AS STRING),
           valid_since, data_opcao_mei
    FROM sim_janela
    WHERE ordem_versao = 1 AND valid_since > baseline_simples AND opcao_pelo_mei_bool = TRUE
    UNION ALL
    -- Saiu do MEI
    SELECT pk, 'SAIU DO MEI',
           CAST(ant_mei AS STRING), CAST(opcao_pelo_mei_bool AS STRING),
           valid_since, data_exclusao_mei
    FROM sim_janela
    WHERE ant_mei = TRUE AND opcao_pelo_mei_bool = FALSE
    UNION ALL
    -- Entrou no Simples (transição)
    SELECT pk, 'ENTROU NO SIMPLES',
           CAST(ant_simples AS STRING), CAST(opcao_pelo_simples_bool AS STRING),
           valid_since, data_opcao_simples
    FROM sim_janela
    WHERE ant_simples = FALSE AND opcao_pelo_simples_bool = TRUE
    UNION ALL
    -- Entrou no Simples (primeira aparição pós-baseline já optante)
    SELECT pk, 'ENTROU NO SIMPLES',
           CAST(NULL AS STRING), CAST(opcao_pelo_simples_bool AS STRING),
           valid_since, data_opcao_simples
    FROM sim_janela
    WHERE ordem_versao = 1 AND valid_since > baseline_simples AND opcao_pelo_simples_bool = TRUE
    UNION ALL
    -- Saiu do Simples
    SELECT pk, 'SAIU DO SIMPLES',
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
        CAST(NULL AS BOOLEAN) AS eh_matriz
    FROM ev_tributario
),

-- =====================================================================================================
-- SOCIOS: entrada, saída, retorno e mudança de qualificação no QSA.
-- =====================================================================================================
soc_versoes AS (
    SELECT
        pk,
        vinculo_ativo,
        qualificacao_socio,
        data_entrada_sociedade,
        valid_since,
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY valid_since) AS ordem_versao,
        LAG(vinculo_ativo)      OVER (PARTITION BY pk ORDER BY valid_since) AS ant_vinculo_ativo,
        LAG(qualificacao_socio) OVER (PARTITION BY pk ORDER BY valid_since) AS ant_qualificacao
    FROM tcc_receita.silver_intermediate.socios
),
soc_janela AS (
    SELECT v.*, p.baseline_socios
    FROM soc_versoes v
    CROSS JOIN params p
    WHERE v.valid_since BETWEEN p.mes_ini AND p.mes_fim
),
-- 7. Eventos do QSA. Nenhum leva documento ou CNPJ em informacao_adicional.
ev_socios AS (
    -- Entrou: primeira versão da relação, com vínculo ativo, depois do baseline (mês de detecção).
    SELECT pk, 'SOCIO ENTROU NO QSA' AS evento,
           CAST(NULL AS STRING) AS valor_antigo, 'ATIVO' AS valor_novo,
           CAST(NULL AS STRING) AS informacao_adicional,
           valid_since, data_entrada_sociedade AS data_texto
    FROM soc_janela
    WHERE ordem_versao = 1 AND valid_since > baseline_socios AND vinculo_ativo = TRUE
    UNION ALL
    -- Saiu: primeiro snapshot completo em que a relação não aparece. Info = qualificação (como antes).
    SELECT pk, 'SOCIO SAIU DO QSA',
           'ATIVO', 'INATIVO',
           qualificacao_socio,
           valid_since, CAST(NULL AS STRING)
    FROM soc_janela
    WHERE ant_vinculo_ativo = TRUE AND vinculo_ativo = FALSE
    UNION ALL
    -- Retornou: relação marcada como inativa volta a aparecer.
    SELECT pk, 'SOCIO RETORNOU AO QSA',
           'INATIVO', 'ATIVO',
           CAST(NULL AS STRING),
           valid_since, data_entrada_sociedade
    FROM soc_janela
    WHERE ant_vinculo_ativo = FALSE AND vinculo_ativo = TRUE
    UNION ALL
    -- Mudou qualificação: só entre duas versões com vínculo ativo.
    SELECT pk, 'MUDOU QUALIFICACAO SOCIO',
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
        CAST(NULL AS BOOLEAN) AS eh_matriz
    FROM ev_socios
),

-- Todas as CTEs acima têm as mesmas 10 colunas, na mesma ordem.
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
    eh_matriz
FROM todos_eventos;
