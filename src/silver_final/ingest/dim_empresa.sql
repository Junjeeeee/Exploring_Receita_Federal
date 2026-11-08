-- Silver Final | dim_empresa
-- Granularidade: uma linha por CNPJ raiz e fatia temporal resultante da
-- interseção dos SCD2 de matriz, empresa e Simples/MEI.
-- Dependência: dim_grupo_cnae deve ser atualizada antes desta dimensão.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_final.dim_empresa (
    sk_empresa BIGINT COMMENT 'Chave substituta hash 64-bit de nk_empresa.',
    sk_grupo_cnae BIGINT COMMENT 'FK para dim_grupo_cnae; -1 quando não há CNAE válido.',
    nk_empresa STRING COMMENT 'Chave natural da versão: cnpj_raiz|inicio_vigencia.',
    cnpj_raiz STRING COMMENT 'Radical de oito dígitos do CNPJ.',
    inicio_vigencia STRING COMMENT 'Início da vigência (YYYY-MM), inclusivo.',
    fim_vigencia STRING COMMENT 'Fim da vigência (YYYY-MM), exclusivo; NULL para versão atual.',
    data_inicio_vigencia DATE COMMENT 'Primeiro dia do mês inicial da vigência.',
    data_fim_vigencia DATE COMMENT 'Primeiro dia do mês final exclusivo; NULL para versão atual.',
    cnpj_matriz STRING COMMENT 'CNPJ completo de 14 dígitos da matriz.',
    registro_ativo BOOLEAN COMMENT 'TRUE quando fim_vigencia é NULL.',
    possui_cadastro_empresa BOOLEAN COMMENT 'Indica correspondência temporal na Intermediate de empresas.',
    razao_social STRING COMMENT 'Razão social da empresa, com acentuação preservada.',
    natureza_juridica STRING COMMENT 'Descrição da natureza jurídica, com fallback para o código.',
    capital_social DOUBLE COMMENT 'Capital social em BRL, conforme a precisão da Silver Intermediate.',
    ente_federativo_responsavel STRING COMMENT 'Ente federativo responsável, quando aplicável.',
    qualificacao_responsavel STRING COMMENT 'Descrição da qualificação do responsável, com fallback para o código.',
    opcao_pelo_mei BOOLEAN COMMENT 'Estado de opção pelo MEI; NULL quando não há informação correspondente.',
    opcao_pelo_simples BOOLEAN COMMENT 'Estado de opção pelo Simples; NULL quando não há informação correspondente.',
    data_inicio_atividade DATE COMMENT 'Data de início da atividade da matriz, validada no calendário.',
    nome_fantasia_matriz STRING COMMENT 'Nome fantasia do estabelecimento matriz.',
    situacao_cadastral_matriz STRING COMMENT 'Descrição da situação cadastral da matriz.',
    motivo_situacao_cadastral STRING COMMENT 'Descrição do motivo cadastral, com fallback para o código.',
    cep STRING COMMENT 'CEP de oito dígitos da matriz.',
    uf STRING COMMENT 'UF consolidada, priorizando o diretório oficial de CEPs.',
    municipio STRING COMMENT 'Município consolidado, priorizando CEP e domínio RFB.',
    codigo_ibge_municipio STRING COMMENT 'Código IBGE de sete dígitos quando disponível.',
    bairro_campo_livre STRING COMMENT 'Bairro informado no cadastro da Receita Federal.',
    localidade_cep STRING COMMENT 'Localidade do diretório de CEPs.',
    localidade_analitica STRING COMMENT 'Localidade analítica: localidade_cep com fallback para bairro cadastral.',
    latitude_cep DOUBLE COMMENT 'Latitude do centroide associado ao CEP.',
    longitude_cep DOUBLE COMMENT 'Longitude do centroide associado ao CEP.',
    logradouro_completo STRING COMMENT 'Tipo de logradouro, logradouro e número concatenados.',
    codigo_porte_empresa STRING COMMENT 'Código original do porte (01, 03, 05).',
    porte_cadastral STRING COMMENT 'Porte cadastral RFB: ME, EPP, DEMAIS ou NAO INFORMADO.',
    enquadramento_porte_analitico STRING COMMENT 'Enquadramento analítico que prioriza MEI e depois o porte cadastral.',
    telefone_principal STRING COMMENT 'Telefone primário formatado.',
    telefone_secundario STRING COMMENT 'Telefone secundário formatado.',
    correio_eletronico STRING COMMENT 'E-mail institucional.',
    situacao_especial STRING COMMENT 'Situação especial declarada na Receita Federal.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na Silver Final.'
)
USING DELTA
COMMENT 'Silver Final: dimensão temporal de empresas por CNPJ raiz e intervalo semiaberto [inicio_vigencia, fim_vigencia).';

INSERT OVERWRITE tcc_receita.silver_final.dim_empresa (
    sk_empresa, sk_grupo_cnae, nk_empresa, cnpj_raiz,
    inicio_vigencia, fim_vigencia, data_inicio_vigencia, data_fim_vigencia,
    cnpj_matriz, registro_ativo, possui_cadastro_empresa, razao_social,
    natureza_juridica, capital_social, ente_federativo_responsavel,
    qualificacao_responsavel, opcao_pelo_mei, opcao_pelo_simples,
    data_inicio_atividade, nome_fantasia_matriz, situacao_cadastral_matriz,
    motivo_situacao_cadastral, cep, uf, municipio, codigo_ibge_municipio,
    bairro_campo_livre, localidade_cep, localidade_analitica,
    latitude_cep, longitude_cep, logradouro_completo,
    codigo_porte_empresa, porte_cadastral, enquadramento_porte_analitico,
    telefone_principal, telefone_secundario, correio_eletronico,
    situacao_especial, data_carga_etl
)
WITH dom_nat AS (
    SELECT TRIM(codigo) AS codigo, MAX(UPPER(TRIM(descricao))) AS descricao
    FROM tcc_receita.bronze.naturezas_juridicas
    GROUP BY TRIM(codigo)
),
dom_qual AS (
    SELECT TRIM(codigo) AS codigo, MAX(UPPER(TRIM(descricao))) AS descricao
    FROM tcc_receita.bronze.qualificacoes_socios
    GROUP BY TRIM(codigo)
),
dom_mun AS (
    SELECT TRIM(codigo) AS codigo, MAX(UPPER(TRIM(descricao))) AS descricao
    FROM tcc_receita.bronze.municipios
    GROUP BY TRIM(codigo)
),
dom_mot AS (
    SELECT TRIM(codigo) AS codigo, MAX(UPPER(TRIM(descricao))) AS descricao
    FROM tcc_receita.bronze.motivos_situacao
    GROUP BY TRIM(codigo)
),
cep_origem AS (
    SELECT
        REGEXP_REPLACE(TRIM(cep), '[^0-9]', '') AS cep_limpo,
        NULLIF(UPPER(TRIM(uf)), '') AS uf_cep,
        NULLIF(UPPER(TRIM(cidade)), '') AS cidade_cep,
        NULLIF(UPPER(TRIM(REGEXP_REPLACE(localidade, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS localidade_cep,
        REGEXP_REPLACE(TRIM(codigo_ibge), '[^0-9]', '') AS codigo_ibge_limpo,
        latitude AS latitude_cep,
        longitude AS longitude_cep
    FROM tcc_receita.bronze.ceps
    WHERE cep IS NOT NULL
),
cep_ordenado AS (
    SELECT
        cep_limpo AS cep,
        uf_cep,
        cidade_cep,
        localidade_cep,
        CASE WHEN LENGTH(codigo_ibge_limpo) = 7 THEN codigo_ibge_limpo END AS codigo_ibge_municipio,
        latitude_cep,
        longitude_cep,
        ROW_NUMBER() OVER (
            PARTITION BY cep_limpo
            ORDER BY
                CASE WHEN codigo_ibge_limpo RLIKE '^[0-9]{7}$' THEN 0 ELSE 1 END,
                CASE WHEN localidade_cep IS NOT NULL THEN 0 ELSE 1 END,
                COALESCE(uf_cep, ''), COALESCE(cidade_cep, ''),
                COALESCE(localidade_cep, ''), COALESCE(codigo_ibge_limpo, ''),
                COALESCE(CAST(latitude_cep AS STRING), ''),
                COALESCE(CAST(longitude_cep AS STRING), '')
        ) AS rn
    FROM cep_origem
    WHERE LENGTH(cep_limpo) = 8 AND cep_limpo RLIKE '^[0-9]{8}$'
),
dom_cep AS (
    SELECT cep, uf_cep, cidade_cep, localidade_cep,
           codigo_ibge_municipio, latitude_cep, longitude_cep
    FROM cep_ordenado
    WHERE rn = 1
),
est_matriz AS (
    SELECT
        cnpj_basico,
        cnpj_ordem,
        cnpj_dv,
        data_inicio_atividade,
        nome_fantasia,
        situacao_cadastral_desc,
        motivo_situacao_cadastral,
        CASE WHEN LENGTH(REGEXP_REPLACE(TRIM(cep), '[^0-9]', '')) = 8
             THEN REGEXP_REPLACE(TRIM(cep), '[^0-9]', '') END AS cep,
        uf,
        municipio,
        bairro,
        tipo_logradouro,
        logradouro,
        numero,
        telefone_1_formatado,
        telefone_2_formatado,
        correio_eletronico,
        situacao_especial,
        CASE WHEN LENGTH(REGEXP_REPLACE(TRIM(cnae_fiscal_principal), '[^0-9]', '')) = 7
             THEN REGEXP_REPLACE(TRIM(cnae_fiscal_principal), '[^0-9]', '') END AS cnae_primario,
        NULLIF(ARRAY_JOIN(ARRAY_SORT(ARRAY_DISTINCT(FILTER(
            TRANSFORM(
                SPLIT(COALESCE(TRIM(cnae_fiscal_secundaria), ''), ','),
                x -> CASE
                    WHEN LENGTH(REGEXP_REPLACE(TRIM(x), '[^0-9]', '')) = 7
                    THEN REGEXP_REPLACE(TRIM(x), '[^0-9]', '')
                END
            ),
            x -> x IS NOT NULL
        ))), ','), '') AS lista_cnaes_secundarios,
        valid_since,
        COALESCE(valid_until, '9999-12') AS valid_until
    FROM tcc_receita.silver_intermediate.estabelecimentos
    WHERE identificador_matriz_filial = '1'
      AND cnpj_basico IS NOT NULL
      AND valid_since IS NOT NULL
      AND valid_since < COALESCE(valid_until, '9999-12')
),
emp_limpa AS (
    SELECT
        pk, cnpj_basico, razao_social, natureza_juridica,
        qualificacao_responsavel, capital_social, porte_empresa,
        ente_federativo_responsavel, valid_since,
        COALESCE(valid_until, '9999-12') AS valid_until
    FROM tcc_receita.silver_intermediate.empresas
    WHERE cnpj_basico IS NOT NULL
      AND valid_since IS NOT NULL
      AND valid_since < COALESCE(valid_until, '9999-12')
),
sim_limpa AS (
    SELECT
        pk, cnpj_basico, opcao_pelo_mei_bool, opcao_pelo_simples_bool,
        valid_since, COALESCE(valid_until, '9999-12') AS valid_until
    FROM tcc_receita.silver_intermediate.dados_simples
    WHERE cnpj_basico IS NOT NULL
      AND valid_since IS NOT NULL
      AND valid_since < COALESCE(valid_until, '9999-12')
),
-- A chave de relacionamento é cnpj_basico; pk é uma chave técnica serializada.
emp_est AS (
    SELECT
        est.*,
        (emp.pk IS NOT NULL) AS possui_cadastro_empresa,
        emp.razao_social,
        emp.natureza_juridica,
        emp.qualificacao_responsavel,
        emp.capital_social,
        emp.porte_empresa,
        emp.ente_federativo_responsavel,
        GREATEST(est.valid_since, COALESCE(emp.valid_since, est.valid_since)) AS vs_1,
        LEAST(est.valid_until, COALESCE(emp.valid_until, est.valid_until)) AS vu_1
    FROM est_matriz est
    LEFT JOIN emp_limpa emp
        ON est.cnpj_basico = emp.cnpj_basico
       AND est.valid_since < emp.valid_until
       AND est.valid_until > emp.valid_since
    WHERE GREATEST(est.valid_since, COALESCE(emp.valid_since, est.valid_since))
        < LEAST(est.valid_until, COALESCE(emp.valid_until, est.valid_until))
),
unificado_raw AS (
    SELECT
        ee.*,
        CASE WHEN sim.pk IS NULL THEN CAST(NULL AS BOOLEAN) ELSE sim.opcao_pelo_mei_bool END AS opcao_pelo_mei_bool,
        CASE WHEN sim.pk IS NULL THEN CAST(NULL AS BOOLEAN) ELSE sim.opcao_pelo_simples_bool END AS opcao_pelo_simples_bool,
        GREATEST(ee.vs_1, COALESCE(sim.valid_since, ee.vs_1)) AS valid_since_final,
        LEAST(ee.vu_1, COALESCE(sim.valid_until, ee.vu_1)) AS valid_until_final,
        CONCAT_WS('|', COALESCE(ee.cnae_primario, 'SEM_PRIMARIO'),
                       COALESCE(ee.lista_cnaes_secundarios, 'SEM_SECUNDARIO')) AS nk_grupo_cnae_calc
    FROM emp_est ee
    LEFT JOIN sim_limpa sim
        ON ee.cnpj_basico = sim.cnpj_basico
       AND ee.vs_1 < sim.valid_until
       AND ee.vu_1 > sim.valid_since
    WHERE GREATEST(ee.vs_1, COALESCE(sim.valid_since, ee.vs_1))
        < LEAST(ee.vu_1, COALESCE(sim.valid_until, ee.vu_1))
),
dim_enriquecida AS (
    SELECT
        (XXHASH64(CONCAT_WS('|', u.cnpj_basico, u.valid_since_final)) & 9223372036854775807) AS sk_empresa,
        CASE WHEN u.cnae_primario IS NULL AND u.lista_cnaes_secundarios IS NULL
             THEN CAST(-1 AS BIGINT)
             ELSE COALESCE(grp.sk_grupo_cnae, CAST(-1 AS BIGINT)) END AS sk_grupo_cnae,
        CONCAT_WS('|', u.cnpj_basico, u.valid_since_final) AS nk_empresa,
        u.cnpj_basico AS cnpj_raiz,
        u.valid_since_final AS inicio_vigencia,
        NULLIF(u.valid_until_final, '9999-12') AS fim_vigencia,
        TO_DATE(CONCAT(u.valid_since_final, '-01'), 'yyyy-MM-dd') AS data_inicio_vigencia,
        TO_DATE(CONCAT(NULLIF(u.valid_until_final, '9999-12'), '-01'), 'yyyy-MM-dd') AS data_fim_vigencia,
        CONCAT(u.cnpj_basico, u.cnpj_ordem, u.cnpj_dv) AS cnpj_matriz,
        u.valid_until_final = '9999-12' AS registro_ativo,
        u.possui_cadastro_empresa,
        u.razao_social,
        COALESCE(nat.descricao, u.natureza_juridica) AS natureza_juridica,
        CAST(u.capital_social AS DOUBLE) AS capital_social,
        u.ente_federativo_responsavel,
        COALESCE(qual.descricao, u.qualificacao_responsavel) AS qualificacao_responsavel,
        u.opcao_pelo_mei_bool AS opcao_pelo_mei,
        u.opcao_pelo_simples_bool AS opcao_pelo_simples,
        CAST(try_to_timestamp(u.data_inicio_atividade, 'yyyyMMdd') AS DATE) AS data_inicio_atividade,
        u.nome_fantasia AS nome_fantasia_matriz,
        u.situacao_cadastral_desc AS situacao_cadastral_matriz,
        COALESCE(mot.descricao, u.motivo_situacao_cadastral) AS motivo_situacao_cadastral,
        u.cep,
        COALESCE(cep_ref.uf_cep, u.uf) AS uf,
        COALESCE(cep_ref.cidade_cep, mun.descricao, u.municipio) AS municipio,
        cep_ref.codigo_ibge_municipio,
        u.bairro AS bairro_campo_livre,
        cep_ref.localidade_cep,
        COALESCE(cep_ref.localidade_cep, u.bairro) AS localidade_analitica,
        cep_ref.latitude_cep,
        cep_ref.longitude_cep,
        CONCAT_WS(' ', u.tipo_logradouro, u.logradouro, u.numero) AS logradouro_completo,
        u.porte_empresa AS codigo_porte_empresa,
        CASE
            WHEN u.porte_empresa = '01' THEN 'ME'
            WHEN u.porte_empresa = '03' THEN 'EPP'
            WHEN u.porte_empresa = '05' THEN 'DEMAIS'
            ELSE 'NAO INFORMADO'
        END AS porte_cadastral,
        CASE
            WHEN u.opcao_pelo_mei_bool = TRUE THEN 'MEI'
            WHEN u.porte_empresa = '01' THEN 'ME'
            WHEN u.porte_empresa = '03' THEN 'EPP'
            WHEN u.porte_empresa = '05' THEN 'DEMAIS'
            ELSE 'NAO INFORMADO'
        END AS enquadramento_porte_analitico,
        u.telefone_1_formatado AS telefone_principal,
        u.telefone_2_formatado AS telefone_secundario,
        u.correio_eletronico,
        u.situacao_especial,
        CURRENT_TIMESTAMP() AS data_carga_etl
    FROM unificado_raw u
    LEFT JOIN tcc_receita.silver_final.dim_grupo_cnae grp
        ON grp.nk_grupo_cnae = u.nk_grupo_cnae_calc
    LEFT JOIN dom_nat nat ON u.natureza_juridica = nat.codigo
    LEFT JOIN dom_qual qual ON u.qualificacao_responsavel = qual.codigo
    LEFT JOIN dom_mun mun ON u.municipio = mun.codigo
    LEFT JOIN dom_mot mot ON u.motivo_situacao_cadastral = mot.codigo
    LEFT JOIN dom_cep cep_ref ON u.cep = cep_ref.cep
),
ghost_row AS (
    SELECT
        CAST(-1 AS BIGINT) AS sk_empresa,
        CAST(-1 AS BIGINT) AS sk_grupo_cnae,
        '__NAO_APLICAVEL__' AS nk_empresa,
        '00000000' AS cnpj_raiz,
        '1900-01' AS inicio_vigencia,
        CAST(NULL AS STRING) AS fim_vigencia,
        DATE'1900-01-01' AS data_inicio_vigencia,
        CAST(NULL AS DATE) AS data_fim_vigencia,
        '00000000000000' AS cnpj_matriz,
        FALSE AS registro_ativo,
        FALSE AS possui_cadastro_empresa,
        'NAO APLICAVEL / INEXISTENTE' AS razao_social,
        'NAO APLICAVEL' AS natureza_juridica,
        CAST(0.0 AS DOUBLE) AS capital_social,
        CAST(NULL AS STRING) AS ente_federativo_responsavel,
        'NAO APLICAVEL' AS qualificacao_responsavel,
        FALSE AS opcao_pelo_mei,
        FALSE AS opcao_pelo_simples,
        CAST(NULL AS DATE) AS data_inicio_atividade,
        CAST(NULL AS STRING) AS nome_fantasia_matriz,
        'INEXISTENTE' AS situacao_cadastral_matriz,
        'NAO APLICAVEL' AS motivo_situacao_cadastral,
        '00000000' AS cep,
        'NA' AS uf,
        'NAO APLICAVEL' AS municipio,
        CAST(NULL AS STRING) AS codigo_ibge_municipio,
        'NAO APLICAVEL' AS bairro_campo_livre,
        'NAO APLICAVEL' AS localidade_cep,
        'NAO APLICAVEL' AS localidade_analitica,
        CAST(NULL AS DOUBLE) AS latitude_cep,
        CAST(NULL AS DOUBLE) AS longitude_cep,
        CAST(NULL AS STRING) AS logradouro_completo,
        '00' AS codigo_porte_empresa,
        'NAO APLICAVEL' AS porte_cadastral,
        'NAO APLICAVEL' AS enquadramento_porte_analitico,
        CAST(NULL AS STRING) AS telefone_principal,
        CAST(NULL AS STRING) AS telefone_secundario,
        CAST(NULL AS STRING) AS correio_eletronico,
        CAST(NULL AS STRING) AS situacao_especial,
        CURRENT_TIMESTAMP() AS data_carga_etl
)
SELECT * FROM ghost_row
UNION ALL
SELECT
    sk_empresa, sk_grupo_cnae, nk_empresa, cnpj_raiz,
    inicio_vigencia, fim_vigencia, data_inicio_vigencia, data_fim_vigencia,
    cnpj_matriz, registro_ativo, possui_cadastro_empresa, razao_social,
    natureza_juridica, capital_social, ente_federativo_responsavel,
    qualificacao_responsavel, opcao_pelo_mei, opcao_pelo_simples,
    data_inicio_atividade, nome_fantasia_matriz, situacao_cadastral_matriz,
    motivo_situacao_cadastral, cep, uf, municipio, codigo_ibge_municipio,
    bairro_campo_livre, localidade_cep, localidade_analitica,
    latitude_cep, longitude_cep, logradouro_completo,
    codigo_porte_empresa, porte_cadastral, enquadramento_porte_analitico,
    telefone_principal, telefone_secundario, correio_eletronico,
    situacao_especial, data_carga_etl
FROM dim_enriquecida;
