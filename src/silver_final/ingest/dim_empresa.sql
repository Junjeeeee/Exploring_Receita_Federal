CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_empresa (
    sk_empresa BIGINT COMMENT 'Chave substituta (Hash 64-bit positivo sobre nk_empresa). -1 para Não Aplicável.',
    sk_grupo_cnae BIGINT COMMENT 'FK para dim_grupo_cnae calculada a partir da chave canônica da combinação de CNAEs.',
    nk_empresa STRING COMMENT 'Chave natural da versão temporal da empresa: cnpj_raiz|inicio_vigencia.',
    cnpj_raiz STRING COMMENT 'Radical de 8 dígitos do CNPJ.',
    inicio_vigencia STRING COMMENT 'Mês inicial de vigência da versão (YYYY-MM, limite inclusivo).',
    fim_vigencia STRING COMMENT 'Mês final de vigência da versão (YYYY-MM, limite exclusivo; NULL se versão ativa).',
    data_inicio_vigencia DATE COMMENT 'Primeiro dia do mês de início de vigência (YYYY-MM-01, limite inclusivo).',
    data_fim_vigencia DATE COMMENT 'Primeiro dia do mês de fim de vigência (YYYY-MM-01, limite exclusivo; NULL se ativa).',
    cnpj_matriz STRING COMMENT 'CNPJ completo de 14 dígitos do estabelecimento matriz.',
    registro_ativo BOOLEAN COMMENT 'TRUE se a versão representa o estado atual vigente (fim_vigencia IS NULL).',
    possui_cadastro_empresa BOOLEAN COMMENT 'Indica se a matriz possui registo correspondente na tabela de empresas (raiz).',
    razao_social STRING COMMENT 'Razão social da empresa (com acentuação preservada).',
    natureza_juridica STRING COMMENT 'Descrição da natureza jurídica.',
    capital_social DOUBLE COMMENT 'Capital social declarado em Reais (BRL).',
    ente_federativo_responsavel STRING COMMENT 'Ente federativo responsável, quando aplicável.',
    qualificacao_responsavel STRING COMMENT 'Descrição da qualificação do responsável legal.',
    opcao_pelo_mei BOOLEAN COMMENT 'Indica se a empresa é optante pelo SIMEI no período.',
    opcao_pelo_simples BOOLEAN COMMENT 'Indica se a empresa é optante pelo Simples Nacional no período.',
    data_inicio_atividade DATE COMMENT 'Data de início de atividade da matriz validada no calendário.',
    nome_fantasia_matriz STRING COMMENT 'Nome fantasia do estabelecimento matriz.',
    situacao_cadastral_matriz STRING COMMENT 'Situação cadastral da matriz (ATIVA, BAIXADA, INAPTA, SUSPENSA, NULA).',
    motivo_situacao_cadastral STRING COMMENT 'Descrição do motivo da situação cadastral.',
    cep STRING COMMENT 'CEP de 8 dígitos da matriz.',
    uf STRING COMMENT 'Sigla da UF consolidada (prioriza diretório de CEP, com fallback para o cadastro RFB).',
    municipio STRING COMMENT 'Nome do município consolidado (prioriza diretório IBGE/CEP, com fallback para RFB).',
    codigo_ibge_municipio STRING COMMENT 'Código IBGE de 7 dígitos do município.',
    bairro_campo_livre STRING COMMENT 'Bairro declarado originalmente no cadastro da Receita Federal.',
    localidade_cep STRING COMMENT 'Localidade/bairro normalizado extraído do diretório oficial de CEPs.',
    localidade_analitica STRING COMMENT 'Localidade consolidada para filtros analíticos: COALESCE(localidade_cep, bairro_campo_livre).',
    latitude_cep DOUBLE COMMENT 'Latitude do centroide do CEP.',
    longitude_cep DOUBLE COMMENT 'Longitude do centroide do CEP.',
    logradouro_completo STRING COMMENT 'Endereço concatenado (tipo, logradouro e número).',
    codigo_porte_empresa STRING COMMENT 'Código original do porte na Receita Federal (01, 03, 05).',
    porte_cadastral STRING COMMENT 'Porte cadastral estrito da Receita Federal (ME, EPP, DEMAIS, NAO INFORMADO).',
    enquadramento_porte_analitico STRING COMMENT 'Classificação analítica combinando opção pelo MEI e porte cadastral (MEI, ME, EPP, DEMAIS).',
    telefone_principal STRING COMMENT 'Telefone principal formatado.',
    telefone_secundario STRING COMMENT 'Telefone secundário formatado.',
    correio_eletronico STRING COMMENT 'E-mail institucional.',
    situacao_especial STRING COMMENT 'Situação especial declarada na RFB.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na camada Silver Final.'
)
USING DELTA
COMMENT 'Camada Silver Final: Dimensão temporal SCD2 de Empresas (granularidade: cnpj_raiz + inicio_vigencia, unificando matriz, raiz empresarial e Simples Nacional no intervalo semiaberto [inicio_vigencia, fim_vigencia)).';

INSERT OVERWRITE tcc_receita.silver_final.dim_empresa
WITH dom_nat AS (
    SELECT TRIM(codigo) AS codigo, MAX(UPPER(TRIM(descricao))) AS descricao 
    FROM tcc_receita.bronze.naturezas_juridicas GROUP BY TRIM(codigo)
),
dom_qual AS (
    SELECT TRIM(codigo) AS codigo, MAX(UPPER(TRIM(descricao))) AS descricao 
    FROM tcc_receita.bronze.qualificacoes_socios GROUP BY TRIM(codigo)
),
dom_mun AS (
    SELECT TRIM(codigo) AS codigo, MAX(UPPER(TRIM(descricao))) AS descricao 
    FROM tcc_receita.bronze.municipios GROUP BY TRIM(codigo)
),
dom_mot AS (
    SELECT TRIM(codigo) AS codigo, MAX(UPPER(TRIM(descricao))) AS descricao 
    FROM tcc_receita.bronze.motivos_situacao GROUP BY TRIM(codigo)
),
-- Deduplicação determinística de CEPs preservando acentos Unicode (sem combinar colunas de linhas distintas via MAX)
cep_ordenado AS (
    SELECT 
        LPAD(REGEXP_REPLACE(TRIM(cep), '[^0-9]', ''), 8, '0') AS cep,
        NULLIF(UPPER(TRIM(uf)), '') AS uf_cep,
        NULLIF(UPPER(TRIM(cidade)), '') AS cidade_cep,
        NULLIF(UPPER(TRIM(REGEXP_REPLACE(bairro, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS localidade_cep,
        NULLIF(TRIM(codigo_ibge), '') AS codigo_ibge_municipio,
        latitude AS latitude_cep,
        longitude AS longitude_cep,
        ROW_NUMBER() OVER (
            PARTITION BY LPAD(REGEXP_REPLACE(TRIM(cep), '[^0-9]', ''), 8, '0')
            ORDER BY 
                CASE WHEN NULLIF(TRIM(codigo_ibge), '') IS NOT NULL THEN 0 ELSE 1 END,
                CASE WHEN NULLIF(TRIM(bairro), '') IS NOT NULL THEN 0 ELSE 1 END,
                uf ASC,
                cidade ASC
        ) AS rn
    FROM tcc_receita.bronze.ceps
    WHERE cep IS NOT NULL 
      AND LENGTH(REGEXP_REPLACE(TRIM(cep), '[^0-9]', '')) = 8
),
dom_cep AS (
    SELECT cep, uf_cep, cidade_cep, localidade_cep, codigo_ibge_municipio, latitude_cep, longitude_cep
    FROM cep_ordenado
    WHERE rn = 1
),
-- Padronização do sentinela temporal para '9999-12' (mesmo formato YYYY-MM de valid_since/valid_until)
est_matriz_raw AS (
    SELECT 
        cnpj_basico, cnpj_ordem, cnpj_dv, data_inicio_atividade, nome_fantasia,
        situacao_cadastral_desc, motivo_situacao_cadastral, 
        LPAD(REGEXP_REPLACE(TRIM(cep), '[^0-9]', ''), 8, '0') AS cep,
        uf, municipio, bairro, tipo_logradouro, logradouro, numero, 
        telefone_1_formatado, telefone_2_formatado, correio_eletronico, situacao_especial,
        NULLIF(TRIM(cnae_fiscal_principal), '') AS cnae_primario,
        NULLIF(
            ARRAY_JOIN(
                ARRAY_SORT(
                    ARRAY_DISTINCT(
                        FILTER(
                            TRANSFORM(SPLIT(COALESCE(TRIM(cnae_fiscal_secundaria), ''), ','), x -> TRIM(x)),
                            x -> x != ''
                        )
                    )
                ),
                ','
            ),
            ''
        ) AS lista_cnaes_secundarios,
        valid_since, 
        COALESCE(valid_until, '9999-12') AS valid_until,
        ROW_NUMBER() OVER (
            PARTITION BY cnpj_basico, valid_since 
            ORDER BY cnpj_ordem ASC, updated_at DESC
        ) AS rn
    FROM tcc_receita.silver_intermediate.estabelecimentos 
    WHERE identificador_matriz_filial = '1'
),
est_matriz AS (
    SELECT * FROM est_matriz_raw WHERE rn = 1 AND valid_since < valid_until
),
emp_limpa AS (
    SELECT 
        pk, razao_social, natureza_juridica, qualificacao_responsavel,
        capital_social, porte_empresa, ente_federativo_responsavel,
        valid_since, 
        COALESCE(valid_until, '9999-12') AS valid_until
    FROM tcc_receita.silver_intermediate.empresas
    WHERE valid_since < COALESCE(valid_until, '9999-12')
),
sim_limpa AS (
    SELECT 
        pk, opcao_pelo_mei_bool, opcao_pelo_simples_bool,
        valid_since, 
        COALESCE(valid_until, '9999-12') AS valid_until
    FROM tcc_receita.silver_intermediate.dados_simples
    WHERE valid_since < COALESCE(valid_until, '9999-12')
),
-- LEFT JOIN preserva matrizes mesmo se ausentes em empresas, controlando interseção temporal
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
        LEAST(est.valid_until, COALESCE(emp.valid_until, est.valid_until))    AS vu_1
    FROM est_matriz est
    LEFT JOIN emp_limpa emp
        ON est.cnpj_basico = emp.pk
       AND est.valid_since < emp.valid_until
       AND est.valid_until > emp.valid_since
    WHERE GREATEST(est.valid_since, COALESCE(emp.valid_since, est.valid_since)) < LEAST(est.valid_until, COALESCE(emp.valid_until, est.valid_until))
),
unificado_raw AS (
    SELECT 
        ee.*,
        COALESCE(sim.opcao_pelo_mei_bool, FALSE) AS opcao_pelo_mei_bool,
        COALESCE(sim.opcao_pelo_simples_bool, FALSE) AS opcao_pelo_simples_bool,
        GREATEST(ee.vs_1, COALESCE(sim.valid_since, ee.vs_1)) AS valid_since_final,
        LEAST(ee.vu_1, COALESCE(sim.valid_until, ee.vu_1))    AS valid_until_final,
        CONCAT_WS('|', COALESCE(ee.cnae_primario, 'SEM_PRIMARIO'), COALESCE(ee.lista_cnaes_secundarios, 'SEM_SECUNDARIO')) AS nk_grupo_cnae_calc
    FROM emp_est ee
    LEFT JOIN sim_limpa sim
        ON ee.cnpj_basico = sim.pk
       AND ee.vs_1 < sim.valid_until
       AND ee.vu_1 > sim.valid_since
    WHERE GREATEST(ee.vs_1, COALESCE(sim.valid_since, ee.vs_1)) < LEAST(ee.vu_1, COALESCE(sim.valid_until, ee.vu_1))
),
-- Garante unicidade estrita de (cnpj_basico, valid_since_final)
unificado AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cnpj_basico, valid_since_final 
                   ORDER BY valid_until_final DESC, possui_cadastro_empresa DESC
               ) AS rn_slice
        FROM unificado_raw
    )
    WHERE rn_slice = 1
),
dim_enriquecida AS (
    SELECT /*+ BROADCAST(nat, qual, mun, mot, cep_ref) */
        (xxhash64(CONCAT_WS('|', u.cnpj_basico, u.valid_since_final)) & 9223372036854775807) AS sk_empresa,
        CASE 
            WHEN u.cnae_primario IS NULL AND u.lista_cnaes_secundarios IS NULL THEN CAST(-1 AS BIGINT)
            ELSE (xxhash64(u.nk_grupo_cnae_calc) & 9223372036854775807)
        END AS sk_grupo_cnae,
        CONCAT_WS('|', u.cnpj_basico, u.valid_since_final) AS nk_empresa,
        u.cnpj_basico AS cnpj_raiz,
        u.valid_since_final AS inicio_vigencia,
        NULLIF(u.valid_until_final, '9999-12') AS fim_vigencia,
        TO_DATE(CONCAT(u.valid_since_final, '-01'), 'yyyy-MM-dd') AS data_inicio_vigencia,
        TO_DATE(CONCAT(NULLIF(u.valid_until_final, '9999-12'), '-01'), 'yyyy-MM-dd') AS data_fim_vigencia,
        CONCAT(u.cnpj_basico, u.cnpj_ordem, u.cnpj_dv) AS cnpj_matriz,
        (u.valid_until_final = '9999-12') AS registro_ativo,
        u.possui_cadastro_empresa,
        u.razao_social AS razao_social,
        COALESCE(nat.descricao, u.natureza_juridica) AS natureza_juridica,
        CAST(u.capital_social AS DOUBLE) AS capital_social,
        u.ente_federativo_responsavel AS ente_federativo_responsavel,
        COALESCE(qual.descricao, u.qualificacao_responsavel) AS qualificacao_responsavel,
        u.opcao_pelo_mei_bool AS opcao_pelo_mei,
        u.opcao_pelo_simples_bool AS opcao_pelo_simples,
        TO_DATE(u.data_inicio_atividade, 'yyyyMMdd') AS data_inicio_atividade,
        u.nome_fantasia AS nome_fantasia_matriz,
        u.situacao_cadastral_desc AS situacao_cadastral_matriz,
        COALESCE(mot.descricao, u.motivo_situacao_cadastral) AS motivo_situacao_cadastral,
        u.cep AS cep,
        COALESCE(cep_ref.uf_cep, u.uf) AS uf,
        COALESCE(cep_ref.cidade_cep, mun.descricao, u.municipio) AS municipio,
        cep_ref.codigo_ibge_municipio AS codigo_ibge_municipio,
        u.bairro AS bairro_campo_livre,
        cep_ref.localidade_cep AS localidade_cep,
        COALESCE(cep_ref.localidade_cep, u.bairro) AS localidade_analitica,
        cep_ref.latitude_cep AS latitude_cep,
        cep_ref.longitude_cep AS longitude_cep,
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
        u.correio_eletronico AS correio_eletronico,
        u.situacao_especial AS situacao_especial,
        NOW() AS data_carga_etl
    FROM unificado u
    LEFT JOIN dom_nat nat     ON u.natureza_juridica = nat.codigo
    LEFT JOIN dom_qual qual   ON u.qualificacao_responsavel = qual.codigo
    LEFT JOIN dom_mun mun     ON u.municipio = mun.codigo
    LEFT JOIN dom_mot mot     ON u.motivo_situacao_cadastral = mot.codigo
    LEFT JOIN dom_cep cep_ref ON u.cep = cep_ref.cep
),
ghost_row AS (
    SELECT
        CAST(-1 AS BIGINT) AS sk_empresa,
        CAST(-1 AS BIGINT) AS sk_grupo_cnae,
        'NAO_APLICAVEL' AS nk_empresa,
        '00000000' AS cnpj_raiz,
        '1900-01' AS inicio_vigencia,
        CAST(NULL AS STRING) AS fim_vigencia,
        TO_DATE('1900-01-01', 'yyyy-MM-dd') AS data_inicio_vigencia,
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
        NOW() AS data_carga_etl
)
SELECT * FROM ghost_row
UNION ALL
SELECT * FROM dim_enriquecida;