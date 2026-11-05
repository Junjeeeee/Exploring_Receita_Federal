CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_empresa AS
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
dom_cep AS (
    SELECT 
        LPAD(REGEXP_REPLACE(TRIM(cep), '[^0-9]', ''), 8, '0') AS cep,
        MAX(NULLIF(UPPER(TRIM(uf)), '')) AS uf_cep,
        MAX(NULLIF(UPPER(TRIM(cidade)), '')) AS cidade_cep,
        MAX(NULLIF(UPPER(TRIM(REGEXP_REPLACE(bairro, '[^a-zA-Z0-9 ]', ''))), '')) AS localidade_cep,
        MAX(NULLIF(TRIM(codigo_ibge), '')) AS codigo_ibge_municipio,
        MAX(latitude) AS latitude_cep,
        MAX(longitude) AS longitude_cep
    FROM tcc_receita.bronze.ceps
    WHERE cep IS NOT NULL
    GROUP BY 1
),
-- Podagem precoce de colunas (Column Pruning) e proteção de valid_until NULL para o intervalo temporal
est_matriz AS (
    SELECT 
        cnpj_basico, cnpj_ordem, cnpj_dv, data_inicio_atividade, nome_fantasia,
        situacao_cadastral_desc, motivo_situacao_cadastral, 
        LPAD(REGEXP_REPLACE(TRIM(cep), '[^0-9]', ''), 8, '0') AS cep,
        uf, municipio, bairro, tipo_logradouro, logradouro, numero, 
        telefone_1_formatado, telefone_2_formatado, correio_eletronico, situacao_especial,
        cnae_fiscal_principal, cnae_fiscal_secundaria, 
        valid_since, 
        COALESCE(valid_until, '9999-12-31') AS valid_until
    FROM tcc_receita.silver_intermediate.estabelecimentos 
    WHERE identificador_matriz_filial = '1'
),
emp_limpa AS (
    SELECT 
        pk, razao_social, natureza_juridica, qualificacao_responsavel,
        capital_social, porte_empresa, ente_federativo_responsavel,
        valid_since, 
        COALESCE(valid_until, '9999-12-31') AS valid_until
    FROM tcc_receita.silver_intermediate.empresas
),
sim_limpa AS (
    SELECT 
        pk, opcao_pelo_mei_bool, opcao_pelo_simples_bool,
        valid_since, 
        COALESCE(valid_until, '9999-12-31') AS valid_until
    FROM tcc_receita.silver_intermediate.dados_simples
),
emp_est AS (
    SELECT 
        est.*,
        emp.razao_social,
        emp.natureza_juridica,
        emp.qualificacao_responsavel,
        emp.capital_social,
        emp.porte_empresa,
        emp.ente_federativo_responsavel,
        GREATEST(est.valid_since, emp.valid_since) AS vs_1,
        LEAST(est.valid_until, emp.valid_until)    AS vu_1
    FROM est_matriz est
    INNER JOIN emp_limpa emp
        ON est.cnpj_basico = emp.pk
       AND est.valid_since < emp.valid_until
       AND est.valid_until > emp.valid_since
),
unificado AS (
    SELECT 
        ee.*,
        sim.opcao_pelo_mei_bool,
        sim.opcao_pelo_simples_bool,
        GREATEST(ee.vs_1, COALESCE(sim.valid_since, ee.vs_1)) AS valid_since_final,
        LEAST(ee.vu_1, COALESCE(sim.valid_until, ee.vu_1))    AS valid_until_final
    FROM emp_est ee
    LEFT JOIN sim_limpa sim
        ON ee.cnpj_basico = sim.pk
       AND ee.vs_1 < sim.valid_until
       AND ee.vu_1 > sim.valid_since
    WHERE GREATEST(ee.vs_1, COALESCE(sim.valid_since, ee.vs_1)) < LEAST(ee.vu_1, COALESCE(sim.valid_until, ee.vu_1))
),
dim_enriquecida AS (
    SELECT /*+ BROADCAST(nat, qual, mun, mot, cep_ref) */
        -- Surrogate Key numérica gerada em paralelo sem ordenação global:
        ABS(xxhash64(u.cnpj_basico, u.valid_since_final)) AS sk_empresa,
        -- sk_grupo_cnae calculada inline sem precisar de JOIN com dim_grupo_cnae:
        ABS(xxhash64(COALESCE(u.cnae_fiscal_principal, ''), COALESCE(u.cnae_fiscal_secundaria, ''))) AS sk_grupo_cnae,
        CONCAT_WS('_', u.cnpj_basico, u.valid_since_final) AS nk_empresa,
        u.cnpj_basico AS cnpj_raiz,
        u.valid_since_final AS inicio_vigencia,
        NULLIF(u.valid_until_final, '9999-12-31') AS fim_vigencia,
        CONCAT(u.cnpj_basico, u.cnpj_ordem, u.cnpj_dv) AS cnpj_matriz,
        (u.valid_until_final = '9999-12-31') AS registro_ativo,
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
        CASE 
            WHEN u.opcao_pelo_mei_bool = TRUE OR u.natureza_juridica = '2135' THEN 'MEI'
            WHEN u.porte_empresa = '01' THEN 'ME'
            WHEN u.porte_empresa = '03' THEN 'EPP'
            WHEN u.porte_empresa = '05' THEN 'DEMAIS' 
            ELSE NULL 
        END AS porte_empresa,
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
    -- Linha fantasma (-1) para integridade referencial na Fato (ex: estado anterior de EMPRESA NASCEU)
    SELECT
        CAST(-1 AS BIGINT) AS sk_empresa,
        CAST(-1 AS BIGINT) AS sk_grupo_cnae,
        'NAO_APLICAVEL' AS nk_empresa,
        '00000000' AS cnpj_raiz,
        '1900-01' AS inicio_vigencia,
        CAST(NULL AS STRING) AS fim_vigencia,
        '00000000000000' AS cnpj_matriz,
         FALSE AS registro_ativo,
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
        'NAO APLICAVEL' AS porte_empresa,
        CAST(NULL AS STRING) AS telefone_principal,
        CAST(NULL AS STRING) AS telefone_secundario,
        CAST(NULL AS STRING) AS correio_eletronico,
        CAST(NULL AS STRING) AS situacao_especial,
        NOW() AS data_carga_etl
)
SELECT * FROM ghost_row
UNION ALL
SELECT * FROM dim_enriquecida;s