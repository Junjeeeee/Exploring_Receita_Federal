CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE OR REPLACE TABLE tcc_receita.silver_final.dim_company AS
WITH dom_nat AS (
    SELECT codigo, MAX(UPPER(TRIM(descricao))) AS descricao 
    FROM tcc_receita.bronze.naturezas_juridicas GROUP BY codigo
),
dom_qual AS (
    SELECT codigo, MAX(UPPER(TRIM(descricao))) AS descricao 
    FROM tcc_receita.bronze.qualificacoes_socios GROUP BY codigo
),
dom_mun AS (
    SELECT codigo, MAX(UPPER(TRIM(descricao))) AS descricao 
    FROM tcc_receita.bronze.municipios GROUP BY codigo
),
dom_mot AS (
    SELECT codigo, MAX(UPPER(TRIM(descricao))) AS descricao 
    FROM tcc_receita.bronze.motivos_situacao GROUP BY codigo
),
-- Podagem precoce de colunas (Column Pruning) para minimizar tráfego de Shuffle
est_matriz AS (
    SELECT 
        cnpj_basico, cnpj_ordem, cnpj_dv, data_inicio_atividade, nome_fantasia,
        situacao_cadastral_desc, motivo_situacao_cadastral, cep, uf, municipio,
        bairro, tipo_logradouro, logradouro, numero, telefone_1_formatado,
        telefone_2_formatado, correio_eletronico, situacao_especial,
        cnae_fiscal_principal, cnae_fiscal_secundaria, valid_since, valid_until
    FROM tcc_receita.silver_intermediate.estabelecimentos 
    WHERE identificador_matriz_filial = '1'
),
emp_limpa AS (
    SELECT 
        pk, razao_social, natureza_juridica, qualificacao_responsavel,
        capital_social, porte_empresa, ente_federativo_responsavel,
        valid_since, valid_until
    FROM tcc_receita.silver_intermediate.empresas
),
sim_limpa AS (
    SELECT 
        pk, opcao_pelo_mei_bool, opcao_pelo_simples_bool,
        valid_since, valid_until
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
)
SELECT /*+ BROADCAST(nat, qual, mun, mot) */
    -- Surrogate Key numérica gerada em paralelo sem ordenação global:
    ABS(xxhash64(u.cnpj_basico, u.valid_since_final)) AS SK_company,
    -- SK_cnae_group calculada inline sem precisar de JOIN com dim_cnae_group:
    ABS(xxhash64(COALESCE(u.cnae_fiscal_principal, ''), COALESCE(u.cnae_fiscal_secundaria, ''))) AS SK_cnae_group,
    concat_ws('_', u.cnpj_basico, u.valid_since_final) AS NK_company,
    u.cnpj_basico AS cnpj_raiz, 
    u.valid_since_final AS valid_since, 
    u.valid_until_final AS valid_until,
    concat(u.cnpj_basico, u.cnpj_ordem, u.cnpj_dv) AS cnpj_matriz,
    (u.valid_until_final = '9999-12-31') AS is_current,
    u.razao_social AS legal_name, 
    COALESCE(nat.descricao, u.natureza_juridica) AS legal_nature, 
    CAST(u.capital_social AS DOUBLE) AS capital_social,
    u.ente_federativo_responsavel AS responsible_federative_entity, 
    COALESCE(qual.descricao, u.qualificacao_responsavel) AS responsable_qualification,
    u.opcao_pelo_mei_bool AS option_for_mei, 
    u.opcao_pelo_simples_bool AS option_for_simples,
    to_date(u.data_inicio_atividade, 'yyyyMMdd') AS start_of_activity_dt, 
    u.nome_fantasia AS matriz_fantasy_name,
    u.situacao_cadastral_desc AS matriz_cadastral_status, 
    COALESCE(mot.descricao, u.motivo_situacao_cadastral) AS cadastral_status_reason,
    u.cep, 
    u.uf AS state, 
    COALESCE(mun.descricao, u.municipio) AS city, 
    u.bairro AS neighborhood,
    concat_ws(' ', u.tipo_logradouro, u.logradouro, u.numero) AS street_address,
    CASE 
        WHEN u.opcao_pelo_mei_bool = TRUE OR u.natureza_juridica = '2135' THEN 'MEI'
        WHEN u.porte_empresa = '01' THEN 'ME'
        WHEN u.porte_empresa = '03' THEN 'EPP'
        WHEN u.porte_empresa = '05' THEN 'DEMAIS' 
        ELSE NULL 
    END AS company_size,
    u.telefone_1_formatado AS primary_phone, 
    u.telefone_2_formatado AS secondary_phone, 
    u.correio_eletronico AS email, 
    u.situacao_especial AS special_situation,
    NOW() AS etl_load_ts
FROM unificado u
LEFT JOIN dom_nat nat  ON u.natureza_juridica = nat.codigo
LEFT JOIN dom_qual qual ON u.qualificacao_responsavel = qual.codigo
LEFT JOIN dom_mun mun  ON u.municipio = mun.codigo
LEFT JOIN dom_mot mot  ON u.motivo_situacao_cadastral = mot.codigo;