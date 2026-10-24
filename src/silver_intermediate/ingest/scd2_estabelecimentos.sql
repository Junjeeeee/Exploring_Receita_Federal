CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.estabelecimentos (
    pk STRING COMMENT 'Chave primária lógica (cnpj_basico-cnpj_ordem-cnpj_dv).',
    cnpj_basico STRING COMMENT 'Radical do CNPJ (primeiros 8 dígitos).',
    cnpj_ordem STRING COMMENT 'Número de ordem do estabelecimento (0001 para matriz).',
    cnpj_dv STRING COMMENT 'Dígitos verificadores do CNPJ.',
    identificador_matriz_filial STRING COMMENT '1 para Matriz, 2 para Filial.',
    nome_fantasia STRING COMMENT 'Nome comercial limpo e padronizado.',
    situacao_cadastral STRING COMMENT 'Código bruto da situação cadastral.',
    situacao_cadastral_desc STRING COMMENT 'Descrição decodificada da situação cadastral (ATIVA, INAPTA, BAIXADA, SUSPENSA, NULA).',
    data_situacao_cadastral STRING COMMENT 'Data da situação cadastral higienizada (AAAAMMDD).',
    motivo_situacao_cadastral STRING COMMENT 'Código do motivo da situação cadastral.',
    nome_cidade_exterior STRING COMMENT 'Nome da cidade no exterior, se aplicável.',
    pais STRING COMMENT 'Código do país.',
    data_inicio_atividade STRING COMMENT 'Data de início das atividades higienizada (AAAAMMDD).',
    cnae_fiscal_principal STRING COMMENT 'Código da atividade econômica primária.',
    cnae_fiscal_secundaria STRING COMMENT 'Lista ordenada de CNAEs secundários.',
    tipo_logradouro STRING COMMENT 'Tipo de logradouro.',
    logradouro STRING COMMENT 'Logradouro limpo e padronizado.',
    numero STRING COMMENT 'Número do endereço.',
    complemento STRING COMMENT 'Complemento limpo e padronizado.',
    bairro STRING COMMENT 'Bairro limpo e padronizado.',
    cep STRING COMMENT 'Código de Endereçamento Postal.',
    uf STRING COMMENT 'Sigla da Unidade Federativa.',
    municipio STRING COMMENT 'Código do município RFB.',
    ddd_1 STRING COMMENT 'DDD do telefone 1.',
    telefone_1 STRING COMMENT 'Número do telefone 1.',
    telefone_1_formatado STRING COMMENT 'Telefone primário padronizado no formato (DD) NNNNNNNN.',
    ddd_2 STRING COMMENT 'DDD do telefone 2.',
    telefone_2 STRING COMMENT 'Número do telefone 2.',
    telefone_2_formatado STRING COMMENT 'Telefone secundário padronizado no formato (DD) NNNNNNNN.',
    ddd_fax STRING COMMENT 'DDD do fax.',
    fax STRING COMMENT 'Número do fax.',
    correio_eletronico STRING COMMENT 'E-mail institucional.',
    situacao_especial STRING COMMENT 'Situação especial, quando aplicável.',
    data_situacao_especial STRING COMMENT 'Data da situação especial higienizada (AAAAMMDD).',
    mes_referencia STRING COMMENT 'Mês de referência do snapshot de origem.',
    inserted_at TIMESTAMP COMMENT 'Timestamp de inserção original na Bronze.',
    updated_at TIMESTAMP COMMENT 'Timestamp da última atualização no SCD2.',
    change_hash STRING COMMENT 'Hash MD5 dos atributos de negócio.',
    valid_since STRING COMMENT 'Mês/Ano de início de vigência da versão (YYYY-MM).',
    valid_until STRING COMMENT 'Mês/Ano de fim de vigência da versão (NULL se versão atual).',
    is_current BOOLEAN COMMENT 'Flag indicando se o registo é a versão ativa atual.'
)
USING DELTA
PARTITIONED BY (is_current)
COMMENT 'Camada Silver Intermediate (SCD2): Histórico de mutações cadastrais de matrizes e filiais (Estabelecimentos).';

-- 1. Idempotência para reprocessamento do intervalo
DELETE FROM tcc_receita.silver_intermediate.estabelecimentos
WHERE valid_since >= COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'));

UPDATE tcc_receita.silver_intermediate.estabelecimentos
SET valid_until = NULL,
    is_current = TRUE,
    updated_at = NOW()
WHERE valid_since < COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))
  AND valid_until >= COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'));

-- 2. Merge SCD2 Set-Based
MERGE INTO tcc_receita.silver_intermediate.estabelecimentos AS target
USING (
    WITH params AS (
        SELECT 
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_fim
    ),
    limpeza_base AS (
        SELECT 
            pk,
            NULLIF(TRIM(cnpj_basico), '') AS cnpj_basico,
            NULLIF(TRIM(cnpj_ordem), '') AS cnpj_ordem,
            NULLIF(TRIM(cnpj_dv), '') AS cnpj_dv,
            NULLIF(TRIM(identificador_matriz_filial), '') AS identificador_matriz_filial,
            NULLIF(UPPER(TRIM(regexp_replace(nome_fantasia, '[^a-zA-Z0-9 ]', ''))), '') AS nome_fantasia,
            NULLIF(TRIM(situacao_cadastral), '') AS situacao_cadastral,
            CASE 
                WHEN TRIM(situacao_cadastral) IN ('02', '2') THEN 'ATIVA'
                WHEN TRIM(situacao_cadastral) IN ('04', '4') THEN 'INAPTA'
                WHEN TRIM(situacao_cadastral) IN ('03', '3') THEN 'SUSPENSA'
                WHEN TRIM(situacao_cadastral) IN ('08', '8') THEN 'BAIXADA'
                WHEN TRIM(situacao_cadastral) IN ('01', '1') THEN 'NULA'
                ELSE NULL
            END AS situacao_cadastral_desc,
            CASE WHEN LENGTH(TRIM(data_situacao_cadastral)) < 8 OR TRIM(data_situacao_cadastral) = '00000000' THEN NULL ELSE TRIM(data_situacao_cadastral) END AS data_situacao_cadastral,
            NULLIF(TRIM(motivo_situacao_cadastral), '') AS motivo_situacao_cadastral,
            NULLIF(TRIM(nome_cidade_exterior), '') AS nome_cidade_exterior,
            NULLIF(TRIM(pais), '') AS pais,
            CASE WHEN LENGTH(TRIM(data_inicio_atividade)) < 8 OR TRIM(data_inicio_atividade) = '00000000' THEN NULL ELSE TRIM(data_inicio_atividade) END AS data_inicio_atividade,
            NULLIF(TRIM(cnae_fiscal_principal), '') AS cnae_fiscal_principal,
            NULLIF(TRIM(cnae_fiscal_secundaria), '') AS cnae_fiscal_secundaria,
            NULLIF(TRIM(tipo_logradouro), '') AS tipo_logradouro,
            NULLIF(UPPER(TRIM(regexp_replace(logradouro, '[^a-zA-Z0-9 ]', ''))), '') AS logradouro,
            NULLIF(TRIM(numero), '') AS numero,
            NULLIF(UPPER(TRIM(regexp_replace(complemento, '[^a-zA-Z0-9 ]', ''))), '') AS complemento,
            NULLIF(UPPER(TRIM(regexp_replace(bairro, '[^a-zA-Z0-9 ]', ''))), '') AS bairro,
            NULLIF(TRIM(cep), '') AS cep,
            NULLIF(TRIM(uf), '') AS uf,
            NULLIF(TRIM(municipio), '') AS municipio,
            NULLIF(TRIM(ddd_1), '') AS ddd_1,
            NULLIF(TRIM(telefone_1), '') AS telefone_1,
            CASE 
                WHEN NULLIF(TRIM(ddd_1), '') IS NOT NULL AND NULLIF(TRIM(telefone_1), '') IS NOT NULL AND TRIM(telefone_1) != '00000000'
                THEN CONCAT('(', TRIM(ddd_1), ') ', TRIM(telefone_1))
                ELSE NULL
            END AS telefone_1_formatado,
            NULLIF(TRIM(ddd_2), '') AS ddd_2,
            NULLIF(TRIM(telefone_2), '') AS telefone_2,
            CASE 
                WHEN NULLIF(TRIM(ddd_2), '') IS NOT NULL AND NULLIF(TRIM(telefone_2), '') IS NOT NULL
                THEN CONCAT('(', TRIM(ddd_2), ') ', TRIM(telefone_2))
                ELSE NULL
            END AS telefone_2_formatado,
            NULLIF(TRIM(ddd_fax), '') AS ddd_fax,
            NULLIF(TRIM(fax), '') AS fax,
            NULLIF(TRIM(correio_eletronico), '') AS correio_eletronico,
            NULLIF(TRIM(situacao_especial), '') AS situacao_especial,
            CASE WHEN LENGTH(TRIM(data_situacao_especial)) < 8 OR TRIM(data_situacao_especial) = '00000000' THEN NULL ELSE TRIM(data_situacao_especial) END AS data_situacao_especial,
            mes_referencia,
            inserted_at,
            change_hash
        FROM tcc_receita.bronze.estabelecimentos b
        CROSS JOIN params p
        WHERE b.mes_referencia >= p.mes_ini
          AND b.mes_referencia <= p.mes_fim
          AND b.pk IS NOT NULL
    ),
    bronze_limpo AS (
        SELECT 
            pk,
            MAX(cnpj_basico) AS cnpj_basico, MAX(cnpj_ordem) AS cnpj_ordem, MAX(cnpj_dv) AS cnpj_dv,
            MAX(identificador_matriz_filial) AS identificador_matriz_filial, MAX(nome_fantasia) AS nome_fantasia,
            MAX(situacao_cadastral) AS situacao_cadastral, MAX(situacao_cadastral_desc) AS situacao_cadastral_desc,
            MAX(data_situacao_cadastral) AS data_situacao_cadastral, MAX(motivo_situacao_cadastral) AS motivo_situacao_cadastral,
            MAX(nome_cidade_exterior) AS nome_cidade_exterior, MAX(pais) AS pais,
            MAX(data_inicio_atividade) AS data_inicio_atividade, MAX(cnae_fiscal_principal) AS cnae_fiscal_principal,
            MAX(cnae_fiscal_secundaria) AS cnae_fiscal_secundaria, MAX(tipo_logradouro) AS tipo_logradouro,
            MAX(logradouro) AS logradouro, MAX(numero) AS numero, MAX(complemento) AS complemento,
            MAX(bairro) AS bairro, MAX(cep) AS cep, MAX(uf) AS uf, MAX(municipio) AS municipio,
            MAX(ddd_1) AS ddd_1, MAX(telefone_1) AS telefone_1, MAX(telefone_1_formatado) AS telefone_1_formatado,
            MAX(ddd_2) AS ddd_2, MAX(telefone_2) AS telefone_2, MAX(telefone_2_formatado) AS telefone_2_formatado,
            MAX(ddd_fax) AS ddd_fax, MAX(fax) AS fax, MAX(correio_eletronico) AS correio_eletronico,
            MAX(situacao_especial) AS situacao_especial, MAX(data_situacao_especial) AS data_situacao_especial,
            mes_referencia,
            MAX(inserted_at) AS inserted_at,
            NOW() AS updated_at,
            MAX(change_hash) AS change_hash
        FROM limpeza_base
        GROUP BY pk, mes_referencia
    ),
    estado_ativo_anterior AS (
        SELECT 
            pk, cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia,
            situacao_cadastral, situacao_cadastral_desc, data_situacao_cadastral, motivo_situacao_cadastral,
            nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria,
            tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio,
            ddd_1, telefone_1, telefone_1_formatado, ddd_2, telefone_2, telefone_2_formatado,
            ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial,
            valid_since AS mes_referencia, inserted_at, updated_at, change_hash
        FROM tcc_receita.silver_intermediate.estabelecimentos
        WHERE is_current = TRUE
    ),
    uniao_historico AS (
        SELECT * FROM estado_ativo_anterior
        UNION ALL
        SELECT * FROM bronze_limpo
    ),
    deteccao_mudancas AS (
        SELECT *, LAG(change_hash) OVER (PARTITION BY pk ORDER BY mes_referencia) AS prev_hash
        FROM uniao_historico
    ),
    apenas_transicoes AS (
        SELECT * FROM deteccao_mudancas
        WHERE prev_hash IS NULL OR prev_hash != change_hash
    ),
    calculo_vigencia AS (
        SELECT 
            pk, cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia,
            situacao_cadastral, situacao_cadastral_desc, data_situacao_cadastral, motivo_situacao_cadastral,
            nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria,
            tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio,
            ddd_1, telefone_1, telefone_1_formatado, ddd_2, telefone_2, telefone_2_formatado,
            ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial,
            mes_referencia, inserted_at, updated_at, change_hash,
            mes_referencia AS valid_since,
            LEAD(mes_referencia) OVER (PARTITION BY pk ORDER BY mes_referencia) AS valid_until,
            (LEAD(mes_referencia) OVER (PARTITION BY pk ORDER BY mes_referencia) IS NULL) AS is_current
        FROM apenas_transicoes
    )
    SELECT c.*
    FROM calculo_vigencia c
    CROSS JOIN params p
    WHERE c.valid_since >= p.mes_ini
       OR c.valid_until IS NOT NULL
) AS staging
ON target.is_current = TRUE 
   AND target.pk = staging.pk 
   AND target.valid_since = staging.valid_since
WHEN MATCHED THEN UPDATE SET
    target.valid_until = staging.valid_until,
    target.is_current = FALSE,
    target.updated_at = NOW()
WHEN NOT MATCHED THEN INSERT (
    pk, cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia,
    situacao_cadastral, situacao_cadastral_desc, data_situacao_cadastral, motivo_situacao_cadastral,
    nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria,
    tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio,
    ddd_1, telefone_1, telefone_1_formatado, ddd_2, telefone_2, telefone_2_formatado,
    ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial,
    mes_referencia, inserted_at, updated_at, change_hash,
    valid_since, valid_until, is_current
) VALUES (
    staging.pk, staging.cnpj_basico, staging.cnpj_ordem, staging.cnpj_dv, staging.identificador_matriz_filial, staging.nome_fantasia,
    staging.situacao_cadastral, staging.situacao_cadastral_desc, staging.data_situacao_cadastral, staging.motivo_situacao_cadastral,
    staging.nome_cidade_exterior, staging.pais, staging.data_inicio_atividade, staging.cnae_fiscal_principal, staging.cnae_fiscal_secundaria,
    staging.tipo_logradouro, staging.logradouro, staging.numero, staging.complemento, staging.bairro, staging.cep, staging.uf, staging.municipio,
    staging.ddd_1, staging.telefone_1, staging.telefone_1_formatado, staging.ddd_2, staging.telefone_2, staging.telefone_2_formatado,
    staging.ddd_fax, staging.fax, staging.correio_eletronico, staging.situacao_especial, staging.data_situacao_especial,
    staging.mes_referencia, staging.inserted_at, staging.updated_at, staging.change_hash,
    staging.valid_since, staging.valid_until, staging.is_current
);