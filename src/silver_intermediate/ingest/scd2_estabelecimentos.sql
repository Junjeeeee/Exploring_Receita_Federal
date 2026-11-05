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
    data_situacao_cadastral STRING COMMENT 'Data da situação cadastral higienizada e validada no calendário (AAAAMMDD).',
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
    data_situacao_especial STRING COMMENT 'Data da situação especial.',
    mes_referencia STRING COMMENT 'Mês de referência do snapshot de origem.',
    inserted_at TIMESTAMP COMMENT 'Timestamp de inserção original na Bronze.',
    updated_at TIMESTAMP COMMENT 'Timestamp da última atualização no SCD2.',
    change_hash STRING COMMENT 'Hash MD5 dos atributos de negócio.',
    valid_since STRING COMMENT 'Mês/Ano de início de vigência da versão (YYYY-MM). O intervalo é semiaberto [valid_since, valid_until).',
    valid_until STRING COMMENT 'Mês/Ano de fim de vigência da versão (NULL se versão atual). O intervalo é semiaberto [valid_since, valid_until).',
    is_current BOOLEAN COMMENT 'Flag indicando se o registo é a versão ativa atual.'
)
USING DELTA
COMMENT 'Camada Silver Intermediate (SCD2): Histórico de mutações cadastrais de matrizes e filiais (Estabelecimentos).';

-- Transação Única e Atômica: Recálculo do histórico preservando o futuro e apagando transições fantasmas
MERGE INTO tcc_receita.silver_intermediate.estabelecimentos AS target
USING (
    WITH params AS (
        SELECT 
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_fim
    ),
    -- 1. Identifica estritamente quais PKs sofreram alterações na janela solicitada
    pks_afetadas AS (
        SELECT DISTINCT b.pk 
        FROM tcc_receita.bronze.estabelecimentos b
        CROSS JOIN params p
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
    ),
    -- 2. Lê os dados crus da janela, deduplicando por registro mais recente (evita registros sintéticos)
    bronze_raw AS (
        SELECT b.*,
               ROW_NUMBER() OVER(PARTITION BY b.pk, b.mes_referencia ORDER BY b.bronze_processed_at DESC) as rn
        FROM tcc_receita.bronze.estabelecimentos b
        INNER JOIN pks_afetadas a ON b.pk = a.pk
        CROSS JOIN params p
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
    ),
    -- 3. Aplica regras de limpeza preservando caracteres Unicode (Acentos) e validando datas
    bronze_limpo AS (
        SELECT 
            pk,
            NULLIF(TRIM(cnpj_basico), '') AS cnpj_basico,
            NULLIF(TRIM(cnpj_ordem), '') AS cnpj_ordem,
            NULLIF(TRIM(cnpj_dv), '') AS cnpj_dv,
            NULLIF(TRIM(identificador_matriz_filial), '') AS identificador_matriz_filial,
            NULLIF(UPPER(TRIM(regexp_replace(nome_fantasia, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS nome_fantasia,
            NULLIF(TRIM(situacao_cadastral), '') AS situacao_cadastral,
            CASE 
                WHEN TRIM(situacao_cadastral) IN ('02', '2') THEN 'ATIVA'
                WHEN TRIM(situacao_cadastral) IN ('04', '4') THEN 'INAPTA'
                WHEN TRIM(situacao_cadastral) IN ('03', '3') THEN 'SUSPENSA'
                WHEN TRIM(situacao_cadastral) IN ('08', '8') THEN 'BAIXADA'
                WHEN TRIM(situacao_cadastral) IN ('01', '1') THEN 'NULA'
                ELSE NULL
            END AS situacao_cadastral_desc,
            CASE WHEN to_date(TRIM(data_situacao_cadastral), 'yyyyMMdd') IS NOT NULL THEN TRIM(data_situacao_cadastral) ELSE NULL END AS data_situacao_cadastral,
            NULLIF(TRIM(motivo_situacao_cadastral), '') AS motivo_situacao_cadastral,
            NULLIF(TRIM(nome_cidade_exterior), '') AS nome_cidade_exterior,
            NULLIF(TRIM(pais), '') AS pais,
            CASE WHEN to_date(TRIM(data_inicio_atividade), 'yyyyMMdd') IS NOT NULL THEN TRIM(data_inicio_atividade) ELSE NULL END AS data_inicio_atividade,
            NULLIF(TRIM(cnae_fiscal_principal), '') AS cnae_fiscal_principal,
            NULLIF(TRIM(cnae_fiscal_secundaria), '') AS cnae_fiscal_secundaria,
            NULLIF(TRIM(tipo_logradouro), '') AS tipo_logradouro,
            NULLIF(UPPER(TRIM(regexp_replace(logradouro, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS logradouro,
            NULLIF(TRIM(numero), '') AS numero,
            NULLIF(UPPER(TRIM(regexp_replace(complemento, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS complemento,
            NULLIF(UPPER(TRIM(regexp_replace(bairro, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS bairro,
            NULLIF(TRIM(cep), '') AS cep,
            NULLIF(TRIM(uf), '') AS uf,
            NULLIF(TRIM(municipio), '') AS municipio,
            NULLIF(TRIM(ddd_1), '') AS ddd_1,
            NULLIF(TRIM(telefone_1), '') AS telefone_1,
            CASE 
                WHEN NULLIF(TRIM(ddd_1), '') IS NOT NULL AND NULLIF(TRIM(telefone_1), '') IS NOT NULL AND TRIM(telefone_1) NOT LIKE '%00000000%'
                THEN CONCAT('(', TRIM(ddd_1), ') ', TRIM(telefone_1))
                ELSE NULL
            END AS telefone_1_formatado,
            NULLIF(TRIM(ddd_2), '') AS ddd_2,
            NULLIF(TRIM(telefone_2), '') AS telefone_2,
            CASE 
                WHEN NULLIF(TRIM(ddd_2), '') IS NOT NULL AND NULLIF(TRIM(telefone_2), '') IS NOT NULL AND TRIM(telefone_2) NOT LIKE '%00000000%'
                THEN CONCAT('(', TRIM(ddd_2), ') ', TRIM(telefone_2))
                ELSE NULL
            END AS telefone_2_formatado,
            NULLIF(TRIM(ddd_fax), '') AS ddd_fax,
            NULLIF(TRIM(fax), '') AS fax,
            NULLIF(TRIM(correio_eletronico), '') AS correio_eletronico,
            NULLIF(TRIM(situacao_especial), '') AS situacao_especial,
            CASE WHEN to_date(TRIM(data_situacao_especial), 'yyyyMMdd') IS NOT NULL THEN TRIM(data_situacao_especial) ELSE NULL END AS data_situacao_especial,
            mes_referencia,
            bronze_processed_at AS inserted_at,
            change_hash
        FROM bronze_raw
        WHERE rn = 1
    ),
    -- 4. Busca o passado e o futuro INTACTOS da tabela SCD2 atual para as PKs afetadas
    historico_intacto AS (
        SELECT 
            pk, cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia,
            situacao_cadastral, situacao_cadastral_desc, data_situacao_cadastral, motivo_situacao_cadastral,
            nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria,
            tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio,
            ddd_1, telefone_1, telefone_1_formatado, ddd_2, telefone_2, telefone_2_formatado,
            ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial,
            valid_since AS mes_referencia, inserted_at, change_hash
        FROM tcc_receita.silver_intermediate.estabelecimentos e
        INNER JOIN pks_afetadas a ON e.pk = a.pk
        CROSS JOIN params p
        WHERE e.valid_since < p.mes_ini OR e.valid_since > p.mes_fim
    ),
    uniao_historico AS (
        SELECT * FROM historico_intacto UNION ALL SELECT * FROM bronze_limpo
    ),
    -- 5. Detecta as transições da PK reconstruindo toda a linha do tempo (ignora falsas mudanças repetidas)
    deteccao_mudancas AS (
        SELECT *, LAG(change_hash) OVER (PARTITION BY pk ORDER BY mes_referencia) AS prev_hash
        FROM uniao_historico
    ),
    recalculo_vigencia AS (
        SELECT 
            pk, cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia,
            situacao_cadastral, situacao_cadastral_desc, data_situacao_cadastral, motivo_situacao_cadastral,
            nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria,
            tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio,
            ddd_1, telefone_1, telefone_1_formatado, ddd_2, telefone_2, telefone_2_formatado,
            ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial,
            inserted_at, change_hash,
            mes_referencia AS valid_since,
            LEAD(mes_referencia) OVER (PARTITION BY pk ORDER BY mes_referencia) AS valid_until,
            (LEAD(mes_referencia) OVER (PARTITION BY pk ORDER BY mes_referencia) IS NULL) AS is_current
        FROM deteccao_mudancas
        WHERE prev_hash IS NULL OR prev_hash != change_hash
    )
    -- 6. FULL OUTER JOIN garante que transições que antes existiam mas foram anuladas pelo recálculo sejam mapeadas como NULL (para posterior DELETE)
    SELECT t.pk AS join_pk, t.valid_since AS join_valid_since, s.*
    FROM recalculo_vigencia s
    FULL OUTER JOIN (
        SELECT e.pk, e.valid_since 
        FROM tcc_receita.silver_intermediate.estabelecimentos e
        INNER JOIN pks_afetadas a ON e.pk = a.pk
    ) t ON s.pk = t.pk AND s.valid_since = t.valid_since
) AS staging
ON target.pk = staging.join_pk AND target.valid_since = staging.join_valid_since
-- Executa o DELETE para registros fantasmas anulados na nova linha do tempo
WHEN MATCHED AND staging.pk IS NULL THEN DELETE
-- Atualiza limites temporais de registros pré e pós-janela
WHEN MATCHED AND (target.valid_until IS DISTINCT FROM staging.valid_until OR target.is_current != staging.is_current OR target.change_hash != staging.change_hash) THEN UPDATE SET
    target.valid_until = staging.valid_until,
    target.is_current = staging.is_current,
    target.updated_at = NOW(),
    target.situacao_cadastral_desc = staging.situacao_cadastral_desc,
    target.motivo_situacao_cadastral = staging.motivo_situacao_cadastral,
    target.cnae_fiscal_principal = staging.cnae_fiscal_principal,
    target.cnae_fiscal_secundaria = staging.cnae_fiscal_secundaria,
    target.nome_fantasia = staging.nome_fantasia,
    target.telefone_1_formatado = staging.telefone_1_formatado
WHEN NOT MATCHED THEN INSERT (
    pk, cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia,
    situacao_cadastral, situacao_cadastral_desc, data_situacao_cadastral, motivo_situacao_cadastral,
    nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria,
    tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio,
    ddd_1, telefone_1, telefone_1_formatado, ddd_2, telefone_2, telefone_2_formatado,
    ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial,
    mes_referencia, inserted_at, updated_at, change_hash, valid_since, valid_until, is_current
) VALUES (
    staging.pk, staging.cnpj_basico, staging.cnpj_ordem, staging.cnpj_dv, staging.identificador_matriz_filial, staging.nome_fantasia,
    staging.situacao_cadastral, staging.situacao_cadastral_desc, staging.data_situacao_cadastral, staging.motivo_situacao_cadastral,
    staging.nome_cidade_exterior, staging.pais, staging.data_inicio_atividade, staging.cnae_fiscal_principal, staging.cnae_fiscal_secundaria,
    staging.tipo_logradouro, staging.logradouro, staging.numero, staging.complemento, staging.bairro, staging.cep, staging.uf, staging.municipio,
    staging.ddd_1, staging.telefone_1, staging.telefone_1_formatado, staging.ddd_2, staging.telefone_2, staging.telefone_2_formatado,
    staging.ddd_fax, staging.fax, staging.correio_eletronico, staging.situacao_especial, staging.data_situacao_especial,
    staging.valid_since, staging.inserted_at, NOW(), staging.change_hash, staging.valid_since, staging.valid_until, staging.is_current
);