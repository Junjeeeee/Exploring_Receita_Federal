CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.dados_simples (
    pk STRING COMMENT 'Chave primária lógica (cnpj_basico).',
    cnpj_basico STRING COMMENT 'Radical do CNPJ (primeiros 8 dígitos).',
    opcao_pelo_simples STRING COMMENT 'Flag original de opção pelo Simples (S/N).',
    opcao_pelo_simples_bool BOOLEAN COMMENT 'Flag booleana enriquecida de opção pelo Simples.',
    data_opcao_simples STRING COMMENT 'Data de adesão ao Simples higienizada (AAAAMMDD).',
    data_exclusao_simples STRING COMMENT 'Data de exclusão do Simples higienizada (AAAAMMDD).',
    opcao_pelo_mei STRING COMMENT 'Flag original de opção pelo MEI (S/N).',
    opcao_pelo_mei_bool BOOLEAN COMMENT 'Flag booleana enriquecida de opção pelo MEI.',
    data_opcao_mei STRING COMMENT 'Data de adesão ao MEI higienizada (AAAAMMDD).',
    data_exclusao_mei STRING COMMENT 'Data de exclusão do MEI higienizada (AAAAMMDD).',
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
COMMENT 'Camada Silver Intermediate (SCD2): Histórico de enquadramento tributário no Simples Nacional e MEI.';

-- 1. Idempotência
DELETE FROM tcc_receita.silver_intermediate.dados_simples
WHERE valid_since >= COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'));

UPDATE tcc_receita.silver_intermediate.dados_simples
SET valid_until = NULL,
    is_current = TRUE,
    updated_at = NOW()
WHERE valid_since < COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))
  AND valid_until >= COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'));

-- 2. Merge SCD2 Set-Based
MERGE INTO tcc_receita.silver_intermediate.dados_simples AS target
USING (
    WITH params AS (
        SELECT 
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_fim
    ),
    bronze_limpo AS (
        SELECT 
            pk,
            MAX(NULLIF(TRIM(cnpj_basico), '')) AS cnpj_basico,
            MAX(NULLIF(TRIM(opcao_pelo_simples), '')) AS opcao_pelo_simples,
            MAX(CASE WHEN TRIM(opcao_pelo_simples) = 'S' THEN TRUE WHEN TRIM(opcao_pelo_simples) = 'N' THEN FALSE ELSE CAST(NULL AS BOOLEAN) END) AS opcao_pelo_simples_bool,
            MAX(CASE WHEN LENGTH(TRIM(data_opcao_simples)) < 8 OR TRIM(data_opcao_simples) = '00000000' THEN NULL ELSE TRIM(data_opcao_simples) END) AS data_opcao_simples,
            MAX(CASE WHEN LENGTH(TRIM(data_exclusao_simples)) < 8 OR TRIM(data_exclusao_simples) = '00000000' THEN NULL ELSE TRIM(data_exclusao_simples) END) AS data_exclusao_simples,
            MAX(NULLIF(TRIM(opcao_pelo_mei), '')) AS opcao_pelo_mei,
            MAX(CASE WHEN TRIM(opcao_pelo_mei) = 'S' THEN TRUE WHEN TRIM(opcao_pelo_mei) = 'N' THEN FALSE ELSE CAST(NULL AS BOOLEAN) END) AS opcao_pelo_mei_bool,
            MAX(CASE WHEN LENGTH(TRIM(data_opcao_mei)) < 8 OR TRIM(data_opcao_mei) = '00000000' THEN NULL ELSE TRIM(data_opcao_mei) END) AS data_opcao_mei,
            MAX(CASE WHEN LENGTH(TRIM(data_exclusao_mei)) < 8 OR TRIM(data_exclusao_mei) = '00000000' THEN NULL ELSE TRIM(data_exclusao_mei) END) AS data_exclusao_mei,
            mes_referencia,
            MAX(inserted_at) AS inserted_at,
            NOW() AS updated_at,
            MAX(change_hash) AS change_hash
        FROM tcc_receita.bronze.dados_simples b
        CROSS JOIN params p
        WHERE b.mes_referencia >= p.mes_ini
          AND b.mes_referencia <= p.mes_fim
          AND b.pk IS NOT NULL
        GROUP BY pk, mes_referencia
    ),
    estado_ativo_anterior AS (
        SELECT 
            pk, cnpj_basico, opcao_pelo_simples, opcao_pelo_simples_bool,
            data_opcao_simples, data_exclusao_simples, opcao_pelo_mei, opcao_pelo_mei_bool,
            data_opcao_mei, data_exclusao_mei,
            valid_since AS mes_referencia, inserted_at, updated_at, change_hash
        FROM tcc_receita.silver_intermediate.dados_simples
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
            pk, cnpj_basico, opcao_pelo_simples, opcao_pelo_simples_bool,
            data_opcao_simples, data_exclusao_simples, opcao_pelo_mei, opcao_pelo_mei_bool,
            data_opcao_mei, data_exclusao_mei,
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
    pk, cnpj_basico, opcao_pelo_simples, opcao_pelo_simples_bool,
    data_opcao_simples, data_exclusao_simples, opcao_pelo_mei, opcao_pelo_mei_bool,
    data_opcao_mei, data_exclusao_mei,
    mes_referencia, inserted_at, updated_at, change_hash,
    valid_since, valid_until, is_current
) VALUES (
    staging.pk, staging.cnpj_basico, staging.opcao_pelo_simples, staging.opcao_pelo_simples_bool,
    staging.data_opcao_simples, staging.data_exclusao_simples, staging.opcao_pelo_mei, staging.opcao_pelo_mei_bool,
    staging.data_opcao_mei, staging.data_exclusao_mei,
    staging.mes_referencia, staging.inserted_at, staging.updated_at, staging.change_hash,
    staging.valid_since, staging.valid_until, staging.is_current
);