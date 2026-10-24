CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.empresas (
    pk STRING COMMENT 'Chave primária lógica (cnpj_basico).',
    cnpj_basico STRING COMMENT 'Radical do CNPJ (primeiros 8 dígitos).',
    razao_social STRING COMMENT 'Nome empresarial limpo e padronizado em maiúsculas.',
    natureza_juridica STRING COMMENT 'Código da natureza jurídica.',
    qualificacao_responsavel STRING COMMENT 'Código da qualificação do responsável.',
    capital_social DOUBLE COMMENT 'Valor do capital social em Reais (BRL).',
    porte_empresa STRING COMMENT 'Código do porte da empresa (01=ME, 03=EPP, 05=Demais).',
    ente_federativo_responsavel STRING COMMENT 'Ente federativo responsável, quando aplicável.',
    mes_referencia STRING COMMENT 'Mês de referência do snapshot que originou a versão.',
    inserted_at TIMESTAMP COMMENT 'Timestamp de inserção original na Bronze.',
    updated_at TIMESTAMP COMMENT 'Timestamp da última atualização no SCD2.',
    change_hash STRING COMMENT 'Hash MD5 dos atributos de negócio.',
    valid_since STRING COMMENT 'Mês/Ano de início de vigência da versão (YYYY-MM).',
    valid_until STRING COMMENT 'Mês/Ano de fim de vigência da versão (NULL se versão atual).',
    is_current BOOLEAN COMMENT 'Flag indicando se o registo é a versão ativa atual.'
)
USING DELTA
PARTITIONED BY (is_current)
COMMENT 'Camada Silver Intermediate (SCD2): Histórico de mutações cadastrais da raiz do CNPJ (Empresas).';

-- 1. Idempotência para reprocessamento do intervalo
DELETE FROM tcc_receita.silver_intermediate.empresas
WHERE valid_since >= COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'));

UPDATE tcc_receita.silver_intermediate.empresas
SET valid_until = NULL,
    is_current = TRUE,
    updated_at = NOW()
WHERE valid_since < COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))
  AND valid_until >= COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'));

-- 2. Merge SCD2 Set-Based (Processa 1 mês ou múltiplos meses numa única passada)
MERGE INTO tcc_receita.silver_intermediate.empresas AS target
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
            MAX(NULLIF(UPPER(TRIM(regexp_replace(razao_social, '[^a-zA-Z0-9 ]', ''))), '')) AS razao_social,
            MAX(NULLIF(TRIM(natureza_juridica), '')) AS natureza_juridica,
            MAX(NULLIF(TRIM(qualificacao_responsavel), '')) AS qualificacao_responsavel,
            MAX(capital_social) AS capital_social,
            MAX(NULLIF(TRIM(porte_empresa), '')) AS porte_empresa,
            MAX(NULLIF(TRIM(ente_federativo_responsavel), '')) AS ente_federativo_responsavel,
            mes_referencia,
            MAX(inserted_at) AS inserted_at,
            NOW() AS updated_at,
            MAX(change_hash) AS change_hash
        FROM tcc_receita.bronze.empresas b
        CROSS JOIN params p
        WHERE b.mes_referencia >= p.mes_ini
          AND b.mes_referencia <= p.mes_fim
          AND b.pk IS NOT NULL
        GROUP BY pk, mes_referencia
    ),
    estado_ativo_anterior AS (
        SELECT 
            pk, cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
            capital_social, porte_empresa, ente_federativo_responsavel,
            valid_since AS mes_referencia, inserted_at, updated_at, change_hash
        FROM tcc_receita.silver_intermediate.empresas
        WHERE is_current = TRUE
    ),
    uniao_historico AS (
        SELECT * FROM estado_ativo_anterior
        UNION ALL
        SELECT * FROM bronze_limpo
    ),
    deteccao_mudancas AS (
        SELECT 
            *,
            LAG(change_hash) OVER (PARTITION BY pk ORDER BY mes_referencia) AS prev_hash
        FROM uniao_historico
    ),
    apenas_transicoes AS (
        SELECT *
        FROM deteccao_mudancas
        WHERE prev_hash IS NULL OR prev_hash != change_hash
    ),
    calculo_vigencia AS (
        SELECT 
            pk, cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
            capital_social, porte_empresa, ente_federativo_responsavel,
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
    pk, cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
    capital_social, porte_empresa, ente_federativo_responsavel,
    mes_referencia, inserted_at, updated_at, change_hash,
    valid_since, valid_until, is_current
) VALUES (
    staging.pk, staging.cnpj_basico, staging.razao_social, staging.natureza_juridica, staging.qualificacao_responsavel,
    staging.capital_social, staging.porte_empresa, staging.ente_federativo_responsavel,
    staging.mes_referencia, staging.inserted_at, staging.updated_at, staging.change_hash,
    staging.valid_since, staging.valid_until, staging.is_current
);