CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.empresas (
    pk STRING COMMENT 'Chave primária lógica (cnpj_basico).',
    cnpj_basico STRING COMMENT 'Radical do CNPJ (primeiros 8 dígitos).',
    razao_social STRING COMMENT 'Nome empresarial limpo e padronizado em maiúsculas (acentos preservados).',
    natureza_juridica STRING COMMENT 'Código da natureza jurídica.',
    qualificacao_responsavel STRING COMMENT 'Código da qualificação do responsável.',
    capital_social DOUBLE COMMENT 'Valor do capital social em Reais (BRL).',
    porte_empresa STRING COMMENT 'Código do porte da empresa (01=ME, 03=EPP, 05=Demais).',
    ente_federativo_responsavel STRING COMMENT 'Ente federativo responsável, quando aplicável.',
    mes_referencia STRING COMMENT 'Mês de referência do snapshot que originou a versão.',
    inserted_at TIMESTAMP COMMENT 'Timestamp de inserção original na Bronze.',
    updated_at TIMESTAMP COMMENT 'Timestamp da última atualização no SCD2.',
    change_hash STRING COMMENT 'Hash MD5 dos atributos de negócio.',
    valid_since STRING COMMENT 'Mês/Ano de início de vigência da versão (YYYY-MM). O intervalo é semiaberto [valid_since, valid_until).',
    valid_until STRING COMMENT 'Mês/Ano de fim de vigência da versão (NULL se versão atual).',
    is_current BOOLEAN COMMENT 'Flag indicando se o registo é a versão ativa atual.'
)
USING DELTA
COMMENT 'Camada Silver Intermediate (SCD2): Histórico de mutações cadastrais da raiz do CNPJ (Empresas).';

-- Transação Única e Atômica: Recálculo do histórico preservando o futuro e apagando transições fantasmas
MERGE INTO tcc_receita.silver_intermediate.empresas AS target
USING (
    WITH params AS (
        SELECT 
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_fim
    ),
    -- 1. Identifica estritamente quais PKs sofreram alterações na janela solicitada
    pks_afetadas AS (
        SELECT DISTINCT b.pk 
        FROM tcc_receita.bronze.empresas b
        CROSS JOIN params p
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
    ),
    -- 2. Lê os dados crus da janela, deduplicando por registro mais recente
    bronze_raw AS (
        SELECT b.*,
               ROW_NUMBER() OVER(PARTITION BY b.pk, b.mes_referencia ORDER BY b.bronze_processed_at DESC) as rn
        FROM tcc_receita.bronze.empresas b
        INNER JOIN pks_afetadas a ON b.pk = a.pk
        CROSS JOIN params p
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
    ),
    -- 3. Aplica regras de limpeza preservando caracteres Unicode (Acentos)
    bronze_limpo AS (
        SELECT 
            pk,
            NULLIF(TRIM(cnpj_basico), '') AS cnpj_basico,
            NULLIF(UPPER(TRIM(regexp_replace(razao_social, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS razao_social,
            NULLIF(TRIM(natureza_juridica), '') AS natureza_juridica,
            NULLIF(TRIM(qualificacao_responsavel), '') AS qualificacao_responsavel,
            capital_social,
            NULLIF(TRIM(porte_empresa), '') AS porte_empresa,
            NULLIF(TRIM(ente_federativo_responsavel), '') AS ente_federativo_responsavel,
            mes_referencia,
            bronze_processed_at AS inserted_at,
            change_hash
        FROM bronze_raw
        WHERE rn = 1
    ),
    -- 4. Busca o passado e o futuro INTACTOS da tabela SCD2 atual para as PKs afetadas
    historico_intacto AS (
        SELECT 
            pk, cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
            capital_social, porte_empresa, ente_federativo_responsavel,
            valid_since AS mes_referencia, inserted_at, change_hash
        FROM tcc_receita.silver_intermediate.empresas e
        INNER JOIN pks_afetadas a ON e.pk = a.pk
        CROSS JOIN params p
        WHERE e.valid_since < p.mes_ini OR e.valid_since > p.mes_fim
    ),
    uniao_historico AS (
        SELECT * FROM historico_intacto UNION ALL SELECT * FROM bronze_limpo
    ),
    -- 5. Detecta as transições da PK reconstruindo toda a linha do tempo
    deteccao_mudancas AS (
        SELECT *, LAG(change_hash) OVER (PARTITION BY pk ORDER BY mes_referencia) AS prev_hash
        FROM uniao_historico
    ),
    recalculo_vigencia AS (
        SELECT 
            pk, cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
            capital_social, porte_empresa, ente_federativo_responsavel,
            inserted_at, change_hash,
            mes_referencia AS valid_since,
            LEAD(mes_referencia) OVER (PARTITION BY pk ORDER BY mes_referencia) AS valid_until,
            (LEAD(mes_referencia) OVER (PARTITION BY pk ORDER BY mes_referencia) IS NULL) AS is_current
        FROM deteccao_mudancas
        WHERE prev_hash IS NULL OR prev_hash != change_hash
    )
    -- 6. FULL OUTER JOIN para sincronizar as mutações com o alvo
    SELECT t.pk AS join_pk, t.valid_since AS join_valid_since, s.*
    FROM recalculo_vigencia s
    FULL OUTER JOIN (
        SELECT e.pk, e.valid_since 
        FROM tcc_receita.silver_intermediate.empresas e
        INNER JOIN pks_afetadas a ON e.pk = a.pk
    ) t ON s.pk = t.pk AND s.valid_since = t.valid_since
) AS staging
ON target.pk = staging.join_pk AND target.valid_since = staging.join_valid_since
WHEN MATCHED AND staging.pk IS NULL THEN DELETE
WHEN MATCHED AND (target.valid_until IS DISTINCT FROM staging.valid_until OR target.is_current != staging.is_current OR target.change_hash != staging.change_hash) THEN UPDATE SET
    target.valid_until = staging.valid_until,
    target.is_current = staging.is_current,
    target.updated_at = NOW(),
    target.razao_social = staging.razao_social,
    target.natureza_juridica = staging.natureza_juridica,
    target.qualificacao_responsavel = staging.qualificacao_responsavel,
    target.capital_social = staging.capital_social,
    target.porte_empresa = staging.porte_empresa,
    target.ente_federativo_responsavel = staging.ente_federativo_responsavel
WHEN NOT MATCHED THEN INSERT (
    pk, cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
    capital_social, porte_empresa, ente_federativo_responsavel,
    mes_referencia, inserted_at, updated_at, change_hash, valid_since, valid_until, is_current
) VALUES (
    staging.pk, staging.cnpj_basico, staging.razao_social, staging.natureza_juridica, staging.qualificacao_responsavel,
    staging.capital_social, staging.porte_empresa, staging.ente_federativo_responsavel,
    staging.valid_since, staging.inserted_at, NOW(), staging.change_hash, staging.valid_since, staging.valid_until, staging.is_current
);