CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.socios (
    pk STRING COMMENT 'Chave primária lógica composta do sócio na sociedade.',
    cnpj_basico STRING COMMENT 'Radical do CNPJ (primeiros 8 dígitos).',
    identificador_socio STRING COMMENT '1=PJ, 2=PF, 3=Estrangeiro.',
    nome_socio_razao_social STRING COMMENT 'Nome do sócio limpo e padronizado.',
    cnpj_cpf_socio STRING COMMENT 'CPF ou CNPJ do sócio.',
    qualificacao_socio STRING COMMENT 'Código da qualificação do sócio.',
    data_entrada_sociedade STRING COMMENT 'Data de entrada na sociedade higienizada (AAAAMMDD).',
    pais STRING COMMENT 'Código do país do sócio estrangeiro.',
    representante_legal STRING COMMENT 'CPF do representante legal.',
    nome_representante STRING COMMENT 'Nome do representante legal.',
    qualificacao_representante_legal STRING COMMENT 'Código da qualificação do representante legal.',
    faixa_etaria STRING COMMENT 'Código da faixa etária do sócio.',
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
COMMENT 'Camada Silver Intermediate (SCD2): Histórico de mutações do Quadro de Sócios e Administradores (QSA).';

-- 1. Idempotência
DELETE FROM tcc_receita.silver_intermediate.socios
WHERE valid_since >= COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'));

UPDATE tcc_receita.silver_intermediate.socios
SET valid_until = NULL,
    is_current = TRUE,
    updated_at = NOW()
WHERE valid_since < COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))
  AND valid_until >= COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'));

-- 2. Merge SCD2 Set-Based
MERGE INTO tcc_receita.silver_intermediate.socios AS target
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
            MAX(NULLIF(TRIM(identificador_socio), '')) AS identificador_socio,
            MAX(NULLIF(UPPER(TRIM(regexp_replace(nome_socio_razao_social, '[^a-zA-Z0-9 ]', ''))), '')) AS nome_socio_razao_social,
            MAX(NULLIF(TRIM(cnpj_cpf_socio), '')) AS cnpj_cpf_socio,
            MAX(NULLIF(TRIM(qualificacao_socio), '')) AS qualificacao_socio,
            MAX(CASE WHEN LENGTH(TRIM(data_entrada_sociedade)) < 8 OR TRIM(data_entrada_sociedade) = '00000000' THEN NULL ELSE TRIM(data_entrada_sociedade) END) AS data_entrada_sociedade,
            MAX(NULLIF(TRIM(pais), '')) AS pais,
            MAX(NULLIF(TRIM(representante_legal), '')) AS representante_legal,
            MAX(NULLIF(TRIM(nome_representante), '')) AS nome_representante,
            MAX(NULLIF(TRIM(qualificacao_representante_legal), '')) AS qualificacao_representante_legal,
            MAX(NULLIF(TRIM(faixa_etaria), '')) AS faixa_etaria,
            mes_referencia,
            MAX(inserted_at) AS inserted_at,
            NOW() AS updated_at,
            MAX(change_hash) AS change_hash
        FROM tcc_receita.bronze.socios b
        CROSS JOIN params p
        WHERE b.mes_referencia >= p.mes_ini
          AND b.mes_referencia <= p.mes_fim
          AND b.pk IS NOT NULL
        GROUP BY pk, mes_referencia
    ),
    estado_ativo_anterior AS (
        SELECT 
            pk, cnpj_basico, identificador_socio, nome_socio_razao_social, cnpj_cpf_socio,
            qualificacao_socio, data_entrada_sociedade, pais, representante_legal,
            nome_representante, qualificacao_representante_legal, faixa_etaria,
            valid_since AS mes_referencia, inserted_at, updated_at, change_hash
        FROM tcc_receita.silver_intermediate.socios
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
            pk, cnpj_basico, identificador_socio, nome_socio_razao_social, cnpj_cpf_socio,
            qualificacao_socio, data_entrada_sociedade, pais, representante_legal,
            nome_representante, qualificacao_representante_legal, faixa_etaria,
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
    pk, cnpj_basico, identificador_socio, nome_socio_razao_social, cnpj_cpf_socio,
    qualificacao_socio, data_entrada_sociedade, pais, representante_legal,
    nome_representante, qualificacao_representante_legal, faixa_etaria,
    mes_referencia, inserted_at, updated_at, change_hash,
    valid_since, valid_until, is_current
) VALUES (
    staging.pk, staging.cnpj_basico, staging.identificador_socio, staging.nome_socio_razao_social, staging.cnpj_cpf_socio,
    staging.qualificacao_socio, staging.data_entrada_sociedade, staging.pais, staging.representante_legal,
    staging.nome_representante, staging.qualificacao_representante_legal, staging.faixa_etaria,
    staging.mes_referencia, staging.inserted_at, staging.updated_at, staging.change_hash,
    staging.valid_since, staging.valid_until, staging.is_current
);