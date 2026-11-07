-- Silver Intermediate: histórico SCD2 de empresas.
-- Parâmetros vazios assumem o mês anterior. Reprocessa apenas PKs presentes na janela,
-- preservando versões anteriores e posteriores e removendo versões que deixaram de ser transições.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.empresas (
    pk STRING COMMENT 'Chave primária lógica da empresa, gerada na Bronze.',
    cnpj_basico STRING COMMENT 'Radical do CNPJ (primeiros 8 dígitos).',
    razao_social STRING COMMENT 'Nome empresarial limpo e padronizado em maiúsculas, com caracteres Unicode preservados.',
    natureza_juridica STRING COMMENT 'Código da natureza jurídica.',
    qualificacao_responsavel STRING COMMENT 'Código da qualificação do responsável.',
    capital_social DOUBLE COMMENT 'Valor do capital social em Reais (BRL).',
    porte_empresa STRING COMMENT 'Código do porte da empresa (01=ME, 03=EPP, 05=Demais).',
    ente_federativo_responsavel STRING COMMENT 'Ente federativo responsável, quando aplicável.',
    mes_referencia STRING COMMENT 'Mês de referência do snapshot que originou a versão.',
    inserted_at TIMESTAMP COMMENT 'Timestamp do processamento Bronze que originou a versão.',
    updated_at TIMESTAMP COMMENT 'Timestamp da última atualização da versão no SCD2.',
    change_hash STRING COMMENT 'Hash SHA-256 herdado da Bronze, calculado sobre os atributos de negócio de origem.',
    valid_since STRING COMMENT 'Início da vigência da versão (YYYY-MM), inclusivo.',
    valid_until STRING COMMENT 'Fim da vigência da versão (YYYY-MM), exclusivo; NULL para versão atual.',
    is_current BOOLEAN COMMENT 'Indica se esta é a versão vigente.'
)
USING DELTA
COMMENT 'Camada Silver Intermediate (SCD2): Histórico de mutações cadastrais da raiz do CNPJ (Empresas).';

MERGE INTO tcc_receita.silver_intermediate.empresas AS target
USING (
    WITH params AS (
        SELECT
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
    ),
    pks_afetadas AS (
        SELECT DISTINCT b.pk
        FROM tcc_receita.bronze.empresas b
        CROSS JOIN params p
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
          AND b.pk IS NOT NULL
    ),
    bronze_raw AS (
        SELECT b.*,
               ROW_NUMBER() OVER (
                   PARTITION BY b.pk, b.mes_referencia
                   ORDER BY b.bronze_processed_at DESC NULLS LAST
               ) AS rn
        FROM tcc_receita.bronze.empresas b
        INNER JOIN pks_afetadas a ON b.pk = a.pk
        CROSS JOIN params p
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
    ),
    bronze_limpo AS (
        SELECT
            pk,
            NULLIF(TRIM(cnpj_basico), '') AS cnpj_basico,
            NULLIF(UPPER(TRIM(regexp_replace(razao_social, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS razao_social,
            NULLIF(TRIM(natureza_juridica), '') AS natureza_juridica,
            NULLIF(TRIM(qualificacao_responsavel), '') AS qualificacao_responsavel,
            TRY_CAST(REPLACE(NULLIF(TRIM(CAST(capital_social AS STRING)), ''), ',', '.') AS DOUBLE) AS capital_social,
            NULLIF(TRIM(porte_empresa), '') AS porte_empresa,
            NULLIF(TRIM(ente_federativo_responsavel), '') AS ente_federativo_responsavel,
            mes_referencia,
            bronze_processed_at AS inserted_at,
            change_hash
        FROM bronze_raw
        WHERE rn = 1
    ),
    historico_intacto AS (
        SELECT
            e.pk, e.cnpj_basico, e.razao_social, e.natureza_juridica,
            e.qualificacao_responsavel, e.capital_social, e.porte_empresa,
            e.ente_federativo_responsavel,
            e.valid_since AS mes_referencia,
            e.inserted_at,
            e.change_hash
        FROM tcc_receita.silver_intermediate.empresas e
        INNER JOIN pks_afetadas a ON e.pk = a.pk
        CROSS JOIN params p
        WHERE e.valid_since < p.mes_ini OR e.valid_since > p.mes_fim
    ),
    uniao_historico AS (
        SELECT * FROM historico_intacto
        UNION ALL
        SELECT * FROM bronze_limpo
    ),
    deteccao_mudancas AS (
        SELECT *,
               LAG(change_hash) OVER (PARTITION BY pk ORDER BY mes_referencia) AS prev_hash
        FROM uniao_historico
    ),
    recalculo_vigencia AS (
        SELECT
            pk, cnpj_basico, razao_social, natureza_juridica,
            qualificacao_responsavel, capital_social, porte_empresa,
            ente_federativo_responsavel,
            mes_referencia, inserted_at, change_hash,
            mes_referencia AS valid_since,
            LEAD(mes_referencia) OVER (PARTITION BY pk ORDER BY mes_referencia) AS valid_until,
            LEAD(mes_referencia) OVER (PARTITION BY pk ORDER BY mes_referencia) IS NULL AS is_current
        FROM deteccao_mudancas
        WHERE prev_hash IS NULL OR prev_hash IS DISTINCT FROM change_hash
    )
    SELECT
        t.pk AS join_pk,
        t.valid_since AS join_valid_since,
        s.*
    FROM recalculo_vigencia s
    FULL OUTER JOIN (
        SELECT e.pk, e.valid_since
        FROM tcc_receita.silver_intermediate.empresas e
        INNER JOIN pks_afetadas a ON e.pk = a.pk
    ) t
      ON s.pk = t.pk AND s.valid_since = t.valid_since
) AS staging
ON target.pk = staging.join_pk
AND target.valid_since = staging.join_valid_since
WHEN MATCHED AND staging.pk IS NULL THEN DELETE
WHEN MATCHED AND (
       target.valid_until IS DISTINCT FROM staging.valid_until
    OR target.is_current IS DISTINCT FROM staging.is_current
    OR target.change_hash IS DISTINCT FROM staging.change_hash
) THEN UPDATE SET
    target.cnpj_basico = staging.cnpj_basico,
    target.razao_social = staging.razao_social,
    target.natureza_juridica = staging.natureza_juridica,
    target.qualificacao_responsavel = staging.qualificacao_responsavel,
    target.capital_social = staging.capital_social,
    target.porte_empresa = staging.porte_empresa,
    target.ente_federativo_responsavel = staging.ente_federativo_responsavel,
    target.mes_referencia = staging.mes_referencia,
    target.change_hash = staging.change_hash,
    target.valid_until = staging.valid_until,
    target.is_current = staging.is_current,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    pk, cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
    capital_social, porte_empresa, ente_federativo_responsavel,
    mes_referencia, inserted_at, updated_at, change_hash,
    valid_since, valid_until, is_current
) VALUES (
    staging.pk, staging.cnpj_basico, staging.razao_social, staging.natureza_juridica,
    staging.qualificacao_responsavel, staging.capital_social, staging.porte_empresa,
    staging.ente_federativo_responsavel,
    staging.mes_referencia, staging.inserted_at, current_timestamp(), staging.change_hash,
    staging.valid_since, staging.valid_until, staging.is_current
);
