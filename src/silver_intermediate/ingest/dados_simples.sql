-- Silver Intermediate: histórico SCD2 de enquadramento no Simples Nacional e MEI.
-- Parâmetros vazios assumem o mês anterior. Reconstrói a linha do tempo das PKs afetadas.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.dados_simples (
    pk STRING COMMENT 'Chave primária lógica da raiz do CNPJ.',
    cnpj_basico STRING COMMENT 'Radical do CNPJ (primeiros 8 dígitos).',
    opcao_pelo_simples STRING COMMENT 'Flag original de opção pelo Simples (S/N).',
    opcao_pelo_simples_bool BOOLEAN COMMENT 'Flag booleana de opção pelo Simples; NULL quando a fonte não permite determinar.',
    data_opcao_simples STRING COMMENT 'Data de adesão ao Simples validada no calendário (AAAAMMDD).',
    data_exclusao_simples STRING COMMENT 'Data de exclusão do Simples validada no calendário (AAAAMMDD).',
    opcao_pelo_mei STRING COMMENT 'Flag original de opção pelo MEI (S/N).',
    opcao_pelo_mei_bool BOOLEAN COMMENT 'Flag booleana de opção pelo MEI; NULL quando a fonte não permite determinar.',
    data_opcao_mei STRING COMMENT 'Data de adesão ao MEI validada no calendário (AAAAMMDD).',
    data_exclusao_mei STRING COMMENT 'Data de exclusão do MEI validada no calendário (AAAAMMDD).',
    mes_referencia STRING COMMENT 'Mês de referência do snapshot de origem.',
    inserted_at TIMESTAMP COMMENT 'Timestamp do processamento Bronze que originou a versão.',
    updated_at TIMESTAMP COMMENT 'Timestamp da última atualização da versão no SCD2.',
    change_hash STRING COMMENT 'Hash SHA-256 herdado da Bronze, calculado sobre os atributos de negócio de origem.',
    valid_since STRING COMMENT 'Início da vigência da versão (YYYY-MM), inclusivo.',
    valid_until STRING COMMENT 'Fim da vigência da versão (YYYY-MM), exclusivo; NULL para versão atual.',
    is_current BOOLEAN COMMENT 'Indica se esta é a versão vigente.'
)
USING DELTA
COMMENT 'Camada Silver Intermediate (SCD2): Histórico de enquadramento tributário no Simples Nacional e MEI.';

MERGE INTO tcc_receita.silver_intermediate.dados_simples AS target
USING (
    WITH params AS (
        SELECT
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
    ),
    pks_afetadas AS (
        SELECT DISTINCT b.pk
        FROM tcc_receita.bronze.dados_simples b
        CROSS JOIN params p
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
          AND b.pk IS NOT NULL
    ),
    bronze_raw AS (
        SELECT b.*,
               ROW_NUMBER() OVER (
                   PARTITION BY b.pk, b.mes_referencia
                   ORDER BY b.bronze_processed_at DESC NULLS LAST, b.change_hash DESC  -- PK repetida na Receita: desempate determinístico (B16)
               ) AS rn
        FROM tcc_receita.bronze.dados_simples b
        INNER JOIN pks_afetadas a ON b.pk = a.pk
        CROSS JOIN params p
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
    ),
    bronze_limpo AS (
        SELECT
            pk,
            NULLIF(TRIM(cnpj_basico), '') AS cnpj_basico,
            NULLIF(TRIM(opcao_pelo_simples), '') AS opcao_pelo_simples,
            CASE
                WHEN UPPER(TRIM(opcao_pelo_simples)) = 'S' THEN TRUE
                WHEN UPPER(TRIM(opcao_pelo_simples)) = 'N' THEN FALSE
                ELSE CAST(NULL AS BOOLEAN)
            END AS opcao_pelo_simples_bool,
            CASE WHEN CAST(try_to_timestamp(TRIM(data_opcao_simples), 'yyyyMMdd') AS DATE) IS NOT NULL THEN TRIM(data_opcao_simples) ELSE NULL END AS data_opcao_simples,
            CASE WHEN CAST(try_to_timestamp(TRIM(data_exclusao_simples), 'yyyyMMdd') AS DATE) IS NOT NULL THEN TRIM(data_exclusao_simples) ELSE NULL END AS data_exclusao_simples,
            NULLIF(TRIM(opcao_pelo_mei), '') AS opcao_pelo_mei,
            CASE
                WHEN UPPER(TRIM(opcao_pelo_mei)) = 'S' THEN TRUE
                WHEN UPPER(TRIM(opcao_pelo_mei)) = 'N' THEN FALSE
                ELSE CAST(NULL AS BOOLEAN)
            END AS opcao_pelo_mei_bool,
            CASE WHEN CAST(try_to_timestamp(TRIM(data_opcao_mei), 'yyyyMMdd') AS DATE) IS NOT NULL THEN TRIM(data_opcao_mei) ELSE NULL END AS data_opcao_mei,
            CASE WHEN CAST(try_to_timestamp(TRIM(data_exclusao_mei), 'yyyyMMdd') AS DATE) IS NOT NULL THEN TRIM(data_exclusao_mei) ELSE NULL END AS data_exclusao_mei,
            mes_referencia,
            bronze_processed_at AS inserted_at,
            change_hash
        FROM bronze_raw
        WHERE rn = 1
    ),
    historico_intacto AS (
        SELECT
            e.pk, e.cnpj_basico, e.opcao_pelo_simples, e.opcao_pelo_simples_bool,
            e.data_opcao_simples, e.data_exclusao_simples,
            e.opcao_pelo_mei, e.opcao_pelo_mei_bool,
            e.data_opcao_mei, e.data_exclusao_mei,
            e.valid_since AS mes_referencia, e.inserted_at, e.change_hash
        FROM tcc_receita.silver_intermediate.dados_simples e
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
            pk, cnpj_basico, opcao_pelo_simples, opcao_pelo_simples_bool,
            data_opcao_simples, data_exclusao_simples,
            opcao_pelo_mei, opcao_pelo_mei_bool,
            data_opcao_mei, data_exclusao_mei,
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
        FROM tcc_receita.silver_intermediate.dados_simples e
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
    target.opcao_pelo_simples = staging.opcao_pelo_simples,
    target.opcao_pelo_simples_bool = staging.opcao_pelo_simples_bool,
    target.data_opcao_simples = staging.data_opcao_simples,
    target.data_exclusao_simples = staging.data_exclusao_simples,
    target.opcao_pelo_mei = staging.opcao_pelo_mei,
    target.opcao_pelo_mei_bool = staging.opcao_pelo_mei_bool,
    target.data_opcao_mei = staging.data_opcao_mei,
    target.data_exclusao_mei = staging.data_exclusao_mei,
    target.mes_referencia = staging.mes_referencia,
    target.change_hash = staging.change_hash,
    target.valid_until = staging.valid_until,
    target.is_current = staging.is_current,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    pk, cnpj_basico, opcao_pelo_simples, opcao_pelo_simples_bool,
    data_opcao_simples, data_exclusao_simples,
    opcao_pelo_mei, opcao_pelo_mei_bool,
    data_opcao_mei, data_exclusao_mei,
    mes_referencia, inserted_at, updated_at, change_hash,
    valid_since, valid_until, is_current
) VALUES (
    staging.pk, staging.cnpj_basico, staging.opcao_pelo_simples, staging.opcao_pelo_simples_bool,
    staging.data_opcao_simples, staging.data_exclusao_simples,
    staging.opcao_pelo_mei, staging.opcao_pelo_mei_bool,
    staging.data_opcao_mei, staging.data_exclusao_mei,
    staging.mes_referencia, staging.inserted_at, current_timestamp(), staging.change_hash,
    staging.valid_since, staging.valid_until, staging.is_current
);
