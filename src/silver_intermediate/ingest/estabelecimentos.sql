-- Silver Intermediate: histórico SCD2 de estabelecimentos.
-- Parâmetros vazios assumem o mês anterior. A reconstrução preserva passado/futuro.
-- Diferentemente da versão anterior, uma correção no hash atualiza TODOS os atributos de negócio.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.estabelecimentos (
    pk STRING COMMENT 'Chave primária lógica do estabelecimento (gerada na Bronze).',
    cnpj_basico STRING COMMENT 'Radical do CNPJ (primeiros 8 dígitos).',
    cnpj_ordem STRING COMMENT 'Número de ordem do estabelecimento.',
    cnpj_dv STRING COMMENT 'Dígitos verificadores do CNPJ.',
    identificador_matriz_filial STRING COMMENT '1 para Matriz, 2 para Filial.',
    nome_fantasia STRING COMMENT 'Nome comercial limpo e padronizado.',
    situacao_cadastral STRING COMMENT 'Código bruto da situação cadastral.',
    situacao_cadastral_desc STRING COMMENT 'Descrição decodificada da situação cadastral.',
    data_situacao_cadastral STRING COMMENT 'Data da situação cadastral validada (AAAAMMDD).',
    motivo_situacao_cadastral STRING COMMENT 'Código do motivo da situação cadastral.',
    nome_cidade_exterior STRING COMMENT 'Nome da cidade no exterior, se aplicável.',
    pais STRING COMMENT 'Código do país.',
    data_inicio_atividade STRING COMMENT 'Data de início das atividades validada (AAAAMMDD).',
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
    telefone_1_formatado STRING COMMENT 'Telefone primário padronizado.',
    ddd_2 STRING COMMENT 'DDD do telefone 2.',
    telefone_2 STRING COMMENT 'Número do telefone 2.',
    telefone_2_formatado STRING COMMENT 'Telefone secundário padronizado.',
    ddd_fax STRING COMMENT 'DDD do fax.',
    fax STRING COMMENT 'Número do fax.',
    correio_eletronico STRING COMMENT 'E-mail institucional.',
    situacao_especial STRING COMMENT 'Situação especial, quando aplicável.',
    data_situacao_especial STRING COMMENT 'Data da situação especial validada (AAAAMMDD).',
    mes_referencia STRING COMMENT 'Mês de referência do snapshot que originou a versão.',
    inserted_at TIMESTAMP COMMENT 'Timestamp do processamento Bronze que originou a versão.',
    updated_at TIMESTAMP COMMENT 'Timestamp da última atualização da versão no SCD2.',
    change_hash STRING COMMENT 'Hash SHA-256 herdado da Bronze, calculado sobre os atributos de negócio de origem.',
    valid_since STRING COMMENT 'Início da vigência da versão (YYYY-MM), inclusivo.',
    valid_until STRING COMMENT 'Fim da vigência da versão (YYYY-MM), exclusivo; NULL para versão atual.',
    is_current BOOLEAN COMMENT 'Indica se esta é a versão vigente.'
)
USING DELTA
COMMENT 'Camada Silver Intermediate (SCD2): Histórico de mutações cadastrais de matrizes e filiais (Estabelecimentos).';

MERGE INTO tcc_receita.silver_intermediate.estabelecimentos AS target
USING (
    WITH params AS (
        SELECT
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
    ),
    pks_afetadas AS (
        SELECT DISTINCT b.pk
        FROM tcc_receita.bronze.estabelecimentos b
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
        FROM tcc_receita.bronze.estabelecimentos b
        INNER JOIN pks_afetadas a ON b.pk = a.pk
        CROSS JOIN params p
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
    ),
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
            CASE WHEN CAST(try_to_timestamp(TRIM(data_situacao_cadastral), 'yyyyMMdd') AS DATE) IS NOT NULL THEN TRIM(data_situacao_cadastral) ELSE NULL END AS data_situacao_cadastral,
            NULLIF(TRIM(motivo_situacao_cadastral), '') AS motivo_situacao_cadastral,
            NULLIF(TRIM(nome_cidade_exterior), '') AS nome_cidade_exterior,
            NULLIF(TRIM(pais), '') AS pais,
            CASE WHEN CAST(try_to_timestamp(TRIM(data_inicio_atividade), 'yyyyMMdd') AS DATE) IS NOT NULL THEN TRIM(data_inicio_atividade) ELSE NULL END AS data_inicio_atividade,
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
                WHEN NULLIF(TRIM(ddd_1), '') IS NOT NULL
                 AND NULLIF(TRIM(telefone_1), '') IS NOT NULL
                 AND regexp_replace(TRIM(telefone_1), '[^0-9]', '') NOT RLIKE '^0+$'
                THEN CONCAT('(', TRIM(ddd_1), ') ', TRIM(telefone_1))
                ELSE NULL
            END AS telefone_1_formatado,
            NULLIF(TRIM(ddd_2), '') AS ddd_2,
            NULLIF(TRIM(telefone_2), '') AS telefone_2,
            CASE
                WHEN NULLIF(TRIM(ddd_2), '') IS NOT NULL
                 AND NULLIF(TRIM(telefone_2), '') IS NOT NULL
                 AND regexp_replace(TRIM(telefone_2), '[^0-9]', '') NOT RLIKE '^0+$'
                THEN CONCAT('(', TRIM(ddd_2), ') ', TRIM(telefone_2))
                ELSE NULL
            END AS telefone_2_formatado,
            NULLIF(TRIM(ddd_fax), '') AS ddd_fax,
            NULLIF(TRIM(fax), '') AS fax,
            NULLIF(TRIM(correio_eletronico), '') AS correio_eletronico,
            NULLIF(TRIM(situacao_especial), '') AS situacao_especial,
            CASE WHEN CAST(try_to_timestamp(TRIM(data_situacao_especial), 'yyyyMMdd') AS DATE) IS NOT NULL THEN TRIM(data_situacao_especial) ELSE NULL END AS data_situacao_especial,
            mes_referencia,
            bronze_processed_at AS inserted_at,
            change_hash
        FROM bronze_raw
        WHERE rn = 1
    ),
    historico_intacto AS (
        SELECT
            e.pk, e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv, e.identificador_matriz_filial,
            e.nome_fantasia, e.situacao_cadastral, e.situacao_cadastral_desc,
            e.data_situacao_cadastral, e.motivo_situacao_cadastral,
            e.nome_cidade_exterior, e.pais, e.data_inicio_atividade,
            e.cnae_fiscal_principal, e.cnae_fiscal_secundaria,
            e.tipo_logradouro, e.logradouro, e.numero, e.complemento, e.bairro,
            e.cep, e.uf, e.municipio,
            e.ddd_1, e.telefone_1, e.telefone_1_formatado,
            e.ddd_2, e.telefone_2, e.telefone_2_formatado,
            e.ddd_fax, e.fax, e.correio_eletronico,
            e.situacao_especial, e.data_situacao_especial,
            e.valid_since AS mes_referencia, e.inserted_at, e.change_hash
        FROM tcc_receita.silver_intermediate.estabelecimentos e
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
            pk, cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial,
            nome_fantasia, situacao_cadastral, situacao_cadastral_desc,
            data_situacao_cadastral, motivo_situacao_cadastral,
            nome_cidade_exterior, pais, data_inicio_atividade,
            cnae_fiscal_principal, cnae_fiscal_secundaria,
            tipo_logradouro, logradouro, numero, complemento, bairro,
            cep, uf, municipio,
            ddd_1, telefone_1, telefone_1_formatado,
            ddd_2, telefone_2, telefone_2_formatado,
            ddd_fax, fax, correio_eletronico,
            situacao_especial, data_situacao_especial,
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
        FROM tcc_receita.silver_intermediate.estabelecimentos e
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
    target.cnpj_ordem = staging.cnpj_ordem,
    target.cnpj_dv = staging.cnpj_dv,
    target.identificador_matriz_filial = staging.identificador_matriz_filial,
    target.nome_fantasia = staging.nome_fantasia,
    target.situacao_cadastral = staging.situacao_cadastral,
    target.situacao_cadastral_desc = staging.situacao_cadastral_desc,
    target.data_situacao_cadastral = staging.data_situacao_cadastral,
    target.motivo_situacao_cadastral = staging.motivo_situacao_cadastral,
    target.nome_cidade_exterior = staging.nome_cidade_exterior,
    target.pais = staging.pais,
    target.data_inicio_atividade = staging.data_inicio_atividade,
    target.cnae_fiscal_principal = staging.cnae_fiscal_principal,
    target.cnae_fiscal_secundaria = staging.cnae_fiscal_secundaria,
    target.tipo_logradouro = staging.tipo_logradouro,
    target.logradouro = staging.logradouro,
    target.numero = staging.numero,
    target.complemento = staging.complemento,
    target.bairro = staging.bairro,
    target.cep = staging.cep,
    target.uf = staging.uf,
    target.municipio = staging.municipio,
    target.ddd_1 = staging.ddd_1,
    target.telefone_1 = staging.telefone_1,
    target.telefone_1_formatado = staging.telefone_1_formatado,
    target.ddd_2 = staging.ddd_2,
    target.telefone_2 = staging.telefone_2,
    target.telefone_2_formatado = staging.telefone_2_formatado,
    target.ddd_fax = staging.ddd_fax,
    target.fax = staging.fax,
    target.correio_eletronico = staging.correio_eletronico,
    target.situacao_especial = staging.situacao_especial,
    target.data_situacao_especial = staging.data_situacao_especial,
    target.mes_referencia = staging.mes_referencia,
    target.change_hash = staging.change_hash,
    target.valid_until = staging.valid_until,
    target.is_current = staging.is_current,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    pk, cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial,
    nome_fantasia, situacao_cadastral, situacao_cadastral_desc,
    data_situacao_cadastral, motivo_situacao_cadastral,
    nome_cidade_exterior, pais, data_inicio_atividade,
    cnae_fiscal_principal, cnae_fiscal_secundaria,
    tipo_logradouro, logradouro, numero, complemento, bairro,
    cep, uf, municipio,
    ddd_1, telefone_1, telefone_1_formatado,
    ddd_2, telefone_2, telefone_2_formatado,
    ddd_fax, fax, correio_eletronico,
    situacao_especial, data_situacao_especial,
    mes_referencia, inserted_at, updated_at, change_hash,
    valid_since, valid_until, is_current
) VALUES (
    staging.pk, staging.cnpj_basico, staging.cnpj_ordem, staging.cnpj_dv, staging.identificador_matriz_filial,
    staging.nome_fantasia, staging.situacao_cadastral, staging.situacao_cadastral_desc,
    staging.data_situacao_cadastral, staging.motivo_situacao_cadastral,
    staging.nome_cidade_exterior, staging.pais, staging.data_inicio_atividade,
    staging.cnae_fiscal_principal, staging.cnae_fiscal_secundaria,
    staging.tipo_logradouro, staging.logradouro, staging.numero, staging.complemento, staging.bairro,
    staging.cep, staging.uf, staging.municipio,
    staging.ddd_1, staging.telefone_1, staging.telefone_1_formatado,
    staging.ddd_2, staging.telefone_2, staging.telefone_2_formatado,
    staging.ddd_fax, staging.fax, staging.correio_eletronico,
    staging.situacao_especial, staging.data_situacao_especial,
    staging.mes_referencia, staging.inserted_at, current_timestamp(), staging.change_hash,
    staging.valid_since, staging.valid_until, staging.is_current
);
