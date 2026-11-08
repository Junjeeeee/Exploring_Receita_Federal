-- Silver Intermediate: histórico SCD2 do QSA (relação empresa-sócio).
-- IMPORTANTE: para detectar saída do QSA, este SQL compara cada snapshot mensal com o snapshot
-- imediatamente anterior. A Bronze precisa conter o mês anterior a mes_ini e TODOS os meses
-- da janela, completos. Exceção (carga inicial): se mes_ini for o primeiro mês existente na Bronze
-- (2023-05, primeiro snapshot publicado pela RFB), não há mês anterior; o mês só abre as versões e
-- nenhuma saída do QSA é inferida nele. Veja README.md antes da primeira execução.
-- A PK é herdada da Bronze; nenhum CPF/CNPJ em claro é reconstruído aqui.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.socios (
    pk STRING COMMENT 'Chave primária lógica da relação entre uma empresa e um sócio, herdada da Bronze.',
    cnpj_basico STRING COMMENT 'Radical do CNPJ da empresa (primeiros 8 dígitos).',
    identificador_socio STRING COMMENT '1=PJ, 2=PF, 3=Estrangeiro, conforme domínio da fonte.',
    nome_socio_razao_social STRING COMMENT 'Nome ou razão social do sócio limpo e padronizado; acentos preservados.',
    cnpj_cpf_socio STRING COMMENT 'Documento do sócio conforme recebido, parcialmente mascarado quando assim fornecido pela origem.',
    qualificacao_socio STRING COMMENT 'Código da qualificação do sócio na sociedade.',
    data_entrada_sociedade STRING COMMENT 'Data de entrada na sociedade validada (AAAAMMDD).',
    pais STRING COMMENT 'Código do país do sócio estrangeiro.',
    representante_legal STRING COMMENT 'Documento do representante legal conforme recebido, sem desmascaramento.',
    nome_representante STRING COMMENT 'Nome do representante legal limpo e padronizado.',
    qualificacao_representante_legal STRING COMMENT 'Código da qualificação do representante legal.',
    faixa_etaria STRING COMMENT 'Código da faixa etária do sócio pessoa física.',
    vinculo_ativo BOOLEAN COMMENT 'TRUE quando a relação aparece no snapshot mensal; FALSE a partir do primeiro snapshot completo em que deixa de aparecer.',
    mes_referencia STRING COMMENT 'Mês do snapshot que originou a versão ou em que a ausência foi detectada.',
    inserted_at TIMESTAMP COMMENT 'Timestamp do processamento Bronze que originou os atributos da versão.',
    updated_at TIMESTAMP COMMENT 'Timestamp da última atualização da versão no SCD2.',
    change_hash STRING COMMENT 'SHA-256 do hash de origem combinado com o estado do vínculo (ativo/inativo).',
    valid_since STRING COMMENT 'Início da vigência da versão (YYYY-MM), inclusivo.',
    valid_until STRING COMMENT 'Fim da vigência da versão (YYYY-MM), exclusivo; NULL para versão atual.',
    is_current BOOLEAN COMMENT 'Indica se esta é a versão vigente.'
)
USING DELTA
PARTITIONED BY (is_current)
COMMENT 'Camada Silver Intermediate (SCD2): Histórico do Quadro de Sócios e Administradores e da vigência das relações societárias.';

MERGE INTO tcc_receita.silver_intermediate.socios AS target
USING (
    WITH params AS (
        SELECT
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim,
            -- Primeiro snapshot disponível: janela iniciando nele é carga inicial (sem mês anterior).
            (SELECT MIN(mes_referencia) FROM tcc_receita.bronze.socios) AS primeiro_mes_bronze
    ),
    bronze_raw AS (
        SELECT b.*,
               ROW_NUMBER() OVER (
                   PARTITION BY b.pk, b.mes_referencia
                   ORDER BY b.bronze_processed_at DESC NULLS LAST
               ) AS rn
        FROM tcc_receita.bronze.socios b
        CROSS JOIN params p
        WHERE b.mes_referencia BETWEEN
              date_format(add_months(to_date(concat(p.mes_ini, '-01')), -1), 'yyyy-MM')
              AND p.mes_fim
          AND b.pk IS NOT NULL
    ),
    validacao_snapshots AS (
        SELECT
            ASSERT_TRUE(
                p.mes_ini RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
                AND p.mes_fim RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
                AND p.mes_ini <= p.mes_fim,
                'Janela inválida: informe mes_ini e mes_fim no formato YYYY-MM e mes_ini <= mes_fim.'
            ) AS janela_valida,
            ASSERT_TRUE(
                COUNT(DISTINCT b.mes_referencia) = CAST(MONTHS_BETWEEN(
                    TO_DATE(CONCAT(p.mes_fim, '-01')),
                    TO_DATE(CONCAT(p.mes_ini, '-01'))
                ) AS INT) + 1
                  + CASE WHEN p.mes_ini > p.primeiro_mes_bronze THEN 1 ELSE 0 END,
                'Snapshots Bronze de socios ausentes: é necessário o mês anterior ao início (exceto na carga inicial) e todos os meses da janela.'
            ) AS snapshots_validos
        FROM bronze_raw b
        CROSS JOIN params p
        GROUP BY p.mes_ini, p.mes_fim, p.primeiro_mes_bronze
    ),
    bronze_limpo AS (
        SELECT
            pk,
            NULLIF(TRIM(cnpj_basico), '') AS cnpj_basico,
            NULLIF(TRIM(identificador_socio), '') AS identificador_socio,
            NULLIF(UPPER(TRIM(regexp_replace(nome_socio_razao_social, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS nome_socio_razao_social,
            NULLIF(TRIM(cnpj_cpf_socio), '') AS cnpj_cpf_socio,
            NULLIF(TRIM(qualificacao_socio), '') AS qualificacao_socio,
            CASE
                WHEN try_to_date(TRIM(data_entrada_sociedade), 'yyyyMMdd') IS NOT NULL
                THEN TRIM(data_entrada_sociedade)
                ELSE NULL
            END AS data_entrada_sociedade,
            NULLIF(TRIM(pais), '') AS pais,
            NULLIF(TRIM(representante_legal), '') AS representante_legal,
            NULLIF(UPPER(TRIM(regexp_replace(nome_representante, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS nome_representante,
            NULLIF(TRIM(qualificacao_representante_legal), '') AS qualificacao_representante_legal,
            NULLIF(TRIM(faixa_etaria), '') AS faixa_etaria,
            mes_referencia,
            bronze_processed_at AS inserted_at,
            change_hash AS source_change_hash
        FROM bronze_raw
        CROSS JOIN validacao_snapshots v
        WHERE rn = 1
          AND COALESCE(v.janela_valida, TRUE)
          AND COALESCE(v.snapshots_validos, TRUE)
    ),
    meses_janela AS (
        SELECT EXPLODE(
            SEQUENCE(
                TO_DATE(CONCAT(p.mes_ini, '-01')),
                TO_DATE(CONCAT(p.mes_fim, '-01')),
                INTERVAL 1 MONTH
            )
        ) AS mes_data
        FROM params p
    ),
    estados_ativos AS (
        SELECT
            b.pk, b.cnpj_basico, b.identificador_socio, b.nome_socio_razao_social,
            b.cnpj_cpf_socio, b.qualificacao_socio, b.data_entrada_sociedade,
            b.pais, b.representante_legal, b.nome_representante,
            b.qualificacao_representante_legal, b.faixa_etaria,
            TRUE AS vinculo_ativo,
            b.mes_referencia,
            b.inserted_at,
            b.source_change_hash AS change_hash
        FROM bronze_limpo b
        CROSS JOIN params p
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
    ),
    estados_inativos AS (
        -- Para cada relação presente no mês anterior e ausente no mês atual,
        -- cria uma versão inativa no mês atual, carregando os últimos atributos conhecidos.
        SELECT
            anterior.pk, anterior.cnpj_basico, anterior.identificador_socio,
            anterior.nome_socio_razao_social, anterior.cnpj_cpf_socio,
            anterior.qualificacao_socio, anterior.data_entrada_sociedade,
            anterior.pais, anterior.representante_legal, anterior.nome_representante,
            anterior.qualificacao_representante_legal, anterior.faixa_etaria,
            FALSE AS vinculo_ativo,
            date_format(m.mes_data, 'yyyy-MM') AS mes_referencia,
            anterior.inserted_at,
            SHA2(TO_JSON(NAMED_STRUCT(
                'source_change_hash', anterior.source_change_hash,
                'vinculo_ativo', FALSE
            )), 256) AS change_hash
        FROM meses_janela m
        INNER JOIN bronze_limpo anterior
            ON anterior.mes_referencia = date_format(add_months(m.mes_data, -1), 'yyyy-MM')
        LEFT ANTI JOIN bronze_limpo atual
            ON atual.pk = anterior.pk
           AND atual.mes_referencia = date_format(m.mes_data, 'yyyy-MM')
    ),
    estados_inativos_legado AS (
        -- Na primeira execução desta regra, corrige vínculos que ficaram marcados como atuais
        -- pela lógica antiga, mas já não aparecem nem no mês anterior nem no primeiro mês da janela.
        SELECT
            s.pk, s.cnpj_basico, s.identificador_socio, s.nome_socio_razao_social,
            s.cnpj_cpf_socio, s.qualificacao_socio, s.data_entrada_sociedade,
            s.pais, s.representante_legal, s.nome_representante,
            s.qualificacao_representante_legal, s.faixa_etaria,
            FALSE AS vinculo_ativo,
            p.mes_ini AS mes_referencia,
            s.inserted_at,
            SHA2(TO_JSON(NAMED_STRUCT(
                'source_change_hash', s.change_hash,
                'vinculo_ativo', FALSE
            )), 256) AS change_hash
        FROM tcc_receita.silver_intermediate.socios s
        CROSS JOIN params p
        LEFT ANTI JOIN bronze_limpo atual
            ON atual.pk = s.pk
           AND atual.mes_referencia = p.mes_ini
        LEFT ANTI JOIN bronze_limpo anterior
            ON anterior.pk = s.pk
           AND anterior.mes_referencia = date_format(add_months(to_date(concat(p.mes_ini, '-01')), -1), 'yyyy-MM')
        WHERE s.is_current = TRUE
          AND s.vinculo_ativo = TRUE
    ),
    eventos_janela AS (
        SELECT * FROM estados_ativos
        UNION ALL
        SELECT * FROM estados_inativos
        UNION ALL
        SELECT * FROM estados_inativos_legado
    ),
    pks_afetadas AS (
        SELECT DISTINCT pk FROM eventos_janela
        UNION
        SELECT DISTINCT s.pk
        FROM tcc_receita.silver_intermediate.socios s
        CROSS JOIN params p
        WHERE s.valid_since BETWEEN p.mes_ini AND p.mes_fim
    ),
    historico_intacto AS (
        SELECT
            s.pk, s.cnpj_basico, s.identificador_socio, s.nome_socio_razao_social,
            s.cnpj_cpf_socio, s.qualificacao_socio, s.data_entrada_sociedade,
            s.pais, s.representante_legal, s.nome_representante,
            s.qualificacao_representante_legal, s.faixa_etaria,
            s.vinculo_ativo,
            s.valid_since AS mes_referencia,
            s.inserted_at,
            s.change_hash
        FROM tcc_receita.silver_intermediate.socios s
        INNER JOIN pks_afetadas a ON s.pk = a.pk
        CROSS JOIN params p
        WHERE s.valid_since < p.mes_ini OR s.valid_since > p.mes_fim
    ),
    uniao_historico AS (
        SELECT * FROM historico_intacto
        UNION ALL
        SELECT * FROM eventos_janela
    ),
    deteccao_mudancas AS (
        SELECT *,
               LAG(change_hash) OVER (PARTITION BY pk ORDER BY mes_referencia) AS prev_hash
        FROM uniao_historico
    ),
    recalculo_vigencia AS (
        SELECT
            pk, cnpj_basico, identificador_socio, nome_socio_razao_social,
            cnpj_cpf_socio, qualificacao_socio, data_entrada_sociedade,
            pais, representante_legal, nome_representante,
            qualificacao_representante_legal, faixa_etaria,
            vinculo_ativo, mes_referencia, inserted_at, change_hash,
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
        SELECT old.pk, old.valid_since
        FROM tcc_receita.silver_intermediate.socios old
        INNER JOIN pks_afetadas a ON old.pk = a.pk
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
    target.identificador_socio = staging.identificador_socio,
    target.nome_socio_razao_social = staging.nome_socio_razao_social,
    target.cnpj_cpf_socio = staging.cnpj_cpf_socio,
    target.qualificacao_socio = staging.qualificacao_socio,
    target.data_entrada_sociedade = staging.data_entrada_sociedade,
    target.pais = staging.pais,
    target.representante_legal = staging.representante_legal,
    target.nome_representante = staging.nome_representante,
    target.qualificacao_representante_legal = staging.qualificacao_representante_legal,
    target.faixa_etaria = staging.faixa_etaria,
    target.vinculo_ativo = staging.vinculo_ativo,
    target.mes_referencia = staging.mes_referencia,
    target.inserted_at = staging.inserted_at,
    target.change_hash = staging.change_hash,
    target.valid_until = staging.valid_until,
    target.is_current = staging.is_current,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    pk, cnpj_basico, identificador_socio, nome_socio_razao_social,
    cnpj_cpf_socio, qualificacao_socio, data_entrada_sociedade,
    pais, representante_legal, nome_representante,
    qualificacao_representante_legal, faixa_etaria,
    vinculo_ativo, mes_referencia, inserted_at, updated_at, change_hash,
    valid_since, valid_until, is_current
) VALUES (
    staging.pk, staging.cnpj_basico, staging.identificador_socio, staging.nome_socio_razao_social,
    staging.cnpj_cpf_socio, staging.qualificacao_socio, staging.data_entrada_sociedade,
    staging.pais, staging.representante_legal, staging.nome_representante,
    staging.qualificacao_representante_legal, staging.faixa_etaria,
    staging.vinculo_ativo, staging.mes_referencia, staging.inserted_at, current_timestamp(), staging.change_hash,
    staging.valid_since, staging.valid_until, staging.is_current
);
