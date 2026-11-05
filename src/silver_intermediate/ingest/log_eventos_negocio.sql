CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.log_eventos_negocio (
    pk STRING COMMENT 'Chave primária da entidade (CPF/CNPJ).',
    entidade_tipo STRING COMMENT 'Indica a granularidade da PK: EMPRESA, ESTABELECIMENTO, SOCIO, TRIBUTARIO.',
    evento STRING COMMENT 'Tipo do evento de negócio detectado.',
    valor_antigo STRING COMMENT 'Estado do atributo antes da transição.',
    valor_novo STRING COMMENT 'Estado do atributo após a transição.',
    informacao_adicional STRING COMMENT 'Metadado complementar do evento.',
    mes_referencia STRING COMMENT 'Mês de referência em que o evento ocorreu (YYYY-MM).',
    gerado_em TIMESTAMP COMMENT 'Timestamp de processamento do evento.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Silver Intermediate: Log consolidado de eventos de transição cadastral, societária e tributária.';

-- REPLACE WHERE substitui o DELETE + INSERT, garantindo que o reprocessamento da janela seja 100% atômico e falhas parciais deem rollback completo.
INSERT OVERWRITE tcc_receita.silver_intermediate.log_eventos_negocio
REPLACE WHERE mes_referencia >= COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) 
          AND mes_referencia <= COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))
WITH params AS (
    SELECT 
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_fim
),
dom_motivos AS (
    SELECT TRIM(codigo) AS cod_motivo, MAX(UPPER(TRIM(descricao))) AS desc_motivo
    FROM tcc_receita.bronze.motivos_situacao
    GROUP BY TRIM(codigo)
),
-- 1. EMPRESA NASCEU 
ev_nascimentos AS (
    SELECT 
        e.cnpj_basico AS pk,
        'EMPRESA' AS entidade_tipo,
        'EMPRESA NASCEU' AS evento,
        CAST(NULL AS STRING) AS valor_antigo,
        'ATIVA' AS valor_novo,
        MAX(TRIM(e.data_inicio_atividade)) AS informacao_adicional,
        CONCAT(SUBSTRING(TRIM(e.data_inicio_atividade), 1, 4), '-', SUBSTRING(TRIM(e.data_inicio_atividade), 5, 2)) AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    CROSS JOIN params p
    WHERE e.identificador_matriz_filial = '1'
      AND to_date(TRIM(e.data_inicio_atividade), 'yyyyMMdd') IS NOT NULL
      AND CONCAT(SUBSTRING(TRIM(e.data_inicio_atividade), 1, 4), '-', SUBSTRING(TRIM(e.data_inicio_atividade), 5, 2)) BETWEEN p.mes_ini AND p.mes_fim
    GROUP BY e.cnpj_basico, CONCAT(SUBSTRING(TRIM(e.data_inicio_atividade), 1, 4), '-', SUBSTRING(TRIM(e.data_inicio_atividade), 5, 2))
),
-- 2. ESTADO CADASTRAL E MOTIVO DA SITUAÇÃO
ev_estado_scd2 AS (
    SELECT /*+ BROADCAST(m) */
        new.pk,
        'ESTABELECIMENTO' AS entidade_tipo,
        'MUDOU ESTADO CADASTRAL' AS evento,
        CONCAT_WS(' - ', old.situacao_cadastral_desc, old.motivo_situacao_cadastral) AS valor_antigo,
        CONCAT_WS(' - ', new.situacao_cadastral_desc, new.motivo_situacao_cadastral) AS valor_novo,
        m.desc_motivo AS informacao_adicional,
        new.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos old
    INNER JOIN tcc_receita.silver_intermediate.estabelecimentos new
        ON old.pk = new.pk AND old.valid_until = new.valid_since
    CROSS JOIN params p
    LEFT JOIN dom_motivos m ON new.motivo_situacao_cadastral = m.cod_motivo
    WHERE new.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND (COALESCE(old.situacao_cadastral_desc, '') != COALESCE(new.situacao_cadastral_desc, '') 
       OR COALESCE(old.motivo_situacao_cadastral, '') != COALESCE(new.motivo_situacao_cadastral, ''))
),
-- 3 e 4. MEI: Separação para suportar eventos simultâneos (Regra 4.1)
ev_mei AS (
    SELECT 
        new.pk,
        'TRIBUTARIO' AS entidade_tipo,
        CASE WHEN old.opcao_pelo_mei_bool = TRUE AND new.opcao_pelo_mei_bool = FALSE THEN 'SAIU DO MEI'
             WHEN old.opcao_pelo_mei_bool = FALSE AND new.opcao_pelo_mei_bool = TRUE THEN 'ENTROU NO MEI' END AS evento,
        CAST(old.opcao_pelo_mei_bool AS STRING) AS valor_antigo,
        CAST(new.opcao_pelo_mei_bool AS STRING) AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        new.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.dados_simples old
    INNER JOIN tcc_receita.silver_intermediate.dados_simples new ON old.pk = new.pk AND old.valid_until = new.valid_since
    CROSS JOIN params p
    WHERE new.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND COALESCE(old.opcao_pelo_mei_bool, FALSE) != COALESCE(new.opcao_pelo_mei_bool, FALSE)
),
-- 5 e 6. SIMPLES: Separação para suportar eventos simultâneos (Regra 4.1)
ev_simples AS (
    SELECT 
        new.pk,
        'TRIBUTARIO' AS entidade_tipo,
        CASE WHEN old.opcao_pelo_simples_bool = TRUE AND new.opcao_pelo_simples_bool = FALSE THEN 'SAIU DO SIMPLES'
             WHEN old.opcao_pelo_simples_bool = FALSE AND new.opcao_pelo_simples_bool = TRUE THEN 'ENTROU NO SIMPLES' END AS evento,
        CAST(old.opcao_pelo_simples_bool AS STRING) AS valor_antigo,
        CAST(new.opcao_pelo_simples_bool AS STRING) AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        new.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.dados_simples old
    INNER JOIN tcc_receita.silver_intermediate.dados_simples new ON old.pk = new.pk AND old.valid_until = new.valid_since
    CROSS JOIN params p
    WHERE new.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND COALESCE(old.opcao_pelo_simples_bool, FALSE) != COALESCE(new.opcao_pelo_simples_bool, FALSE)
),
-- 7. Natureza Juridica
ev_natureza AS (
    SELECT 
        new.pk,
        'EMPRESA' AS entidade_tipo,
        'MUDOU NATUREZA JURIDICA' AS evento,
        old.natureza_juridica AS valor_antigo,
        new.natureza_juridica AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        new.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.empresas old
    INNER JOIN tcc_receita.silver_intermediate.empresas new ON old.pk = new.pk AND old.valid_until = new.valid_since
    CROSS JOIN params p
    WHERE new.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND COALESCE(old.natureza_juridica, '') != COALESCE(new.natureza_juridica, '')
),
-- 8. CNAE Principal (Isolado)
ev_cnae_principal AS (
    SELECT 
        new.pk,
        'ESTABELECIMENTO' AS entidade_tipo,
        'MUDOU CNAE PRINCIPAL' AS evento,
        old.cnae_fiscal_principal AS valor_antigo,
        new.cnae_fiscal_principal AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        new.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos old
    INNER JOIN tcc_receita.silver_intermediate.estabelecimentos new ON old.pk = new.pk AND old.valid_until = new.valid_since
    CROSS JOIN params p
    WHERE new.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND COALESCE(old.cnae_fiscal_principal, '') != COALESCE(new.cnae_fiscal_principal, '')
),
-- 9. CNAE Secundario (Isolado e explícito)
ev_cnae_secundario AS (
    SELECT 
        new.pk,
        'ESTABELECIMENTO' AS entidade_tipo,
        'MUDOU LISTA CNAE SECUNDARIO' AS evento,
        old.cnae_fiscal_secundaria AS valor_antigo,
        new.cnae_fiscal_secundaria AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        new.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos old
    INNER JOIN tcc_receita.silver_intermediate.estabelecimentos new ON old.pk = new.pk AND old.valid_until = new.valid_since
    CROSS JOIN params p
    WHERE new.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND COALESCE(old.cnae_fiscal_secundaria, '') != COALESCE(new.cnae_fiscal_secundaria, '')
),
todos_eventos AS (
    SELECT * FROM ev_nascimentos
    UNION ALL SELECT * FROM ev_estado_scd2
    UNION ALL SELECT * FROM ev_mei
    UNION ALL SELECT * FROM ev_simples
    UNION ALL SELECT * FROM ev_natureza
    UNION ALL SELECT * FROM ev_cnae_principal
    UNION ALL SELECT * FROM ev_cnae_secundario
)
SELECT 
    pk, entidade_tipo, evento, valor_antigo, valor_novo, informacao_adicional, mes_referencia, NOW() AS gerado_em
FROM todos_eventos;