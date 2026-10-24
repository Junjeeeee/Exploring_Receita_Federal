CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.log_eventos_negocio (
    pk STRING COMMENT 'Chave primária da entidade onde o evento ocorreu.',
    evento STRING COMMENT 'Tipo do evento de negócio detectado (7 regras mapeadas).',
    valor_antigo STRING COMMENT 'Estado do atributo antes da transição.',
    valor_novo STRING COMMENT 'Estado do atributo após a transição.',
    informacao_adicional STRING COMMENT 'Metadado complementar do evento (motivo cadastral, data ou CNAE secundário).',
    mes_referencia STRING COMMENT 'Mês de referência em que o evento ocorreu (YYYY-MM).',
    gerado_em TIMESTAMP COMMENT 'Timestamp de processamento do evento.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Silver Intermediate: Log consolidado de eventos de transição cadastral, societária e tributária.';

-- 1. Idempotência para o intervalo processado
DELETE FROM tcc_receita.silver_intermediate.log_eventos_negocio
WHERE mes_referencia >= COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))
  AND mes_referencia <= COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'));

-- 2. Inserção vetorial das 7 regras de negócio
INSERT INTO tcc_receita.silver_intermediate.log_eventos_negocio
WITH params AS (
    SELECT 
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_fim
),
dom_motivos AS (
    SELECT 
        TRIM(codigo) AS cod_motivo, 
        MAX(UPPER(TRIM(descricao))) AS desc_motivo
    FROM tcc_receita.bronze.motivos_situacao
    GROUP BY TRIM(codigo)
),
-- 1. EMPRESA NASCEU (Matrizes com data_inicio_atividade dentro da janela)
ev_nascimentos AS (
    SELECT 
        e.cnpj_basico AS pk,
        'EMPRESA NASCEU' AS evento,
        CAST(NULL AS STRING) AS valor_antigo,
        'ATIVA' AS valor_novo,
        MAX(TRIM(e.data_inicio_atividade)) AS informacao_adicional,
        CONCAT(SUBSTRING(TRIM(e.data_inicio_atividade), 1, 4), '-', SUBSTRING(TRIM(e.data_inicio_atividade), 5, 2)) AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    CROSS JOIN params p
    WHERE e.identificador_matriz_filial = '1'
      AND LENGTH(TRIM(e.data_inicio_atividade)) = 8
      AND CONCAT(SUBSTRING(TRIM(e.data_inicio_atividade), 1, 4), '-', SUBSTRING(TRIM(e.data_inicio_atividade), 5, 2)) >= p.mes_ini
      AND CONCAT(SUBSTRING(TRIM(e.data_inicio_atividade), 1, 4), '-', SUBSTRING(TRIM(e.data_inicio_atividade), 5, 2)) <= p.mes_fim
    GROUP BY e.cnpj_basico, CONCAT(SUBSTRING(TRIM(e.data_inicio_atividade), 1, 4), '-', SUBSTRING(TRIM(e.data_inicio_atividade), 5, 2))
),
-- 2A. MUDOU ESTADO CADASTRAL (Mês base 2023-05 via data_situacao_cadastral)
ev_estado_base AS (
    SELECT /*+ BROADCAST(m) */
        e.pk,
        'MUDOU ESTADO CADASTRAL' AS evento,
        CAST(NULL AS STRING) AS valor_antigo,
        e.situacao_cadastral_desc AS valor_novo,
        CONCAT_WS(' - ', e.motivo_situacao_cadastral, m.desc_motivo) AS informacao_adicional,
        '2023-05' AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    CROSS JOIN params p
    LEFT JOIN dom_motivos m ON e.motivo_situacao_cadastral = m.cod_motivo
    WHERE '2023-05' >= p.mes_ini AND '2023-05' <= p.mes_fim
      AND e.valid_since = '2023-05'
      AND LENGTH(TRIM(e.data_situacao_cadastral)) = 8
      AND SUBSTRING(TRIM(e.data_situacao_cadastral), 1, 6) = '202305'
      AND e.situacao_cadastral_desc != 'ATIVA'
),
-- 2B. MUDOU ESTADO CADASTRAL (Demais meses via transição SCD2)
ev_estado_scd2 AS (
    SELECT /*+ BROADCAST(m) */
        new.pk,
        'MUDOU ESTADO CADASTRAL' AS evento,
        old.situacao_cadastral_desc AS valor_antigo,
        new.situacao_cadastral_desc AS valor_novo,
        CONCAT_WS(' - ', new.motivo_situacao_cadastral, m.desc_motivo) AS informacao_adicional,
        new.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos old
    INNER JOIN tcc_receita.silver_intermediate.estabelecimentos new
        ON old.pk = new.pk AND old.valid_until = new.valid_since
    CROSS JOIN params p
    LEFT JOIN dom_motivos m ON new.motivo_situacao_cadastral = m.cod_motivo
    WHERE new.valid_since >= p.mes_ini AND new.valid_since <= p.mes_fim
      AND new.valid_since != '2023-05'
      AND COALESCE(old.situacao_cadastral_desc, '') != COALESCE(new.situacao_cadastral_desc, '')
),
-- 3, 4 e 5. Transições do Simples e MEI
ev_simples_mei AS (
    SELECT 
        new.pk,
        CASE 
            WHEN old.opcao_pelo_mei_bool = TRUE AND new.opcao_pelo_mei_bool = FALSE THEN 'SAIU DO MEI'
            WHEN old.opcao_pelo_simples_bool = FALSE AND new.opcao_pelo_simples_bool = TRUE THEN 'ENTROU NO SIMPLES'
            WHEN old.opcao_pelo_simples_bool = TRUE AND new.opcao_pelo_simples_bool = FALSE THEN 'SAIU DO SIMPLES'
        END AS evento,
        CASE 
            WHEN old.opcao_pelo_mei_bool = TRUE AND new.opcao_pelo_mei_bool = FALSE THEN 'True'
            WHEN old.opcao_pelo_simples_bool = FALSE AND new.opcao_pelo_simples_bool = TRUE THEN 'False'
            WHEN old.opcao_pelo_simples_bool = TRUE AND new.opcao_pelo_simples_bool = FALSE THEN 'True'
        END AS valor_antigo,
        CASE 
            WHEN old.opcao_pelo_mei_bool = TRUE AND new.opcao_pelo_mei_bool = FALSE THEN 'False'
            WHEN old.opcao_pelo_simples_bool = FALSE AND new.opcao_pelo_simples_bool = TRUE THEN 'True'
            WHEN old.opcao_pelo_simples_bool = TRUE AND new.opcao_pelo_simples_bool = FALSE THEN 'False'
        END AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        new.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.dados_simples old
    INNER JOIN tcc_receita.silver_intermediate.dados_simples new
        ON old.pk = new.pk AND old.valid_until = new.valid_since
    CROSS JOIN params p
    WHERE new.valid_since >= p.mes_ini AND new.valid_since <= p.mes_fim
      AND (
          (old.opcao_pelo_mei_bool = TRUE AND new.opcao_pelo_mei_bool = FALSE) OR
          (old.opcao_pelo_simples_bool = FALSE AND new.opcao_pelo_simples_bool = TRUE) OR
          (old.opcao_pelo_simples_bool = TRUE AND new.opcao_pelo_simples_bool = FALSE)
      )
),
-- 6. Mudou de Natureza Jurídica
ev_natureza AS (
    SELECT 
        new.pk,
        'MUDOU NATUREZA JURIDICA' AS evento,
        old.natureza_juridica AS valor_antigo,
        new.natureza_juridica AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        new.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.empresas old
    INNER JOIN tcc_receita.silver_intermediate.empresas new
        ON old.pk = new.pk AND old.valid_until = new.valid_since
    CROSS JOIN params p
    WHERE new.valid_since >= p.mes_ini AND new.valid_since <= p.mes_fim
      AND COALESCE(old.natureza_juridica, '') != COALESCE(new.natureza_juridica, '')
),
-- 7. Mudou CNAE
ev_cnae AS (
    SELECT 
        new.pk,
        'MUDOU CNAE' AS evento,
        old.cnae_fiscal_principal AS valor_antigo,
        new.cnae_fiscal_principal AS valor_novo,
        new.cnae_fiscal_secundaria AS informacao_adicional,
        new.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos old
    INNER JOIN tcc_receita.silver_intermediate.estabelecimentos new
        ON old.pk = new.pk AND old.valid_until = new.valid_since
    CROSS JOIN params p
    WHERE new.valid_since >= p.mes_ini AND new.valid_since <= p.mes_fim
      AND (
          COALESCE(old.cnae_fiscal_principal, '') != COALESCE(new.cnae_fiscal_principal, '') OR
          COALESCE(old.cnae_fiscal_secundaria, '') != COALESCE(new.cnae_fiscal_secundaria, '')
      )
),
todos_eventos AS (
    SELECT * FROM ev_nascimentos
    UNION ALL SELECT * FROM ev_estado_base
    UNION ALL SELECT * FROM ev_estado_scd2
    UNION ALL SELECT * FROM ev_simples_mei
    UNION ALL SELECT * FROM ev_natureza
    UNION ALL SELECT * FROM ev_cnae
)
SELECT 
    pk,
    evento,
    valor_antigo,
    valor_novo,
    informacao_adicional,
    mes_referencia,
    NOW() AS gerado_em
FROM todos_eventos;