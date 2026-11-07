-- Silver Intermediate: log consolidado de eventos de negócio.
-- IMPORTANTE: substitua os dois parâmetros por literais YYYY-MM antes da execução.
-- Exemplo: '{{mes_referencia_inicial}}' -> '2026-01'. Não deixe os parâmetros vazios.
-- O predicado REPLACE WHERE do Databricks aceita apenas expressões restritas e valores literais.
-- Para o padrão do projeto, resolva ambos para o mês anterior quando nenhum intervalo for informado.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.log_eventos_negocio (
    pk STRING COMMENT 'Chave lógica da entidade ou da relação, conforme a granularidade da tabela de origem.',
    entidade_tipo STRING COMMENT 'Granularidade da chave: EMPRESA, ESTABELECIMENTO, SOCIO ou TRIBUTARIO.',
    evento STRING COMMENT 'Tipo do evento de negócio detectado.',
    valor_antigo STRING COMMENT 'Estado do atributo antes da transição.',
    valor_novo STRING COMMENT 'Estado do atributo após a transição.',
    informacao_adicional STRING COMMENT 'Metadado complementar do evento; não deve conter documentos pessoais em claro.',
    mes_referencia STRING COMMENT 'Mês de referência do evento (YYYY-MM). Para saídas do QSA, mês em que a ausência foi detectada.',
    gerado_em TIMESTAMP COMMENT 'Timestamp de processamento do evento.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Silver Intermediate: Log consolidado de transições cadastrais, societárias e tributárias.';

WITH params AS (
    SELECT
        CAST('{{mes_referencia_inicial}}' AS STRING) AS mes_ini,
        CAST('{{mes_referencia_final}}' AS STRING) AS mes_fim
),
dom_motivos AS (
    SELECT
        TRIM(codigo) AS cod_motivo,
        MAX(UPPER(TRIM(descricao))) AS desc_motivo
    FROM tcc_receita.bronze.motivos_situacao
    GROUP BY TRIM(codigo)
),
-- 1. Início de atividade da empresa. A PK é obtida da tabela empresas, não do CNPJ básico em texto.
ev_inicio_atividade_empresa AS (
    SELECT DISTINCT
        emp.pk,
        'EMPRESA' AS entidade_tipo,
        'EMPRESA NASCEU' AS evento,
        CAST(NULL AS STRING) AS valor_antigo,
        'ATIVIDADE INICIADA' AS valor_novo,
        CONCAT('Data informada pela Receita Federal: ', est.data_inicio_atividade) AS informacao_adicional,
        date_format(to_date(TRIM(est.data_inicio_atividade), 'yyyyMMdd'), 'yyyy-MM') AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos est
    INNER JOIN tcc_receita.silver_intermediate.empresas emp
        ON emp.cnpj_basico = est.cnpj_basico
    CROSS JOIN params p
    WHERE est.identificador_matriz_filial = '1'
      AND to_date(TRIM(est.data_inicio_atividade), 'yyyyMMdd') IS NOT NULL
      AND date_format(to_date(TRIM(est.data_inicio_atividade), 'yyyyMMdd'), 'yyyy-MM') BETWEEN p.mes_ini AND p.mes_fim
),
-- 2. Mudanças na situação cadastral ou no motivo do estabelecimento.
ev_estado_cadastral AS (
    SELECT /*+ BROADCAST(m) */
        novo.pk,
        'ESTABELECIMENTO' AS entidade_tipo,
        'MUDOU ESTADO CADASTRAL' AS evento,
        CONCAT_WS(' - ', antigo.situacao_cadastral_desc, antigo.motivo_situacao_cadastral) AS valor_antigo,
        CONCAT_WS(' - ', novo.situacao_cadastral_desc, novo.motivo_situacao_cadastral) AS valor_novo,
        m.desc_motivo AS informacao_adicional,
        novo.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos antigo
    INNER JOIN tcc_receita.silver_intermediate.estabelecimentos novo
        ON antigo.pk = novo.pk
       AND antigo.valid_until = novo.valid_since
    CROSS JOIN params p
    LEFT JOIN dom_motivos m
        ON novo.motivo_situacao_cadastral = m.cod_motivo
    WHERE novo.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND (
             antigo.situacao_cadastral_desc IS DISTINCT FROM novo.situacao_cadastral_desc
          OR antigo.motivo_situacao_cadastral IS DISTINCT FROM novo.motivo_situacao_cadastral
      )
),
-- 3. Entradas no MEI.
ev_entrou_mei AS (
    SELECT
        novo.pk,
        'TRIBUTARIO' AS entidade_tipo,
        'ENTROU NO MEI' AS evento,
        CAST(antigo.opcao_pelo_mei_bool AS STRING) AS valor_antigo,
        CAST(novo.opcao_pelo_mei_bool AS STRING) AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        novo.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.dados_simples antigo
    INNER JOIN tcc_receita.silver_intermediate.dados_simples novo
        ON antigo.pk = novo.pk
       AND antigo.valid_until = novo.valid_since
    CROSS JOIN params p
    WHERE novo.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND antigo.opcao_pelo_mei_bool = FALSE
      AND novo.opcao_pelo_mei_bool = TRUE
),
-- 4. Saídas do MEI.
ev_saiu_mei AS (
    SELECT
        novo.pk,
        'TRIBUTARIO' AS entidade_tipo,
        'SAIU DO MEI' AS evento,
        CAST(antigo.opcao_pelo_mei_bool AS STRING) AS valor_antigo,
        CAST(novo.opcao_pelo_mei_bool AS STRING) AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        novo.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.dados_simples antigo
    INNER JOIN tcc_receita.silver_intermediate.dados_simples novo
        ON antigo.pk = novo.pk
       AND antigo.valid_until = novo.valid_since
    CROSS JOIN params p
    WHERE novo.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND antigo.opcao_pelo_mei_bool = TRUE
      AND novo.opcao_pelo_mei_bool = FALSE
),
-- 5. Entradas no Simples Nacional.
ev_entrou_simples AS (
    SELECT
        novo.pk,
        'TRIBUTARIO' AS entidade_tipo,
        'ENTROU NO SIMPLES' AS evento,
        CAST(antigo.opcao_pelo_simples_bool AS STRING) AS valor_antigo,
        CAST(novo.opcao_pelo_simples_bool AS STRING) AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        novo.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.dados_simples antigo
    INNER JOIN tcc_receita.silver_intermediate.dados_simples novo
        ON antigo.pk = novo.pk
       AND antigo.valid_until = novo.valid_since
    CROSS JOIN params p
    WHERE novo.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND antigo.opcao_pelo_simples_bool = FALSE
      AND novo.opcao_pelo_simples_bool = TRUE
),
-- 6. Saídas do Simples Nacional.
ev_saiu_simples AS (
    SELECT
        novo.pk,
        'TRIBUTARIO' AS entidade_tipo,
        'SAIU DO SIMPLES' AS evento,
        CAST(antigo.opcao_pelo_simples_bool AS STRING) AS valor_antigo,
        CAST(novo.opcao_pelo_simples_bool AS STRING) AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        novo.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.dados_simples antigo
    INNER JOIN tcc_receita.silver_intermediate.dados_simples novo
        ON antigo.pk = novo.pk
       AND antigo.valid_until = novo.valid_since
    CROSS JOIN params p
    WHERE novo.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND antigo.opcao_pelo_simples_bool = TRUE
      AND novo.opcao_pelo_simples_bool = FALSE
),
-- 7. Mudança de natureza jurídica da empresa.
ev_natureza_juridica AS (
    SELECT
        novo.pk,
        'EMPRESA' AS entidade_tipo,
        'MUDOU NATUREZA JURIDICA' AS evento,
        antigo.natureza_juridica AS valor_antigo,
        novo.natureza_juridica AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        novo.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.empresas antigo
    INNER JOIN tcc_receita.silver_intermediate.empresas novo
        ON antigo.pk = novo.pk
       AND antigo.valid_until = novo.valid_since
    CROSS JOIN params p
    WHERE novo.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND antigo.natureza_juridica IS DISTINCT FROM novo.natureza_juridica
),
-- 8. Mudança de CNAE principal.
ev_cnae_principal AS (
    SELECT
        novo.pk,
        'ESTABELECIMENTO' AS entidade_tipo,
        'MUDOU CNAE PRINCIPAL' AS evento,
        antigo.cnae_fiscal_principal AS valor_antigo,
        novo.cnae_fiscal_principal AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        novo.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos antigo
    INNER JOIN tcc_receita.silver_intermediate.estabelecimentos novo
        ON antigo.pk = novo.pk
       AND antigo.valid_until = novo.valid_since
    CROSS JOIN params p
    WHERE novo.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND antigo.cnae_fiscal_principal IS DISTINCT FROM novo.cnae_fiscal_principal
),
-- 9. Mudança na lista de CNAEs secundários.
ev_cnae_secundario AS (
    SELECT
        novo.pk,
        'ESTABELECIMENTO' AS entidade_tipo,
        'MUDOU LISTA CNAE SECUNDARIO' AS evento,
        antigo.cnae_fiscal_secundaria AS valor_antigo,
        novo.cnae_fiscal_secundaria AS valor_novo,
        CAST(NULL AS STRING) AS informacao_adicional,
        novo.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.estabelecimentos antigo
    INNER JOIN tcc_receita.silver_intermediate.estabelecimentos novo
        ON antigo.pk = novo.pk
       AND antigo.valid_until = novo.valid_since
    CROSS JOIN params p
    WHERE novo.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND antigo.cnae_fiscal_secundaria IS DISTINCT FROM novo.cnae_fiscal_secundaria
),
-- 10. Entrada de sócio, usando a data declarada pela fonte (não a data de detecção do SCD2).
ev_socio_entrou AS (
    SELECT DISTINCT
        s.pk,
        'SOCIO' AS entidade_tipo,
        'SOCIO ENTROU NO QSA' AS evento,
        CAST(NULL AS STRING) AS valor_antigo,
        'ATIVO' AS valor_novo,
        CONCAT('Data de entrada informada pela Receita Federal: ', s.data_entrada_sociedade) AS informacao_adicional,
        date_format(to_date(s.data_entrada_sociedade, 'yyyyMMdd'), 'yyyy-MM') AS mes_referencia
    FROM tcc_receita.silver_intermediate.socios s
    CROSS JOIN params p
    WHERE s.vinculo_ativo = TRUE
      AND to_date(s.data_entrada_sociedade, 'yyyyMMdd') IS NOT NULL
      AND date_format(to_date(s.data_entrada_sociedade, 'yyyyMMdd'), 'yyyy-MM') BETWEEN p.mes_ini AND p.mes_fim
),
-- 11. Saída do QSA: a data é o mês do primeiro snapshot completo em que a relação não aparece.
ev_socio_saiu AS (
    SELECT
        novo.pk,
        'SOCIO' AS entidade_tipo,
        'SOCIO SAIU DO QSA' AS evento,
        'ATIVO' AS valor_antigo,
        'INATIVO' AS valor_novo,
        novo.qualificacao_socio AS informacao_adicional,
        novo.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.socios antigo
    INNER JOIN tcc_receita.silver_intermediate.socios novo
        ON antigo.pk = novo.pk
       AND antigo.valid_until = novo.valid_since
    CROSS JOIN params p
    WHERE novo.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND antigo.vinculo_ativo = TRUE
      AND novo.vinculo_ativo = FALSE
),
-- 12. Reingresso de relação que havia sido marcada como inativa.
ev_socio_reentrou AS (
    SELECT
        novo.pk,
        'SOCIO' AS entidade_tipo,
        'SOCIO RETORNOU AO QSA' AS evento,
        'INATIVO' AS valor_antigo,
        'ATIVO' AS valor_novo,
        novo.data_entrada_sociedade AS informacao_adicional,
        novo.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.socios antigo
    INNER JOIN tcc_receita.silver_intermediate.socios novo
        ON antigo.pk = novo.pk
       AND antigo.valid_until = novo.valid_since
    CROSS JOIN params p
    WHERE novo.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND antigo.vinculo_ativo = FALSE
      AND novo.vinculo_ativo = TRUE
),
-- 13. Mudança da qualificação societária, mantendo apenas relações ativas nos dois estados.
ev_socio_mudou_qualificacao AS (
    SELECT
        novo.pk,
        'SOCIO' AS entidade_tipo,
        'MUDOU QUALIFICACAO SOCIO' AS evento,
        antigo.qualificacao_socio AS valor_antigo,
        novo.qualificacao_socio AS valor_novo,
        novo.cnpj_basico AS informacao_adicional,
        novo.valid_since AS mes_referencia
    FROM tcc_receita.silver_intermediate.socios antigo
    INNER JOIN tcc_receita.silver_intermediate.socios novo
        ON antigo.pk = novo.pk
       AND antigo.valid_until = novo.valid_since
    CROSS JOIN params p
    WHERE novo.valid_since BETWEEN p.mes_ini AND p.mes_fim
      AND antigo.vinculo_ativo = TRUE
      AND novo.vinculo_ativo = TRUE
      AND antigo.qualificacao_socio IS DISTINCT FROM novo.qualificacao_socio
),
todos_eventos AS (
    SELECT * FROM ev_inicio_atividade_empresa
    UNION ALL SELECT * FROM ev_estado_cadastral
    UNION ALL SELECT * FROM ev_entrou_mei
    UNION ALL SELECT * FROM ev_saiu_mei
    UNION ALL SELECT * FROM ev_entrou_simples
    UNION ALL SELECT * FROM ev_saiu_simples
    UNION ALL SELECT * FROM ev_natureza_juridica
    UNION ALL SELECT * FROM ev_cnae_principal
    UNION ALL SELECT * FROM ev_cnae_secundario
    UNION ALL SELECT * FROM ev_socio_entrou
    UNION ALL SELECT * FROM ev_socio_saiu
    UNION ALL SELECT * FROM ev_socio_reentrou
    UNION ALL SELECT * FROM ev_socio_mudou_qualificacao
)

INSERT INTO tcc_receita.silver_intermediate.log_eventos_negocio
REPLACE WHERE mes_referencia >= '{{mes_referencia_inicial}}'
              AND mes_referencia <= '{{mes_referencia_final}}'
SELECT
    pk,
    entidade_tipo,
    evento,
    valor_antigo,
    valor_novo,
    informacao_adicional,
    mes_referencia,
    current_timestamp() AS gerado_em
FROM todos_eventos;
