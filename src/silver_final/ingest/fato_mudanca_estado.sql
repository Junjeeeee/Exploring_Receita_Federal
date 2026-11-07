-- Silver Final | fato_mudanca_estado
-- A tabela relaciona cada evento à versão empresarial vigente antes/depois do mês.
-- Inclui eventos de empresa, estabelecimento, tributação e QSA; eventos de sócio
-- são associados à empresa pelo cnpj_basico da relação, sem interpretar a PK.
-- Substitua os dois tokens por literais YYYY-MM antes de executar.
-- Exemplo: {{mes_referencia_inicial}} -> 2023-05; {{mes_referencia_final}} -> 2023-08.

CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_final;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_final.fato_mudanca_estado (
    sk_mudanca_estado BIGINT COMMENT 'FK para dim_mudanca_estado.',
    sk_empresa_anterior BIGINT COMMENT 'FK da versão empresarial vigente imediatamente antes do evento; -1 se não encontrada.',
    sk_empresa_posterior BIGINT COMMENT 'FK da versão empresarial vigente no mês do evento; -1 se não encontrada.',
    sk_grupo_cnae_anterior BIGINT COMMENT 'FK do grupo CNAE associado à versão empresarial anterior; -1 se ausente.',
    sk_grupo_cnae_posterior BIGINT COMMENT 'FK do grupo CNAE associado à versão empresarial posterior; -1 se ausente.',
    sk_tempo BIGINT COMMENT 'FK para dim_tempo pelo mês do evento.',
    cnpj_raiz STRING COMMENT 'CNPJ raiz associado ao evento, resolvido por JOIN com Silver Intermediate.',
    mes_referencia STRING COMMENT 'Mês do evento (YYYY-MM); partição da fato.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de processamento na Silver Final.',
    entidade_tipo STRING COMMENT 'Granularidade da entidade que originou o evento.',
    pk_entidade_origem STRING COMMENT 'PK lógica original da entidade na Silver Intermediate.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Silver Final: fato mensal de eventos de negócio associado à versão da empresa antes/depois da ocorrência.';

WITH params AS (
    SELECT
        CAST('{{mes_referencia_inicial}}' AS STRING) AS mes_ini,
        CAST('{{mes_referencia_final}}' AS STRING) AS mes_fim
),
mapeamento_pk AS (
    SELECT DISTINCT 'EMPRESA' AS entidade_tipo, pk, cnpj_basico
    FROM tcc_receita.silver_intermediate.empresas
    WHERE pk IS NOT NULL AND cnpj_basico IS NOT NULL
    UNION
    SELECT DISTINCT 'TRIBUTARIO', pk, cnpj_basico
    FROM tcc_receita.silver_intermediate.dados_simples
    WHERE pk IS NOT NULL AND cnpj_basico IS NOT NULL
    UNION
    SELECT DISTINCT 'ESTABELECIMENTO', pk, cnpj_basico
    FROM tcc_receita.silver_intermediate.estabelecimentos
    WHERE pk IS NOT NULL AND cnpj_basico IS NOT NULL
    UNION
    SELECT DISTINCT 'SOCIO', pk, cnpj_basico
    FROM tcc_receita.silver_intermediate.socios
    WHERE pk IS NOT NULL AND cnpj_basico IS NOT NULL
),
logs_base AS (
    SELECT DISTINCT
        l.pk AS pk_entidade_origem,
        COALESCE(NULLIF(UPPER(TRIM(l.entidade_tipo)), ''), 'ENTIDADE_DESCONHECIDA') AS entidade_tipo,
        COALESCE(NULLIF(TRIM(l.evento), ''), 'EVENTO_DESCONHECIDO') AS evento,
        COALESCE(NULLIF(TRIM(l.valor_antigo), ''), 'NULO') AS valor_anterior,
        COALESCE(NULLIF(TRIM(l.valor_novo), ''), 'NULO') AS valor_posterior,
        COALESCE(NULLIF(TRIM(l.informacao_adicional), ''), 'SEM_DETALHE_ADICIONAL') AS informacao_adicional_norm,
        l.mes_referencia,
        m.cnpj_basico AS cnpj_raiz
    FROM tcc_receita.silver_intermediate.log_eventos_negocio l
    CROSS JOIN params p
    LEFT JOIN mapeamento_pk m
        ON m.entidade_tipo = UPPER(TRIM(l.entidade_tipo))
       AND m.pk = l.pk
    WHERE l.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
      AND l.pk IS NOT NULL
),
logs_com_dimensoes AS (
    SELECT
        l.*,
        dme.sk_mudanca_estado,
        dt.sk_tempo
    FROM logs_base l
    LEFT JOIN tcc_receita.silver_final.dim_mudanca_estado dme
        ON dme.entidade_tipo = l.entidade_tipo
       AND dme.tipo_mudanca_estado = l.evento
       AND dme.valor_anterior = l.valor_anterior
       AND dme.valor_posterior = l.valor_posterior
       AND COALESCE(dme.descricao_adicional, 'SEM_DETALHE_ADICIONAL') = l.informacao_adicional_norm
    LEFT JOIN tcc_receita.silver_final.dim_tempo dt
        ON dt.mes_referencia = l.mes_referencia
),
primeira_versao_empresa AS (
    SELECT cnpj_raiz, sk_empresa, sk_grupo_cnae
    FROM (
        SELECT
            cnpj_raiz, sk_empresa, sk_grupo_cnae,
            ROW_NUMBER() OVER (PARTITION BY cnpj_raiz ORDER BY inicio_vigencia ASC) AS rn
        FROM tcc_receita.silver_final.dim_empresa
        WHERE sk_empresa <> -1
    ) x
    WHERE rn = 1
),
versoes_pareadas AS (
    SELECT
        l.*,
        CASE
            WHEN l.evento = 'EMPRESA NASCEU' THEN CAST(-1 AS BIGINT)
            ELSE COALESCE(d_before.sk_empresa, CAST(-1 AS BIGINT))
        END AS sk_empresa_anterior,
        CASE
            WHEN l.evento = 'EMPRESA NASCEU'
                THEN COALESCE(d_after.sk_empresa, primeira.sk_empresa, CAST(-1 AS BIGINT))
            ELSE COALESCE(d_after.sk_empresa, primeira.sk_empresa, CAST(-1 AS BIGINT))
        END AS sk_empresa_posterior,
        CASE
            WHEN l.evento = 'EMPRESA NASCEU' THEN CAST(-1 AS BIGINT)
            ELSE COALESCE(d_before.sk_grupo_cnae, CAST(-1 AS BIGINT))
        END AS sk_grupo_cnae_anterior,
        COALESCE(d_after.sk_grupo_cnae, primeira.sk_grupo_cnae, CAST(-1 AS BIGINT)) AS sk_grupo_cnae_posterior
    FROM logs_com_dimensoes l
    LEFT JOIN tcc_receita.silver_final.dim_empresa d_after
        ON l.cnpj_raiz = d_after.cnpj_raiz
       AND l.mes_referencia >= d_after.inicio_vigencia
       AND l.mes_referencia < COALESCE(d_after.fim_vigencia, '9999-12')
       AND d_after.sk_empresa <> -1
    LEFT JOIN tcc_receita.silver_final.dim_empresa d_before
        ON l.cnpj_raiz = d_before.cnpj_raiz
       AND d_before.inicio_vigencia < l.mes_referencia
       AND (d_before.fim_vigencia IS NULL OR d_before.fim_vigencia >= l.mes_referencia)
       AND d_before.sk_empresa <> -1
    LEFT JOIN primeira_versao_empresa primeira
        ON l.cnpj_raiz = primeira.cnpj_raiz
),
resultado AS (
    SELECT DISTINCT
        COALESCE(sk_mudanca_estado, CAST(-1 AS BIGINT)) AS sk_mudanca_estado,
        sk_empresa_anterior,
        sk_empresa_posterior,
        sk_grupo_cnae_anterior,
        sk_grupo_cnae_posterior,
        COALESCE(sk_tempo, CAST(-1 AS BIGINT)) AS sk_tempo,
        cnpj_raiz,
        mes_referencia,
        CURRENT_TIMESTAMP() AS data_carga_etl,
        entidade_tipo,
        pk_entidade_origem
    FROM versoes_pareadas
)
INSERT INTO tcc_receita.silver_final.fato_mudanca_estado
REPLACE WHERE mes_referencia >= '{{mes_referencia_inicial}}'
              AND mes_referencia <= '{{mes_referencia_final}}'
SELECT
    sk_mudanca_estado,
    sk_empresa_anterior,
    sk_empresa_posterior,
    sk_grupo_cnae_anterior,
    sk_grupo_cnae_posterior,
    sk_tempo,
    cnpj_raiz,
    mes_referencia,
    data_carga_etl,
    entidade_tipo,
    pk_entidade_origem
FROM resultado;
