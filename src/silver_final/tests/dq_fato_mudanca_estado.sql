-- DQ fato_mudanca_estado.
-- As FKs são COALESCE(..., -1) e o -1 existe nas dimensões, então "FK nula/órfã" não pode falhar.
-- O que detecta falha real é a PROPORÇÃO de linhas que caíram em -1 (resolução que não funcionou).
WITH params AS (
    SELECT
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_fim
),
fato_janela AS (
    SELECT f.*
    FROM tcc_receita.silver_final.fato_mudanca_estado f
    CROSS JOIN params p
    WHERE f.mes_referencia >= p.mes_ini AND f.mes_referencia <= p.mes_fim
),
metricas AS (
    SELECT
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_mudanca_estado = -1 THEN 1 ELSE 0 END) AS qtd_sem_mudanca,
        SUM(CASE WHEN sk_tempo = -1 THEN 1 ELSE 0 END) AS qtd_sem_tempo,
        SUM(CASE WHEN cnpj_raiz IS NULL THEN 1 ELSE 0 END) AS qtd_sem_cnpj,
        SUM(CASE WHEN sk_empresa_posterior = -1 THEN 1 ELSE 0 END) AS qtd_sem_empresa_post,
        -- grão: um evento do log = (pk, mês, assinatura via sk, data, matriz); não pode repetir por pareamento de versões
        COUNT(DISTINCT CONCAT_WS('|', pk_entidade_origem, mes_referencia, CAST(sk_mudanca_estado AS STRING),
                                 COALESCE(CAST(data_evento AS STRING), ''), COALESCE(CAST(eh_matriz AS STRING), ''))) AS qtd_eventos_distintos
    FROM fato_janela
),
-- Reconciliação: mesma regra de distinção do fato (normalizações + DISTINCT + pk não nula).
log_distinto AS (
    SELECT COUNT(*) AS qtd
    FROM (
        SELECT DISTINCT
            l.pk,
            COALESCE(NULLIF(UPPER(TRIM(l.entidade_tipo)), ''), 'ENTIDADE_DESCONHECIDA') AS entidade_tipo,
            COALESCE(NULLIF(TRIM(l.evento), ''), 'EVENTO_DESCONHECIDO') AS evento,
            COALESCE(NULLIF(TRIM(l.valor_antigo), ''), 'NULO') AS valor_anterior,
            COALESCE(NULLIF(TRIM(l.valor_novo), ''), 'NULO') AS valor_posterior,
            COALESCE(NULLIF(TRIM(l.informacao_adicional), ''), 'SEM_DETALHE_ADICIONAL') AS informacao_adicional_norm,
            NULLIF(TRIM(l.codigo_motivo), '') AS codigo_motivo_norm,
            l.data_evento,
            l.eh_matriz,
            l.mes_referencia
        FROM tcc_receita.silver_intermediate.log_eventos_negocio l
        CROSS JOIN params p
        WHERE l.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
          AND l.pk IS NOT NULL
    ) x
)
SELECT
    ASSERT_TRUE(m.total_linhas > 0, 'ERRO DQ [fato_mudanca_estado]: Nenhum evento carregado para o período informado!'),
    ASSERT_TRUE(m.qtd_sem_mudanca = 0,
        CONCAT('ERRO DQ [fato_mudanca_estado]: ', m.qtd_sem_mudanca, ' de ', m.total_linhas, ' linhas com sk_mudanca_estado = -1 (assinatura não casou com a dimensão)!')),
    ASSERT_TRUE(m.qtd_sem_tempo = 0,
        CONCAT('ERRO DQ [fato_mudanca_estado]: ', m.qtd_sem_tempo, ' de ', m.total_linhas, ' linhas com sk_tempo = -1 (mês não existe em dim_tempo)!')),
    ASSERT_TRUE(m.qtd_sem_cnpj <= 0.001 * m.total_linhas,
        CONCAT('ERRO DQ [fato_mudanca_estado]: ', m.qtd_sem_cnpj, ' de ', m.total_linhas, ' linhas com cnpj_raiz NULL (limite 0,1%)!')),
    ASSERT_TRUE(m.qtd_sem_empresa_post <= 0.01 * m.total_linhas,
        CONCAT('ERRO DQ [fato_mudanca_estado]: ', m.qtd_sem_empresa_post, ' de ', m.total_linhas, ' linhas com sk_empresa_posterior = -1 (limite 1%)!')),
    ASSERT_TRUE(m.qtd_eventos_distintos = m.total_linhas,
        CONCAT('ERRO DQ [fato_mudanca_estado]: ', m.total_linhas, ' linhas para ', m.qtd_eventos_distintos, ' eventos distintos (duplicidade por pareamento de versões)!')),
    ASSERT_TRUE(m.total_linhas = ld.qtd,
        CONCAT('ERRO DQ [fato_mudanca_estado]: reconciliação falhou: ', m.total_linhas, ' linhas no fato x ', ld.qtd, ' linhas distintas no log!'))
FROM metricas m
CROSS JOIN log_distinto ld;
