WITH params AS (
    SELECT 
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_fim
),
por_evento AS (
    SELECT
        l.evento,
        COUNT(*) AS total,
        SUM(CASE WHEN l.pk IS NULL OR TRIM(l.pk) = '' THEN 1 ELSE 0 END) AS pks_nulas,
        -- valor_novo NULL é legítimo quando o estabelecimento remove TODOS os CNAEs secundários
        -- (lista anterior preenchida → vazia). Nos demais eventos o novo estado é sempre definido.
        SUM(CASE WHEN l.evento IS NULL
                   OR (l.valor_novo IS NULL
                       AND NOT (l.evento = 'MUDOU LISTA CNAE SECUNDARIO' AND l.valor_antigo IS NOT NULL))
                 THEN 1 ELSE 0 END) AS incompletos
    FROM tcc_receita.silver_intermediate.log_eventos_negocio l
    CROSS JOIN params p
    WHERE l.mes_referencia >= p.mes_ini
      AND l.mes_referencia <= p.mes_fim
    GROUP BY l.evento
),
metricas AS (
    SELECT
        SUM(total) AS total_eventos_janela,
        SUM(pks_nulas) AS pks_nulas,
        SUM(incompletos) AS eventos_incompletos,
        -- Detalhe por tipo, para o erro dizer de onde vem o problema.
        CONCAT_WS(', ', COLLECT_LIST(CASE WHEN incompletos > 0
                                          THEN CONCAT(COALESCE(evento, 'NULL'), '=', incompletos) END)) AS detalhe
    FROM por_evento
)
SELECT 
    ASSERT_TRUE(total_eventos_janela > 0, 'ERRO DQ [log_eventos_negocio]: Nenhum evento gerado para o período!'),
    ASSERT_TRUE(pks_nulas = 0, CONCAT('ERRO DQ [log_eventos_negocio]: ', pks_nulas, ' eventos com PK nula!')),
    ASSERT_TRUE(eventos_incompletos = 0, CONCAT('ERRO DQ [log_eventos_negocio]: ', eventos_incompletos, ' eventos sem tipo ou valor_novo! Por tipo: ', detalhe))
FROM metricas;