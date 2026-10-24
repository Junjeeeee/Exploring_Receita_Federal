WITH params AS (
    SELECT 
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_fim
),
metricas AS (
    SELECT 
        COUNT(*) AS total_eventos_janela,
        SUM(CASE WHEN l.pk IS NULL OR TRIM(l.pk) = '' THEN 1 ELSE 0 END) AS pks_nulas,
        SUM(CASE WHEN l.evento IS NULL OR l.valor_novo IS NULL THEN 1 ELSE 0 END) AS eventos_incompletos
    FROM tcc_receita.silver_intermediate.log_eventos_negocio l
    CROSS JOIN params p
    WHERE l.mes_referencia >= p.mes_ini
      AND l.mes_referencia <= p.mes_fim
)
SELECT 
    ASSERT_TRUE(total_eventos_janela > 0, 'ERRO DQ [log_eventos_negocio]: Nenhum evento gerado para o período!'),
    ASSERT_TRUE(pks_nulas = 0, CONCAT('ERRO DQ [log_eventos_negocio]: ', pks_nulas, ' eventos com PK nula!')),
    ASSERT_TRUE(eventos_incompletos = 0, CONCAT('ERRO DQ [log_eventos_negocio]: ', eventos_incompletos, ' eventos sem tipo ou valor_novo!'))
FROM metricas;