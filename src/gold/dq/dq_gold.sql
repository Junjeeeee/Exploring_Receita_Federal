WITH params AS (
    SELECT 
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_fim
),
contagem_fato AS (
    SELECT COUNT(*) AS qtd_fato
    FROM tcc_receita.silver_final.fato_mudanca_estado f
    CROSS JOIN params p
    WHERE f.mes_referencia >= p.mes_ini AND f.mes_referencia <= p.mes_fim
),
contagem_obt AS (
    SELECT 
        COUNT(*) AS qtd_obt,
        SUM(CASE WHEN porte_empresa IS NULL OR uf IS NULL OR codigo_cnae IS NULL THEN 1 ELSE 0 END) AS qtd_nulos_obt
    FROM tcc_receita.gold.obt_dashboard_eventos o
    CROSS JOIN params p
    WHERE o.mes_referencia >= p.mes_ini AND o.mes_referencia <= p.mes_fim
),
metricas_estoque AS (
    SELECT 
        COUNT(*) AS total_estoque,
        COUNT(DISTINCT CONCAT_WS('|', cnpj_raiz, mes_referencia)) AS total_distinto_estoque
    FROM tcc_receita.gold.fato_empresa_mes e
    CROSS JOIN params p
    WHERE e.mes_referencia >= p.mes_ini AND e.mes_referencia <= p.mes_fim
)
SELECT 
    ASSERT_TRUE(cf.qtd_fato = co.qtd_obt, CONCAT('ERRO DQ [Gold OBT]: Divergência de linhas entre fato_mudanca_estado (', cf.qtd_fato, ') e obt_dashboard_eventos (', co.qtd_obt, ')!')),
    ASSERT_TRUE(co.qtd_nulos_obt = 0, CONCAT('ERRO DQ [Gold OBT]: ', co.qtd_nulos_obt, ' registos com atributos críticos nulos (deveriam usar NAO IDENTIFICADO)!')),
    ASSERT_TRUE(me.total_estoque > 0, 'ERRO DQ [fato_empresa_mes]: Estoque mensal vazio para a janela!'),
    ASSERT_TRUE(me.total_estoque = me.total_distinto_estoque, 'ERRO DQ [fato_empresa_mes]: Duplicidade de (cnpj_raiz, mes_referencia) no estoque mensal!')
FROM contagem_fato cf
CROSS JOIN contagem_obt co
CROSS JOIN metricas_estoque me;