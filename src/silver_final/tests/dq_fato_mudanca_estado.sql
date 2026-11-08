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
        SUM(CASE WHEN sk_mudanca_estado IS NULL 
                   OR sk_empresa_anterior IS NULL 
                   OR sk_empresa_posterior IS NULL 
                   OR sk_grupo_cnae_anterior IS NULL 
                   OR sk_grupo_cnae_posterior IS NULL 
                   OR sk_tempo IS NULL THEN 1 ELSE 0 END) AS fks_nulas
    FROM fato_janela
),
orfaos_emp_post AS (
    SELECT COUNT(*) AS qtd
    FROM fato_janela f
    LEFT ANTI JOIN tcc_receita.silver_final.dim_empresa d ON f.sk_empresa_posterior = d.sk_empresa
),
orfaos_emp_ant AS (
    SELECT COUNT(*) AS qtd
    FROM fato_janela f
    LEFT ANTI JOIN tcc_receita.silver_final.dim_empresa d ON f.sk_empresa_anterior = d.sk_empresa
),
orfaos_cnae_post AS (
    SELECT COUNT(*) AS qtd
    FROM fato_janela f
    LEFT ANTI JOIN tcc_receita.silver_final.dim_grupo_cnae d ON f.sk_grupo_cnae_posterior = d.sk_grupo_cnae
),
orfaos_cnae_ant AS (
    SELECT COUNT(*) AS qtd
    FROM fato_janela f
    LEFT ANTI JOIN tcc_receita.silver_final.dim_grupo_cnae d ON f.sk_grupo_cnae_anterior = d.sk_grupo_cnae
),
orfaos_mudanca AS (
    SELECT COUNT(*) AS qtd
    FROM fato_janela f
    LEFT ANTI JOIN tcc_receita.silver_final.dim_mudanca_estado d ON f.sk_mudanca_estado = d.sk_mudanca_estado
),
orfaos_tempo AS (
    SELECT COUNT(*) AS qtd
    FROM fato_janela f
    LEFT ANTI JOIN tcc_receita.silver_final.dim_tempo d ON f.sk_tempo = d.sk_tempo
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 0, 'ERRO DQ [fato_mudanca_estado]: Nenhum evento carregado para o período informado!'),
    ASSERT_TRUE(m.fks_nulas = 0, CONCAT('ERRO DQ [fato_mudanca_estado]: ', m.fks_nulas, ' chaves estrangeiras nulas encontradas!')),
    ASSERT_TRUE(oep.qtd = 0, CONCAT('ERRO DQ [fato_mudanca_estado]: ', oep.qtd, ' sk_empresa_posterior órfãs em relação à dim_empresa!')),
    ASSERT_TRUE(oea.qtd = 0, CONCAT('ERRO DQ [fato_mudanca_estado]: ', oea.qtd, ' sk_empresa_anterior órfãs em relação à dim_empresa!')),
    ASSERT_TRUE(ocp.qtd = 0, CONCAT('ERRO DQ [fato_mudanca_estado]: ', ocp.qtd, ' sk_grupo_cnae_posterior órfãs em relação à dim_grupo_cnae!')),
    ASSERT_TRUE(oca.qtd = 0, CONCAT('ERRO DQ [fato_mudanca_estado]: ', oca.qtd, ' sk_grupo_cnae_anterior órfãs em relação à dim_grupo_cnae!')),
    ASSERT_TRUE(om.qtd = 0, CONCAT('ERRO DQ [fato_mudanca_estado]: ', om.qtd, ' sk_mudanca_estado órfãs em relação à dim_mudanca_estado!')),
    ASSERT_TRUE(ot.qtd = 0, CONCAT('ERRO DQ [fato_mudanca_estado]: ', ot.qtd, ' sk_tempo órfãs em relação à dim_tempo!'))
FROM metricas m 
CROSS JOIN orfaos_emp_post oep
CROSS JOIN orfaos_emp_ant oea
CROSS JOIN orfaos_cnae_post ocp
CROSS JOIN orfaos_cnae_ant oca
CROSS JOIN orfaos_mudanca om
CROSS JOIN orfaos_tempo ot;