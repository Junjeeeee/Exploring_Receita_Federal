WITH params AS (
    SELECT 
        CAST(REGEXP_REPLACE(COALESCE(NULLIF(TRIM({{mes_referencia_inicial}}), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')), '-', '') AS INT) AS nk_ini,
        CAST(REGEXP_REPLACE(COALESCE(NULLIF(TRIM({{mes_referencia_final}}), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')), '-', '') AS INT) AS nk_fim
),
fato_janela AS (
    SELECT f.*
    FROM tcc_receita.silver_final.fato_mudanca_estado f
    INNER JOIN tcc_receita.silver_final.dim_tempo t ON f.sk_tempo = t.sk_tempo
    CROSS JOIN params p
    WHERE t.nk_tempo >= p.nk_ini AND t.nk_tempo <= p.nk_fim
),
metricas AS (
    SELECT 
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_mudanca_estado IS NULL 
                   OR sk_empresa_anterior IS NULL 
                   OR sk_empresa_posterior IS NULL 
                   OR sk_grupo_cnae_anterior IS NULL 
                   OR sk_grupo_cnae_posterior IS NULL 
                   OR sk_tempo IS NULL THEN 1 ELSE 0 END) AS fks_nulas,
        SUM(CASE WHEN sk_empresa_posterior = -1 THEN 1 ELSE 0 END) AS posterior_invalido
    FROM fato_janela
),
orfaos_empresa AS (
    SELECT COUNT(*) AS qtd_orfaos_emp
    FROM fato_janela f
    LEFT ANTI JOIN tcc_receita.silver_final.dim_empresa d ON f.sk_empresa_posterior = d.sk_empresa
),
orfaos_mudanca AS (
    SELECT COUNT(*) AS qtd_orfaos_mud
    FROM fato_janela f
    LEFT ANTI JOIN tcc_receita.silver_final.dim_mudanca_estado d ON f.sk_mudanca_estado = d.sk_mudanca_estado
)
SELECT 
    ASSERT_TRUE(m.total_linhas > 0, 'ERRO DQ [fato_mudanca_estado]: Nenhum evento carregado para o período informado!'),
    ASSERT_TRUE(m.fks_nulas = 0, CONCAT('ERRO DQ [fato_mudanca_estado]: ', m.fks_nulas, ' chaves estrangeiras nulas encontradas (deveriam usar -1)!')),
    ASSERT_TRUE(m.posterior_invalido = 0, CONCAT('ERRO DQ [fato_mudanca_estado]: ', m.posterior_invalido, ' registos com sk_empresa_posterior = -1!')),
    ASSERT_TRUE(oe.qtd_orfaos_emp = 0, CONCAT('ERRO DQ [fato_mudanca_estado]: ', oe.qtd_orfaos_emp, ' FKs órfãs em relação à dim_empresa!')),
    ASSERT_TRUE(om.qtd_orfaos_mud = 0, CONCAT('ERRO DQ [fato_mudanca_estado]: ', om.qtd_orfaos_mud, ' FKs órfãs em relação à dim_mudanca_estado!'))
FROM metricas m 
CROSS JOIN orfaos_empresa oe 
CROSS JOIN orfaos_mudanca om;