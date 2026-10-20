SELECT 
    data_referencia,
    categoria_macro_evento,
    porte_empresa,
    SUM(CASE WHEN uf = :param_uf AND municipio = :param_municipio AND bairro = :param_bairro THEN 1 ELSE 0 END) AS qtd_no_bairro,
    SUM(CASE WHEN uf = :param_uf AND municipio = :param_municipio THEN 1 ELSE 0 END) AS qtd_na_cidade,
    SUM(CASE WHEN uf = :param_uf THEN 1 ELSE 0 END) AS qtd_no_estado,
    COUNT(*) AS qtd_no_brasil
FROM tcc_receita.gold.obt_dashboard_eventos
WHERE nome_cnae = :param_cnae
GROUP BY ALL;