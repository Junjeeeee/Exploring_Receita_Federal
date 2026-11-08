SELECT 
    data_referencia,
    categoria_macro_evento,
    SUM(CASE WHEN nome_cnae = :param_cnae THEN 1 ELSE 0 END) AS total_cnae_especifico,
    SUM(CASE WHEN classe_cnae = (SELECT MAX(nome_classe) FROM tcc_receita.silver_final.dim_cnae WHERE descricao_cnae = :param_cnae) THEN 1 ELSE 0 END) AS total_na_classe_cnae,
    COUNT(*) AS total_na_grande_area
FROM tcc_receita.gold.obt_dashboard_eventos
WHERE uf = :param_uf 
  AND municipio = :param_municipio
  AND grande_area_cnae = (SELECT MAX(grande_setor_economico) FROM tcc_receita.silver_final.dim_cnae WHERE descricao_cnae = :param_cnae)
GROUP BY ALL;