SELECT 
    data_referencia,
    porte_empresa,
    tipo_evento,
    categoria_macro_evento,
    motivo_detalhe,
    grande_area_cnae,
    uf,
    COUNT(*) AS total_empresas
FROM tcc_receita.gold.obt_dashboard_eventos
WHERE porte_empresa IS NOT NULL
GROUP BY ALL;