CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

-- Remove a view antiga com nome enganoso, se existir
DROP VIEW IF EXISTS tcc_receita.gold.vw_dashboard_eventos_cnae_secundario;

CREATE OR REPLACE VIEW tcc_receita.gold.vw_dashboard_eventos_cnae
COMMENT 'Camada Gold: View de eventos expandida por todos os CNAEs (principais e secundários, anteriores e posteriores). ATENÇÃO: Para contar empresas ou eventos sem duplicação por múltiplos CNAEs, utilize COUNT(DISTINCT cnpj_raiz).'
AS
WITH cnaes_posteriores AS (
    SELECT 
        f.mes_referencia,
        t.data_referencia,
        sc.tipo_mudanca_estado AS tipo_evento,
        sc.detalhe_mudanca_estado AS transicao_detalhe,
        f.cnpj_raiz,
        c_post.razao_social,
        COALESCE(NULLIF(c_post.enquadramento_porte_analitico, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS porte_empresa,
        COALESCE(NULLIF(c_post.uf, 'NA'), 'NI') AS uf,
        COALESCE(NULLIF(c_post.municipio, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS municipio,
        COALESCE(NULLIF(c_post.localidade_analitica, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS bairro,
        'POSTERIOR_AO_EVENTO' AS momento_cnae,
        b.cnae_principal,
        cnae.codigo_cnae,
        cnae.descricao_cnae AS nome_cnae,
        cnae.nome_classe AS classe_cnae,
        cnae.grande_setor_economico AS grande_area_cnae
    FROM tcc_receita.silver_final.fato_mudanca_estado f
    INNER JOIN tcc_receita.silver_final.dim_tempo t ON f.sk_tempo = t.sk_tempo
    INNER JOIN tcc_receita.silver_final.dim_mudanca_estado sc ON f.sk_mudanca_estado = sc.sk_mudanca_estado
    LEFT JOIN tcc_receita.silver_final.dim_empresa c_post ON f.sk_empresa_posterior = c_post.sk_empresa
    INNER JOIN tcc_receita.silver_final.ponte_cnae b ON f.sk_grupo_cnae_posterior = b.sk_grupo_cnae
    INNER JOIN tcc_receita.silver_final.dim_cnae cnae ON b.sk_cnae = cnae.sk_cnae
    WHERE f.sk_grupo_cnae_posterior != -1
),
cnaes_anteriores AS (
    SELECT 
        f.mes_referencia,
        t.data_referencia,
        sc.tipo_mudanca_estado AS tipo_evento,
        sc.detalhe_mudanca_estado AS transicao_detalhe,
        f.cnpj_raiz,
        c_ant.razao_social,
        COALESCE(NULLIF(c_ant.enquadramento_porte_analitico, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS porte_empresa,
        COALESCE(NULLIF(c_ant.uf, 'NA'), 'NI') AS uf,
        COALESCE(NULLIF(c_ant.municipio, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS municipio,
        COALESCE(NULLIF(c_ant.localidade_analitica, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS bairro,
        'ANTERIOR_AO_EVENTO' AS momento_cnae,
        b.cnae_principal,
        cnae.codigo_cnae,
        cnae.descricao_cnae AS nome_cnae,
        cnae.nome_classe AS classe_cnae,
        cnae.grande_setor_economico AS grande_area_cnae
    FROM tcc_receita.silver_final.fato_mudanca_estado f
    INNER JOIN tcc_receita.silver_final.dim_tempo t ON f.sk_tempo = t.sk_tempo
    INNER JOIN tcc_receita.silver_final.dim_mudanca_estado sc ON f.sk_mudanca_estado = sc.sk_mudanca_estado
    LEFT JOIN tcc_receita.silver_final.dim_empresa c_ant ON f.sk_empresa_anterior = c_ant.sk_empresa
    INNER JOIN tcc_receita.silver_final.ponte_cnae b ON f.sk_grupo_cnae_anterior = b.sk_grupo_cnae
    INNER JOIN tcc_receita.silver_final.dim_cnae cnae ON b.sk_cnae = cnae.sk_cnae
    WHERE f.sk_grupo_cnae_anterior != -1
)
SELECT * FROM cnaes_posteriores
UNION ALL
SELECT * FROM cnaes_anteriores;