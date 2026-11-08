CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE OR REPLACE TABLE tcc_receita.gold.dashboard_mercado_ativo
COMMENT 'Camada Gold: Fotografia atual das empresas matrizes ativas para exploração de concorrência e ranking de capital social.'
AS
WITH ponte_primaria AS (
    SELECT sk_grupo_cnae, sk_cnae 
    FROM (
        SELECT sk_grupo_cnae, sk_cnae, ROW_NUMBER() OVER (PARTITION BY sk_grupo_cnae ORDER BY nk_cnae ASC) AS rn
        FROM tcc_receita.silver_final.ponte_cnae 
        WHERE cnae_principal = TRUE AND sk_grupo_cnae != -1
    )
    WHERE rn = 1
)
SELECT /*+ BROADCAST(cnae) */
    c.cnpj_raiz,
    c.cnpj_matriz,
    c.razao_social,
    c.nome_fantasia_matriz AS nome_fantasia,
    COALESCE(NULLIF(c.enquadramento_porte_analitico, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS porte_empresa,
    c.porte_cadastral,
    c.opcao_pelo_mei,
    c.opcao_pelo_simples,
    c.capital_social,
    c.data_inicio_atividade,
    c.uf,
    c.municipio,
    c.codigo_ibge_municipio,
    c.localidade_analitica AS bairro,
    c.bairro_campo_livre,
    c.latitude_cep,
    c.longitude_cep,
    cnae.codigo_cnae,
    cnae.descricao_cnae AS nome_cnae,
    cnae.nome_classe AS classe_cnae,
    cnae.nome_divisao AS divisao_cnae,
    cnae.grande_setor_economico AS grande_area_cnae,
    NOW() AS data_carga_etl
FROM tcc_receita.silver_final.dim_empresa c
LEFT JOIN ponte_primaria b 
    ON c.sk_grupo_cnae = b.sk_grupo_cnae
LEFT JOIN tcc_receita.silver_final.dim_cnae cnae 
    ON COALESCE(b.sk_cnae, -1) = cnae.sk_cnae
WHERE c.sk_empresa != -1
  AND c.registro_ativo = TRUE 
  AND c.situacao_cadastral_matriz = 'ATIVA';

OPTIMIZE tcc_receita.gold.dashboard_mercado_ativo ZORDER BY (uf, municipio, nome_cnae);