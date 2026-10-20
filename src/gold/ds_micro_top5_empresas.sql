WITH base_cnae AS (
    SELECT 
        cnpj_matriz,
        razao_social,
        nome_fantasia,
        porte_empresa,
        capital_social,
        data_inicio_atividade,
        bairro,
        municipio,
        uf
    FROM tcc_receita.gold.dashboard_mercado_ativo
    WHERE nome_cnae = :param_cnae
),
top_bairro AS (
    SELECT '1. Top 5 no Bairro' AS recorte_geografico, *,
           ROW_NUMBER() OVER (ORDER BY capital_social DESC, data_inicio_atividade ASC) AS ranking
    FROM base_cnae
    WHERE uf = :param_uf AND municipio = :param_municipio AND bairro = :param_bairro
),
top_cidade AS (
    SELECT '2. Top 5 na Cidade' AS recorte_geografico, *,
           ROW_NUMBER() OVER (ORDER BY capital_social DESC, data_inicio_atividade ASC) AS ranking
    FROM base_cnae
    WHERE uf = :param_uf AND municipio = :param_municipio
),
top_estado AS (
    SELECT '3. Top 5 no Estado (UF)' AS recorte_geografico, *,
           ROW_NUMBER() OVER (ORDER BY capital_social DESC, data_inicio_atividade ASC) AS ranking
    FROM base_cnae
    WHERE uf = :param_uf
),
top_brasil AS (
    SELECT '4. Top 5 no Brasil' AS recorte_geografico, *,
           ROW_NUMBER() OVER (ORDER BY capital_social DESC, data_inicio_atividade ASC) AS ranking
    FROM base_cnae
)
SELECT recorte_geografico, ranking, razao_social, nome_fantasia, porte_empresa, capital_social, data_inicio_atividade, bairro, municipio, uf
FROM top_bairro WHERE ranking <= 5
UNION ALL
SELECT recorte_geografico, ranking, razao_social, nome_fantasia, porte_empresa, capital_social, data_inicio_atividade, bairro, municipio, uf
FROM top_cidade WHERE ranking <= 5
UNION ALL
SELECT recorte_geografico, ranking, razao_social, nome_fantasia, porte_empresa, capital_social, data_inicio_atividade, bairro, municipio, uf
FROM top_estado WHERE ranking <= 5
UNION ALL
SELECT recorte_geografico, ranking, razao_social, nome_fantasia, porte_empresa, capital_social, data_inicio_atividade, bairro, municipio, uf
FROM top_brasil WHERE ranking <= 5
ORDER BY recorte_geografico, ranking;