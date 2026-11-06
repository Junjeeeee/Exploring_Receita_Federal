CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE OR REPLACE VIEW tcc_receita.gold.ds_coorte_sobrevivencia
COMMENT 'Camada Gold: Dataset de coortes de nascimento empresarial com indicador de censura à direita para curvas de Kaplan-Meier e Regressão de Cox.'
AS
WITH resumo_empresa AS (
    SELECT 
        cnpj_raiz,
        MIN(data_inicio_atividade) AS data_nascimento,
        DATE_FORMAT(MIN(data_inicio_atividade), 'yyyy-MM') AS coorte_mes_nascimento,
        YEAR(MIN(data_inicio_atividade)) AS coorte_ano_nascimento,
        FIRST(porte_cadastral) AS porte_inicial,
        FIRST(enquadramento_porte_analitico) AS enquadramento_inicial,
        MAX(CAST(opcao_pelo_mei AS INT)) AS ja_foi_mei,
        MAX(CAST(opcao_pelo_simples AS INT)) AS ja_foi_simples,
        FIRST(uf) AS uf,
        FIRST(municipio) AS municipio,
        FIRST(sk_grupo_cnae) AS sk_grupo_cnae_inicial,
        MIN(CASE WHEN situacao_cadastral_matriz IN ('BAIXADA', 'INAPTA', 'NULA') THEN inicio_vigencia ELSE NULL END) AS mes_primeiro_encerramento,
        MAX(CASE WHEN registro_ativo = TRUE THEN situacao_cadastral_matriz ELSE NULL END) AS situacao_atual,
        MAX(COALESCE(fim_vigencia, DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM'))) AS ultimo_mes_observado
    FROM (
        SELECT * FROM tcc_receita.silver_final.dim_empresa 
        WHERE sk_empresa != -1
        DISTRIBUTE BY cnpj_raiz SORT BY inicio_vigencia ASC
    )
    GROUP BY cnpj_raiz
),
ponte_primaria AS (
    SELECT sk_grupo_cnae, sk_cnae 
    FROM (
        SELECT sk_grupo_cnae, sk_cnae, ROW_NUMBER() OVER (PARTITION BY sk_grupo_cnae ORDER BY nk_cnae ASC) AS rn
        FROM tcc_receita.silver_final.ponte_cnae 
        WHERE cnae_principal = TRUE AND sk_grupo_cnae != -1
    )
    WHERE rn = 1
)
SELECT 
    r.cnpj_raiz,
    r.data_nascimento,
    r.coorte_mes_nascimento,
    r.coorte_ano_nascimento,
    r.porte_inicial,
    r.enquadramento_inicial,
    r.ja_foi_mei,
    r.ja_foi_simples,
    r.uf,
    r.municipio,
    cnae.codigo_cnae,
    cnae.descricao_cnae AS nome_cnae,
    cnae.grande_setor_economico AS grande_area_cnae,
    r.situacao_atual,
    r.mes_primeiro_encerramento,
    -- 1 = Falha observada (encerramento/inaptidão); 0 = Censura à direita (continua ativa ao fim do estudo)
    CASE WHEN r.mes_primeiro_encerramento IS NOT NULL THEN 1 ELSE 0 END AS evento_encerramento,
    GREATEST(
        1,
        CAST(FLOOR(MONTHS_BETWEEN(
            TO_DATE(CONCAT(COALESCE(r.mes_primeiro_encerramento, r.ultimo_mes_observado), '-01'), 'yyyy-MM-dd'),
            TRUNC(r.data_nascimento, 'MM')
        )) AS INT)
    ) AS tempo_sobrevivencia_meses
FROM resumo_empresa r
LEFT JOIN ponte_primaria b ON r.sk_grupo_cnae_inicial = b.sk_grupo_cnae
LEFT JOIN tcc_receita.silver_final.dim_cnae cnae ON COALESCE(b.sk_cnae, -1) = cnae.sk_cnae
WHERE r.data_nascimento IS NOT NULL;