-- Gold | ds_coorte_sobrevivencia (view)
-- Grão: uma linha por empresa (cnpj_raiz) com data de nascimento conhecida.
-- Uso: curvas de Kaplan-Meier e regressão de Cox (sobrevivência com censura à direita).
-- Spec: docs/specs/features/2026-11-14-semantica-eventos-e-fluxos.md (seção 2.4) e docs/critica.md (C-01).
--
-- Regras:
-- - Nascimento = data_inicio_atividade da primeira versão observada.
-- - Evento terminal = primeira versão com situação BAIXADA (INAPTA/SUSPENSA são reversíveis, não contam).
--   Data da morte = data_situacao_cadastral dessa versão (data real declarada pela Receita). Fallback para o
--   mês de detecção (inicio_vigencia) só se a data faltar ou for anterior ao nascimento
--   (data_encerramento_estimada = TRUE).
-- - Censura = último mês com dados na dim_empresa (nunca CURRENT_DATE: o resultado não muda com o dia).
-- - Covariáveis = primeira versão observada (MIN_BY por inicio_vigencia). Nada olha o futuro.
-- - Tempo em meses de calendário (mês da morte/censura − mês do nascimento). Tempo <= 0 vira NULL e
--   tempo_invalido = TRUE (não é escondido com GREATEST).

CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE OR REPLACE VIEW tcc_receita.gold.ds_coorte_sobrevivencia
COMMENT 'Camada Gold: coortes de nascimento empresarial para Kaplan-Meier/Cox. Morte = primeira versão BAIXADA (data real da Receita); censura no último mês da dim_empresa; covariáveis da primeira observação.'
AS
WITH limites_dim AS (
    -- Baseline = primeiro mês da dimensão; último mês = fim do período observado (data de censura).
    SELECT
        MIN(inicio_vigencia) AS mes_baseline,
        MAX(inicio_vigencia) AS mes_ultimo
    FROM tcc_receita.silver_final.dim_empresa
    WHERE sk_empresa != -1
),
resumo_empresa AS (
    -- Uma linha por empresa. MIN_BY(col, inicio_vigencia) = valor da coluna na primeira versão.
    -- As colunas "_baixa" usam FILTER: só as versões BAIXADA entram na agregação.
    SELECT
        cnpj_raiz,
        MIN(inicio_vigencia) AS mes_entrada_observacao,
        MIN_BY(data_inicio_atividade, inicio_vigencia) AS data_nascimento,
        MIN_BY(porte_cadastral, inicio_vigencia) AS porte_inicial,
        MIN_BY(enquadramento_porte_analitico, inicio_vigencia) AS enquadramento_inicial,
        MIN_BY(opcao_pelo_mei, inicio_vigencia) AS mei_na_primeira_observacao,
        MIN_BY(opcao_pelo_simples, inicio_vigencia) AS simples_na_primeira_observacao,
        MIN_BY(uf, inicio_vigencia) AS uf,
        MIN_BY(municipio, inicio_vigencia) AS municipio,
        MIN_BY(sk_grupo_cnae, inicio_vigencia) AS sk_grupo_cnae_inicial,
        MAX_BY(situacao_cadastral_matriz, inicio_vigencia) AS situacao_atual,
        MIN(inicio_vigencia) FILTER (WHERE situacao_cadastral_matriz = 'BAIXADA') AS mes_primeiro_encerramento,
        MIN_BY(data_situacao_cadastral, inicio_vigencia) FILTER (WHERE situacao_cadastral_matriz = 'BAIXADA') AS data_situacao_primeira_baixa
    FROM tcc_receita.silver_final.dim_empresa
    WHERE sk_empresa != -1
    GROUP BY cnpj_raiz
),
encerramento AS (
    -- Data da morte: data real; se nula ou anterior ao nascimento, o 1º dia do mês de detecção (estimada).
    SELECT
        r.*,
        CASE
            WHEN r.mes_primeiro_encerramento IS NULL THEN CAST(NULL AS DATE)
            WHEN r.data_situacao_primeira_baixa IS NOT NULL
             AND r.data_situacao_primeira_baixa >= r.data_nascimento THEN r.data_situacao_primeira_baixa
            ELSE TO_DATE(CONCAT(r.mes_primeiro_encerramento, '-01'))
        END AS data_encerramento,
        CASE
            WHEN r.mes_primeiro_encerramento IS NULL THEN CAST(NULL AS BOOLEAN)
            ELSE NOT (r.data_situacao_primeira_baixa IS NOT NULL
                      AND r.data_situacao_primeira_baixa >= r.data_nascimento)
        END AS data_encerramento_estimada
    FROM resumo_empresa r
    WHERE r.data_nascimento IS NOT NULL
),
duracao AS (
    -- Fim da observação: data da morte (evento) ou último mês da dimensão (censura).
    SELECT
        e.*,
        l.mes_baseline,
        l.mes_ultimo AS mes_censura,
        CAST(MONTHS_BETWEEN(
            TRUNC(COALESCE(e.data_encerramento, TO_DATE(CONCAT(l.mes_ultimo, '-01'))), 'MM'),
            TRUNC(e.data_nascimento, 'MM')
        ) AS INT) AS tempo_bruto_meses
    FROM encerramento e
    CROSS JOIN limites_dim l
),
ponte_primaria AS (
    -- CNAE principal de cada grupo de CNAEs (um por grupo).
    SELECT sk_grupo_cnae, sk_cnae
    FROM (
        SELECT sk_grupo_cnae, sk_cnae, ROW_NUMBER() OVER (PARTITION BY sk_grupo_cnae ORDER BY nk_cnae ASC) AS rn
        FROM tcc_receita.silver_final.ponte_cnae
        WHERE cnae_principal = TRUE AND sk_grupo_cnae != -1
    )
    WHERE rn = 1
)
SELECT /*+ BROADCAST(cnae) */
    d.cnpj_raiz,
    d.data_nascimento,
    DATE_FORMAT(d.data_nascimento, 'yyyy-MM') AS coorte_mes_nascimento,
    YEAR(d.data_nascimento) AS coorte_ano_nascimento,
    d.porte_inicial,
    d.enquadramento_inicial,
    d.uf,
    d.municipio,
    cnae.codigo_cnae,
    cnae.descricao_cnae AS nome_cnae,
    cnae.grande_setor_economico AS grande_area_cnae,
    d.situacao_atual,
    -- Mês de DETECÇÃO da primeira versão BAIXADA (relógio do snapshot); a data real está em data_encerramento.
    d.mes_primeiro_encerramento,
    -- 1 = morte observada (primeira BAIXADA); 0 = censura à direita no último mês da dimensão.
    CASE WHEN d.mes_primeiro_encerramento IS NOT NULL THEN 1 ELSE 0 END AS evento_encerramento,
    CASE WHEN d.tempo_bruto_meses > 0 THEN d.tempo_bruto_meses END AS tempo_sobrevivencia_meses,
    -- Colunas novas (2026-11-14).
    d.mei_na_primeira_observacao,
    d.simples_na_primeira_observacao,
    d.data_encerramento,
    d.data_encerramento_estimada,
    d.mes_entrada_observacao,
    d.mes_censura,
    -- TRUE = nasceu depois do primeiro snapshot (coorte observada desde o nascimento, sem truncamento à esquerda).
    (DATE_FORMAT(d.data_nascimento, 'yyyy-MM') > d.mes_baseline) AS coorte_pos_baseline,
    -- TRUE = tempo <= 0 (morte no mês do nascimento ou datas incoerentes); tempo_sobrevivencia_meses fica NULL.
    COALESCE(d.tempo_bruto_meses <= 0, TRUE) AS tempo_invalido
FROM duracao d
LEFT JOIN ponte_primaria b ON d.sk_grupo_cnae_inicial = b.sk_grupo_cnae
LEFT JOIN tcc_receita.silver_final.dim_cnae cnae ON COALESCE(b.sk_cnae, -1) = cnae.sk_cnae;
