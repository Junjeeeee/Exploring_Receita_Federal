-- Gold | tabelas do dashboard no formato de cada gráfico (dash_*)
-- Spec: docs/specs/features/2026-11-16-otimizacao-log-fato-dashboard.md, §3.5.
--
-- Por que existe: no Lakeview, dataset com mais de 100 mil linhas não vai para o navegador; CADA clique (filtro,
-- widget) roda de novo a consulta do dataset no SQL warehouse. Os datasets das páginas 2, 4 e 5 faziam contas
-- pesadas (varrer o fato de todos os meses, agregar o mercado inteiro) a cada clique. Aqui o Job grava essas
-- contas uma vez; o dataset do dashboard passa a ser um SELECT simples destas tabelas.
--
-- Por que os números não mudam: dash_fluxos_mes e as três dash_mercado* são EXATAMENTE o SQL que o dataset
-- tinha, gravado em tabela (mesmas linhas → mesmas médias, medianas e somas para qualquer filtro). As outras três
-- (dash_eventos_mes, dash_baixas_idade_mes, dash_baixas_motivo_mes) somam os mesmos fatos num grão por mês sem
-- as colunas que nenhum widget usa; o dataset soma os meses (soma de somas = mesma soma).
--
-- Carga:
--   * dash_*_mes: só os meses da janela (INSERT ... REPLACE WHERE), como o fato. Na migração da fase 3 é
--     obrigatória uma carga com a janela inteira; o DQ de cobertura (dq_gold.sql) falha se faltar mês.
--   * dash_mercado*: fotografia do último mês (CREATE OR REPLACE), como dashboard_mercado_ativo.
-- LGPD: nenhuma tabela tem CNPJ, CPF ou endereço além de município/bairro. dash_mercado_empresas mostra razão social
-- só onde dashboard_mercado_ativo.exibir_razao_social permite (fora MEI, empresário individual e 11 dígitos).

CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

-- Parâmetros obrigatórios: os REPLACE WHERE abaixo precisam da janela explícita (parâmetro vazio falha aqui).
SELECT ASSERT_TRUE(
    '{{mes_referencia_inicial}}' RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
    AND '{{mes_referencia_final}}' RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
    AND '{{mes_referencia_inicial}}' <= '{{mes_referencia_final}}',
    'ERRO [dashboard_agregados]: informe mes_referencia_inicial e mes_referencia_final no formato YYYY-MM, com inicial <= final (recebido: [{{mes_referencia_inicial}}] a [{{mes_referencia_final}}]).'
);

-- =====================================================================================================
-- 1. dash_fluxos_mes — páginas 2 e 4 (percentuais de MEI, falência, idade na baixa; aberturas × baixas por porte)
--    Uma linha por empresa × mês × fluxo ('Aberturas' | 'Baixas'). Empresa que abre e já aparece BAIXADA
--    (D-17) gera DUAS linhas. Percentuais: colunas 0/100 (NULL = não se aplica), o widget tira a média (AVG).
--    Sem CNPJ: só os atributos usados pelos filtros e pelos indicadores.
-- =====================================================================================================
CREATE TABLE IF NOT EXISTS tcc_receita.gold.dash_fluxos_mes (
    mes_referencia STRING COMMENT 'Mês de detecção (YYYY-MM). Partição.',
    data_referencia DATE COMMENT 'Primeiro dia do mês.',
    uf STRING COMMENT 'UF da matriz.',
    porte_empresa STRING COMMENT 'Porte como em fato_empresa_mes (na baixa, o porte de antes da baixa).',
    grande_area_cnae STRING COMMENT 'Grande área do CNAE principal.',
    fluxo STRING COMMENT 'Aberturas ou Baixas.',
    qtd INT COMMENT 'Sempre 1 (o widget soma).',
    pct_mei_abertura INT COMMENT 'Aberturas: 100 se MEI, 0 se não; NULL nas baixas.',
    pct_mei_baixa INT COMMENT 'Baixas: 100 se MEI, 0 se não; NULL nas aberturas.',
    pct_falencia_baixa INT COMMENT 'Baixas: 100 se falência/liquidação judicial (motivos 05, 06, 10); NULL nas aberturas.',
    idade_baixa_anos DECIMAL(17,6) COMMENT 'Baixas: idade na baixa em anos (data da baixa − início de atividade); NULL = sem data.',
    pct_baixa_menos_2_anos INT COMMENT 'Baixas com idade conhecida: 100 se menos de 2 anos, 0 se não.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Gold (dashboard): aberturas e baixas de matrizes, uma linha por empresa × mês × fluxo, sem identificação. Fonte: fato_empresa_mes. Carga por janela de meses.';

INSERT INTO tcc_receita.gold.dash_fluxos_mes
REPLACE WHERE mes_referencia >= '{{mes_referencia_inicial}}'
          AND mes_referencia <= '{{mes_referencia_final}}'
SELECT
    f.mes_referencia,
    f.data_referencia,
    f.uf,
    f.porte_empresa,
    f.grande_area_cnae,
    t.fluxo,
    1 AS qtd,
    CASE WHEN t.fluxo = 'Aberturas' THEN IF(f.porte_empresa = 'MEI', 100, 0) END AS pct_mei_abertura,
    CASE WHEN t.fluxo = 'Baixas'    THEN IF(f.porte_empresa = 'MEI', 100, 0) END AS pct_mei_baixa,
    CASE WHEN t.fluxo = 'Baixas'    THEN IF(f.teve_falencia_mes, 100, 0) END     AS pct_falencia_baixa,
    -- Idade na baixa pela data real (data da baixa − início de atividade); NULL = sem data, fora da mediana.
    CASE WHEN t.fluxo = 'Baixas'    THEN f.idade_na_baixa_meses / 12.0 END AS idade_baixa_anos,
    CASE WHEN t.fluxo = 'Baixas' AND f.idade_na_baixa_meses IS NOT NULL
         THEN IF(f.idade_na_baixa_meses < 24, 100, 0) END AS pct_baixa_menos_2_anos
FROM tcc_receita.gold.fato_empresa_mes f
JOIN (SELECT 'Aberturas' AS fluxo UNION ALL SELECT 'Baixas') t
  ON (t.fluxo = 'Aberturas' AND f.teve_abertura_mes) OR (t.fluxo = 'Baixas' AND f.teve_baixa_mes)
WHERE f.mes_referencia >= '{{mes_referencia_inicial}}'
  AND f.mes_referencia <= '{{mes_referencia_final}}'
  -- 2023-05 (primeiro mês do fato) é a carga inicial: não tem abertura nem baixa por construção. O filtro deixa
  -- isso explícito (era o SEM_CARGA_INICIAL do dataset).
  AND f.mes_referencia > (SELECT MIN(mes_referencia) FROM tcc_receita.gold.fato_empresa_mes)
  AND (f.teve_abertura_mes OR f.teve_baixa_mes);

-- =====================================================================================================
-- 2. dash_eventos_mes — página 4, "O que mais muda no cadastro" (todas as mudanças da OBT, por tipo)
-- =====================================================================================================
CREATE TABLE IF NOT EXISTS tcc_receita.gold.dash_eventos_mes (
    mes_referencia STRING COMMENT 'Mês de detecção (YYYY-MM). Partição.',
    tipo_evento STRING COMMENT 'Tipo do evento (como na OBT).',
    uf STRING COMMENT 'UF (como na OBT).',
    porte_empresa STRING COMMENT 'Porte (como na OBT).',
    grande_area_cnae STRING COMMENT 'Grande área (como na OBT).',
    qtd_eventos BIGINT COMMENT 'Número de eventos.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Gold (dashboard): eventos da OBT por mês × tipo × UF × porte × grande área. Carga por janela de meses.';

INSERT INTO tcc_receita.gold.dash_eventos_mes
REPLACE WHERE mes_referencia >= '{{mes_referencia_inicial}}'
          AND mes_referencia <= '{{mes_referencia_final}}'
SELECT mes_referencia, tipo_evento, uf, porte_empresa, grande_area_cnae, COUNT(*) AS qtd_eventos
FROM tcc_receita.gold.obt_dashboard_eventos
WHERE mes_referencia >= '{{mes_referencia_inicial}}'
  AND mes_referencia <= '{{mes_referencia_final}}'
GROUP BY mes_referencia, tipo_evento, uf, porte_empresa, grande_area_cnae;

-- =====================================================================================================
-- 3. dash_baixas_idade_mes — página 4, "Com que idade as empresas fecham?"
-- =====================================================================================================
CREATE TABLE IF NOT EXISTS tcc_receita.gold.dash_baixas_idade_mes (
    mes_referencia STRING COMMENT 'Mês de detecção (YYYY-MM). Partição.',
    uf STRING COMMENT 'UF da matriz.',
    porte_empresa STRING COMMENT 'Porte como em fato_empresa_mes.',
    grande_area_cnae STRING COMMENT 'Grande área do CNAE principal.',
    faixa_idade_baixa STRING COMMENT 'Faixa de idade na baixa (como em fato_empresa_mes).',
    qtd_baixas BIGINT COMMENT 'Número de baixas.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Gold (dashboard): baixas de matrizes por mês × UF × porte × grande área × faixa de idade. Carga por janela de meses.';

INSERT INTO tcc_receita.gold.dash_baixas_idade_mes
REPLACE WHERE mes_referencia >= '{{mes_referencia_inicial}}'
          AND mes_referencia <= '{{mes_referencia_final}}'
SELECT mes_referencia, uf, porte_empresa, grande_area_cnae, faixa_idade_baixa, COUNT(*) AS qtd_baixas
FROM tcc_receita.gold.fato_empresa_mes
WHERE mes_referencia >= '{{mes_referencia_inicial}}'
  AND mes_referencia <= '{{mes_referencia_final}}'
  AND teve_baixa_mes
GROUP BY mes_referencia, uf, porte_empresa, grande_area_cnae, faixa_idade_baixa;

-- =====================================================================================================
-- 4. dash_baixas_motivo_mes — página 4, "Por que as empresas são baixadas?"
--    População e atributos (UF, porte, área) do FATO (teve_baixa_mes: mesmo porte de baixa dos outros widgets,
--    inclusive o MEI que já aparece BAIXADA — D-17b); da OBT vem só a categoria do motivo (regra única em
--    dim_mudanca_estado), pelo mesmo critério: matriz que termina BAIXADA sem estar BAIXADA antes (anterior
--    INEXISTENTE = primeira aparição, D-17). Se a OBT não tiver o evento, a baixa aparece como NAO IDENTIFICADO
--    em vez de sumir. Uma categoria por empresa e mês. Os dois lados fecham (dq_gold.sql, 2b).
-- =====================================================================================================
CREATE TABLE IF NOT EXISTS tcc_receita.gold.dash_baixas_motivo_mes (
    mes_referencia STRING COMMENT 'Mês de detecção (YYYY-MM). Partição.',
    uf STRING COMMENT 'UF da matriz.',
    porte_empresa STRING COMMENT 'Porte como em fato_empresa_mes.',
    grande_area_cnae STRING COMMENT 'Grande área do CNAE principal.',
    categoria_macro_evento STRING COMMENT 'Categoria do motivo da baixa (da OBT); NAO IDENTIFICADO se a OBT não tem o evento.',
    qtd_empresas BIGINT COMMENT 'Número de matrizes baixadas.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Gold (dashboard): baixas de matrizes por mês × UF × porte × grande área × categoria do motivo. Carga por janela de meses.';

INSERT INTO tcc_receita.gold.dash_baixas_motivo_mes
REPLACE WHERE mes_referencia >= '{{mes_referencia_inicial}}'
          AND mes_referencia <= '{{mes_referencia_final}}'
WITH motivo AS (
    SELECT mes_referencia, cnpj_raiz, MAX(categoria_macro_evento) AS categoria_macro_evento
    FROM tcc_receita.gold.obt_dashboard_eventos
    WHERE mes_referencia >= '{{mes_referencia_inicial}}'
      AND mes_referencia <= '{{mes_referencia_final}}'
      AND tipo_evento = 'MUDOU ESTADO CADASTRAL'
      AND eh_matriz = TRUE
      AND situacao_cadastral_posterior = 'BAIXADA'
      AND situacao_cadastral_anterior <> 'BAIXADA'
    GROUP BY mes_referencia, cnpj_raiz
)
SELECT
    f.mes_referencia,
    f.uf,
    f.porte_empresa,
    f.grande_area_cnae,
    COALESCE(m.categoria_macro_evento, 'NAO IDENTIFICADO') AS categoria_macro_evento,
    COUNT(*) AS qtd_empresas
FROM tcc_receita.gold.fato_empresa_mes f
LEFT JOIN motivo m
  ON m.mes_referencia = f.mes_referencia
 AND m.cnpj_raiz = f.cnpj_raiz
WHERE f.mes_referencia >= '{{mes_referencia_inicial}}'
  AND f.mes_referencia <= '{{mes_referencia_final}}'
  AND f.teve_baixa_mes
GROUP BY f.mes_referencia, f.uf, f.porte_empresa, f.grande_area_cnae, COALESCE(m.categoria_macro_evento, 'NAO IDENTIFICADO');

-- =====================================================================================================
-- 5. dash_mercado — página 5 (contador de ativas, barras por porte e por setor)
--    Grão = o cruzamento dos quatro filtros da página (UF, município, bairro, CNAE) com porte e área (U6: o
--    cruzamento bairro × CNAE é mantido). Não cresce com os meses: é a fotografia do último cadastro.
--    Bairro nulo ou vazio vira 'NAO INFORMADO' (mesma regra nas três dash_mercado*, para o filtro casar).
--    CLUSTER BY (uf, municipio): os filtros da página começam por UF e município; o warehouse lê menos arquivos.
-- =====================================================================================================
CREATE OR REPLACE TABLE tcc_receita.gold.dash_mercado
CLUSTER BY (uf, municipio)
COMMENT 'Camada Gold (dashboard): matrizes ativas no último cadastro por UF × município × bairro × porte × grande área × CNAE, sem identificação.'
AS
SELECT
    uf,
    municipio,
    COALESCE(NULLIF(TRIM(bairro), ''), 'NAO INFORMADO') AS bairro,
    porte_empresa,
    grande_area_cnae,
    nome_cnae,
    COUNT(*) AS empresas_ativas
FROM tcc_receita.gold.dashboard_mercado_ativo
GROUP BY ALL;

-- Sem ZORDER (falha com liquid clustering): OPTIMIZE simples reagrupa pelos campos do CLUSTER BY.
OPTIMIZE tcc_receita.gold.dash_mercado;

-- =====================================================================================================
-- 6. dash_mercado_bairros — página 5, "Em quais bairros essas empresas estão?"
--    Grão bairro × CNAE, restrito aos 20 bairros com mais empresas ativas de cada município (o widget de barras
--    não tem top-N). Como o grão tem CNAE e bairro, os quatro filtros da página valem aqui.
-- =====================================================================================================
CREATE OR REPLACE TABLE tcc_receita.gold.dash_mercado_bairros
COMMENT 'Camada Gold (dashboard): matrizes ativas por bairro × CNAE nos 20 maiores bairros de cada município, sem identificação.'
AS
WITH por_bairro_cnae AS (
    SELECT uf, municipio, COALESCE(NULLIF(TRIM(bairro), ''), 'NAO INFORMADO') AS bairro, nome_cnae,
           COUNT(*) AS empresas_ativas
    FROM tcc_receita.gold.dashboard_mercado_ativo
    GROUP BY ALL
),
ranking_bairros AS (
    -- Posição do bairro no município pelo total de ativas (todos os CNAEs).
    SELECT
        uf, municipio, bairro,
        ROW_NUMBER() OVER (PARTITION BY uf, municipio ORDER BY SUM(empresas_ativas) DESC, bairro) AS posicao
    FROM por_bairro_cnae
    GROUP BY uf, municipio, bairro
)
SELECT c.uf, c.municipio, c.bairro, c.nome_cnae, c.empresas_ativas
FROM por_bairro_cnae c
JOIN ranking_bairros r
  ON r.uf = c.uf AND r.municipio = c.municipio AND r.bairro = c.bairro
WHERE r.posicao <= 20;

-- =====================================================================================================
-- 7. dash_mercado_empresas — página 5, tabela das 1.000 maiores por capital social (razão social liberada)
--    LGPD: só onde exibir_razao_social = TRUE. Sem endereço além de município/bairro.
-- =====================================================================================================
CREATE OR REPLACE TABLE tcc_receita.gold.dash_mercado_empresas
COMMENT 'Camada Gold (dashboard): as 1.000 matrizes ativas de maior capital social cuja razão social pode ser exibida (LGPD).'
AS
SELECT razao_social, nome_fantasia, porte_empresa, capital_social, data_inicio_atividade,
       uf, municipio, COALESCE(NULLIF(TRIM(bairro), ''), 'NAO INFORMADO') AS bairro, grande_area_cnae, nome_cnae
FROM tcc_receita.gold.dashboard_mercado_ativo
WHERE exibir_razao_social
ORDER BY capital_social DESC
LIMIT 1000;
