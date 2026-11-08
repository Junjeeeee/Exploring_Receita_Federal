CREATE SCHEMA IF NOT EXISTS tcc_receita.gold;

CREATE TABLE IF NOT EXISTS tcc_receita.gold.obt_dashboard_eventos (
    mes_referencia STRING COMMENT 'Partição mensal do evento (YYYY-MM).',
    data_referencia DATE COMMENT 'Primeiro dia do mês de referência (YYYY-MM-01).',
    ano INT COMMENT 'Ano do evento.',
    trimestre INT COMMENT 'Trimestre do evento.',
    mes INT COMMENT 'Mês numérico do evento (1 a 12).',
    cnpj_raiz STRING COMMENT 'Radical de 8 dígitos do CNPJ da empresa.',
    tipo_evento STRING COMMENT 'Tipo do evento de negócio originado na Silver.',
    transicao_detalhe STRING COMMENT 'Par valor_anterior -> valor_posterior.',
    motivo_detalhe STRING COMMENT 'Descrição complementar do motivo cadastral ou data.',
    categoria_macro_evento STRING COMMENT 'Classificação amigável do evento para dashboards.',
    razao_social STRING COMMENT 'Razão social da empresa.',
    nome_fantasia STRING COMMENT 'Nome fantasia da matriz.',
    porte_anterior STRING COMMENT 'Enquadramento de porte antes do evento (MEI, ME, EPP, DEMAIS, NAO APLICAVEL).',
    porte_empresa STRING COMMENT 'Enquadramento de porte após o evento (MEI, ME, EPP, DEMAIS, NAO IDENTIFICADO).',
    situacao_cadastral_anterior STRING COMMENT 'Situação cadastral antes do evento.',
    situacao_cadastral_posterior STRING COMMENT 'Situação cadastral após o evento.',
    opcao_mei_anterior BOOLEAN COMMENT 'Status MEI antes do evento.',
    opcao_mei_posterior BOOLEAN COMMENT 'Status MEI após o evento.',
    opcao_simples_anterior BOOLEAN COMMENT 'Status Simples Nacional antes do evento.',
    opcao_simples_posterior BOOLEAN COMMENT 'Status Simples Nacional após o evento.',
    capital_social DOUBLE COMMENT 'Capital social em Reais (BRL) após o evento.',
    uf STRING COMMENT 'UF consolidada da matriz.',
    municipio STRING COMMENT 'Município consolidado da matriz.',
    codigo_ibge_municipio STRING COMMENT 'Código IBGE de 7 dígitos do município.',
    bairro STRING COMMENT 'Localidade/bairro analítico normalizado via CEP (com fallback para campo livre).',
    bairro_campo_livre STRING COMMENT 'Bairro bruto declarado na Receita Federal.',
    codigo_cnae_anterior STRING COMMENT 'Código CNAE principal antes do evento.',
    nome_cnae_anterior STRING COMMENT 'Descrição do CNAE principal antes do evento.',
    grande_area_cnae_anterior STRING COMMENT 'Grande setor econômico antes do evento.',
    codigo_cnae STRING COMMENT 'Código CNAE principal após o evento.',
    nome_cnae STRING COMMENT 'Descrição do CNAE principal após o evento.',
    classe_cnae STRING COMMENT 'Nome da classe CNAE após o evento.',
    grupo_cnae STRING COMMENT 'Nome do grupo CNAE após o evento.',
    divisao_cnae STRING COMMENT 'Nome da divisão CNAE após o evento.',
    grande_area_cnae STRING COMMENT 'Grande setor econômico após o evento.',
    data_carga_etl TIMESTAMP COMMENT 'Timestamp de carga na camada Gold.'
)
USING DELTA
PARTITIONED BY (mes_referencia)
COMMENT 'Camada Gold: OBT de Eventos de Transição Empresarial (grão: 1 linha por evento/empresa no mês, com estados anterior e posterior).';

-- REPLACE WHERE só aceita INSERT INTO e predicado com literais: o Job sempre passa YYYY-MM.
INSERT INTO tcc_receita.gold.obt_dashboard_eventos
REPLACE WHERE mes_referencia >= '{{mes_referencia_inicial}}'
          AND mes_referencia <= '{{mes_referencia_final}}'
WITH params AS (
    SELECT 
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM')) AS mes_fim
),
-- Garante unicidade estrita (1 CNAE primário por grupo) para nunca multiplicar linhas da Fato
ponte_primaria AS (
    SELECT sk_grupo_cnae, sk_cnae
    FROM (
        SELECT 
            sk_grupo_cnae, 
            sk_cnae,
            ROW_NUMBER() OVER (PARTITION BY sk_grupo_cnae ORDER BY nk_cnae ASC) AS rn
        FROM tcc_receita.silver_final.ponte_cnae
        WHERE cnae_principal = TRUE AND sk_grupo_cnae != -1
    )
    WHERE rn = 1
)
SELECT /*+ BROADCAST(t, sc, cnae_ant, cnae_post) */
    f.mes_referencia,
    t.data_referencia,
    t.ano,
    t.trimestre,
    t.mes,
    COALESCE(NULLIF(f.cnpj_raiz, ''), c_post.cnpj_raiz, c_ant.cnpj_raiz, '00000000') AS cnpj_raiz,
    sc.tipo_mudanca_estado AS tipo_evento,
    sc.detalhe_mudanca_estado AS transicao_detalhe,
    sc.descricao_adicional AS motivo_detalhe,
    CASE 
        WHEN sc.tipo_mudanca_estado = 'EMPRESA NASCEU' THEN 'ABERTURA'
        WHEN sc.tipo_mudanca_estado = 'MUDOU ESTADO CADASTRAL' AND UPPER(COALESCE(sc.descricao_adicional, '')) RLIKE 'FALENCIA|FALÊNCIA' THEN 'FECHAMENTO POR FALENCIA'
        WHEN sc.tipo_mudanca_estado = 'MUDOU ESTADO CADASTRAL' AND UPPER(COALESCE(sc.descricao_adicional, '')) RLIKE 'OBITO|ÓBITO' THEN 'EXTINCAO POR OBITO (MEI)'
        WHEN sc.tipo_mudanca_estado = 'MUDOU ESTADO CADASTRAL' AND UPPER(COALESCE(sc.descricao_adicional, '')) RLIKE 'LIQUIDACAO|LIQUIDAÇÃO' THEN 'EXTINCAO POR LIQUIDACAO'
        WHEN sc.tipo_mudanca_estado = 'MUDOU ESTADO CADASTRAL' AND UPPER(COALESCE(sc.descricao_adicional, '')) RLIKE 'OMISSAO|OMISSÃO|INAPTIDAO|INAPTIDÃO' THEN 'INAPTIDAO FISCAL / OMISSAO'
        WHEN sc.tipo_mudanca_estado = 'MUDOU ESTADO CADASTRAL' AND sc.valor_posterior = 'BAIXADA' THEN 'BAIXA VOLUNTARIA / OUTRAS'
        WHEN sc.tipo_mudanca_estado = 'MUDOU ESTADO CADASTRAL' AND sc.valor_posterior = 'INAPTA' THEN 'INAPTIDAO FISCAL'
        WHEN sc.tipo_mudanca_estado = 'MUDOU ESTADO CADASTRAL' AND sc.valor_posterior = 'SUSPENSA' THEN 'SUSPENSAO TEMPORARIA'
        WHEN sc.tipo_mudanca_estado = 'MUDOU ESTADO CADASTRAL' AND sc.valor_posterior = 'NULA' THEN 'INSCRICAO NULA / VICIOS'
        ELSE COALESCE(sc.tipo_mudanca_estado, 'NAO IDENTIFICADO')
    END AS categoria_macro_evento,
    COALESCE(NULLIF(c_post.razao_social, 'NAO APLICAVEL / INEXISTENTE'), c_ant.razao_social, 'NAO IDENTIFICADO') AS razao_social,
    COALESCE(c_post.nome_fantasia_matriz, c_ant.nome_fantasia_matriz) AS nome_fantasia,
    COALESCE(c_ant.enquadramento_porte_analitico, 'NAO APLICAVEL') AS porte_anterior,
    COALESCE(NULLIF(c_post.enquadramento_porte_analitico, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS porte_empresa,
    COALESCE(c_ant.situacao_cadastral_matriz, 'INEXISTENTE') AS situacao_cadastral_anterior,
    COALESCE(c_post.situacao_cadastral_matriz, 'NAO IDENTIFICADO') AS situacao_cadastral_posterior,
    COALESCE(c_ant.opcao_pelo_mei, FALSE) AS opcao_mei_anterior,
    COALESCE(c_post.opcao_pelo_mei, FALSE) AS opcao_mei_posterior,
    COALESCE(c_ant.opcao_pelo_simples, FALSE) AS opcao_simples_anterior,
    COALESCE(c_post.opcao_pelo_simples, FALSE) AS opcao_simples_posterior,
    COALESCE(c_post.capital_social, c_ant.capital_social, 0.0) AS capital_social,
    COALESCE(NULLIF(c_post.uf, 'NA'), NULLIF(c_ant.uf, 'NA'), 'NI') AS uf,
    COALESCE(NULLIF(c_post.municipio, 'NAO APLICAVEL'), NULLIF(c_ant.municipio, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS municipio,
    COALESCE(c_post.codigo_ibge_municipio, c_ant.codigo_ibge_municipio) AS codigo_ibge_municipio,
    COALESCE(NULLIF(c_post.localidade_analitica, 'NAO APLICAVEL'), NULLIF(c_ant.localidade_analitica, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS bairro,
    COALESCE(NULLIF(c_post.bairro_campo_livre, 'NAO APLICAVEL'), NULLIF(c_ant.bairro_campo_livre, 'NAO APLICAVEL'), 'NAO IDENTIFICADO') AS bairro_campo_livre,
    COALESCE(cnae_ant.codigo_cnae, '0000000') AS codigo_cnae_anterior,
    COALESCE(cnae_ant.descricao_cnae, 'NAO INFORMADO / NAO APLICAVEL') AS nome_cnae_anterior,
    COALESCE(cnae_ant.grande_setor_economico, 'OUTROS / NAO ESPECIFICADO') AS grande_area_cnae_anterior,
    COALESCE(cnae_post.codigo_cnae, '0000000') AS codigo_cnae,
    COALESCE(cnae_post.descricao_cnae, 'NAO INFORMADO / NAO APLICAVEL') AS nome_cnae,
    COALESCE(cnae_post.nome_classe, 'NAO ESPECIFICADO') AS classe_cnae,
    COALESCE(cnae_post.nome_grupo, 'NAO ESPECIFICADO') AS grupo_cnae,
    COALESCE(cnae_post.nome_divisao, 'NAO ESPECIFICADO') AS divisao_cnae,
    COALESCE(cnae_post.grande_setor_economico, 'OUTROS / NAO ESPECIFICADO') AS grande_area_cnae,
    NOW() AS data_carga_etl
FROM tcc_receita.silver_final.fato_mudanca_estado f
CROSS JOIN params p
LEFT JOIN tcc_receita.silver_final.dim_tempo t 
    ON f.sk_tempo = t.sk_tempo
LEFT JOIN tcc_receita.silver_final.dim_mudanca_estado sc 
    ON f.sk_mudanca_estado = sc.sk_mudanca_estado
LEFT JOIN tcc_receita.silver_final.dim_empresa c_ant 
    ON f.sk_empresa_anterior = c_ant.sk_empresa
LEFT JOIN tcc_receita.silver_final.dim_empresa c_post 
    ON f.sk_empresa_posterior = c_post.sk_empresa
LEFT JOIN ponte_primaria b_ant 
    ON f.sk_grupo_cnae_anterior = b_ant.sk_grupo_cnae
LEFT JOIN tcc_receita.silver_final.dim_cnae cnae_ant 
    ON COALESCE(b_ant.sk_cnae, -1) = cnae_ant.sk_cnae
LEFT JOIN ponte_primaria b_post 
    ON f.sk_grupo_cnae_posterior = b_post.sk_grupo_cnae
LEFT JOIN tcc_receita.silver_final.dim_cnae cnae_post 
    ON COALESCE(b_post.sk_cnae, -1) = cnae_post.sk_cnae
WHERE f.mes_referencia >= p.mes_ini
  AND f.mes_referencia <= p.mes_fim;