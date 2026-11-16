-- Silver Intermediate: histórico SCD2 de empresas (raiz do CNPJ).
-- Parâmetros vazios assumem o mês anterior; mes_referencia_final vazio = o inicial.
--
-- Como funciona (SCD2 em duas fases; spec docs/specs/features/2026-11-16-otimizacao-scd2.md):
--   Fase 1 (detectar): lê só (pk, mês, carimbo, change_hash) da bronze da janela e das versões da silver que
--     a janela pode alterar, aplica a regra de sempre (abre versão quando o hash muda em relação à anterior)
--     e grava uma tabela pequena de AÇÕES: INSERE (versão nova), AJUSTA_FIM (mudar o fim de uma versão
--     existente) e APAGA (versão que deixou de existir; só em reprocessamento).
--   Fase 2 (gravar): um único MERGE, atômico, aplica as ações. Os atributos e a limpeza (datas, flags) só
--     são buscados na bronze para as linhas que abrem versão (~1-2% por mês, fora a carga inicial).
-- Reexecutar a mesma janela gera zero ações (idempotente). Uma falha em qualquer instrução deixa o destino
-- como estava: só o MERGE escreve nele, e ele é uma transação. Para retomar, basta rodar de novo.
--
-- Instruções: 0 DDL | 1 validação | 2 ações (fase 1) | 3 meses densos | 4 guarda de memória | 5 MERGE | 6 limpeza

-- ---------------------------------------------------------------------------------------------------------
-- 0. Tabela de destino (DDL inalterado).
-- ---------------------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.empresas (
    pk STRING COMMENT 'Chave primária lógica da empresa, gerada na Bronze.',
    cnpj_basico STRING COMMENT 'Radical do CNPJ (primeiros 8 dígitos).',
    razao_social STRING COMMENT 'Nome empresarial limpo e padronizado em maiúsculas, com caracteres Unicode preservados.',
    natureza_juridica STRING COMMENT 'Código da natureza jurídica.',
    qualificacao_responsavel STRING COMMENT 'Código da qualificação do responsável.',
    capital_social DOUBLE COMMENT 'Valor do capital social em Reais (BRL).',
    porte_empresa STRING COMMENT 'Código do porte da empresa (01=ME, 03=EPP, 05=Demais).',
    ente_federativo_responsavel STRING COMMENT 'Ente federativo responsável, quando aplicável.',
    mes_referencia STRING COMMENT 'Mês de referência do snapshot que originou a versão.',
    inserted_at TIMESTAMP COMMENT 'Timestamp do processamento Bronze que originou a versão.',
    updated_at TIMESTAMP COMMENT 'Timestamp da última atualização da versão no SCD2.',
    change_hash STRING COMMENT 'Hash SHA-256 herdado da Bronze, calculado sobre os atributos de negócio de origem.',
    valid_since STRING COMMENT 'Início da vigência da versão (YYYY-MM), inclusivo.',
    valid_until STRING COMMENT 'Fim da vigência da versão (YYYY-MM), exclusivo; NULL para versão atual.',
    is_current BOOLEAN COMMENT 'Indica se esta é a versão vigente.'
)
USING DELTA
COMMENT 'Camada Silver Intermediate (SCD2): Histórico de mutações cadastrais da raiz do CNPJ (Empresas).';

-- ---------------------------------------------------------------------------------------------------------
-- 1. Validação, antes de qualquer escrita: janela no formato YYYY-MM, início <= fim, e TODOS os meses da
--    janela presentes na bronze. Um mês faltando é quase sempre erro de ingestão; seguir em frente apagaria
--    as versões existentes daquele mês (decisão D5 da spec).
--    Agregação sem GROUP BY: devolve sempre uma linha, então as asserções sempre rodam (mesmo com bronze vazia).
--    try_to_timestamp devolve NULL (em vez de erro) para mês inválido, com ANSI ligado ou desligado.
-- ---------------------------------------------------------------------------------------------------------
WITH params AS (
    SELECT
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
),
meses_bronze AS (
    SELECT COUNT(DISTINCT b.mes_referencia) AS qtd_meses
    FROM tcc_receita.bronze.empresas b
    CROSS JOIN params p
    WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
)
SELECT
    ASSERT_TRUE(
        p.mes_ini RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
        AND p.mes_fim RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
        AND p.mes_ini <= p.mes_fim,
        'Janela inválida: informe mes_ini e mes_fim no formato YYYY-MM e mes_ini <= mes_fim.'
    ) AS janela_valida,
    ASSERT_TRUE(
        c.qtd_meses = CAST(months_between(CAST(try_to_timestamp(p.mes_fim, 'yyyy-MM') AS DATE),
                                          CAST(try_to_timestamp(p.mes_ini, 'yyyy-MM') AS DATE)) AS INT) + 1,
        CONCAT('Bronze empresas sem todos os meses da janela ', p.mes_ini, '..', p.mes_fim,
               ': meses encontrados = ', CAST(c.qtd_meses AS STRING), '. Nada foi gravado.')
    ) AS meses_completos
FROM params p
CROSS JOIN meses_bronze c;

-- ---------------------------------------------------------------------------------------------------------
-- 2. Fase 1 — detecção. Resultado: uma linha por ação, só com chaves, vigência e hash (tabela estreita).
-- ---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE tcc_receita.silver_intermediate.tmp_scd2_acoes_empresas AS
WITH params AS (
    SELECT
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
),
-- (a) Bronze da janela: só chave, mês, carimbo e hash (4 colunas em vez da linha inteira).
bronze_estreita AS (
    SELECT b.pk, b.mes_referencia AS mes, 'B' AS origem,
           b.bronze_processed_at AS ts, b.change_hash AS h,
           CAST(NULL AS STRING) AS valid_until_atual
    FROM tcc_receita.bronze.empresas b
    CROSS JOIN params p
    WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
      AND b.pk IS NOT NULL
),
-- (b) Versões existentes que a janela pode alterar: abertas, ou que terminam em mes_ini ou depois.
--     Uma versão com valid_until < mes_ini tem sucessora antes da janela, então nada nela muda.
--     S_DENTRO = começa dentro da janela (é o "antes" a comparar com a bronze nova);
--     S_FORA   = começa antes ou depois da janela (é um ponto fixo da linha do tempo, como o
--                "historico_intacto" da versão anterior deste arquivo).
silver_estreita AS (
    SELECT s.pk, s.valid_since AS mes,
           CASE WHEN s.valid_since BETWEEN p.mes_ini AND p.mes_fim THEN 'S_DENTRO' ELSE 'S_FORA' END AS origem,
           CAST(NULL AS TIMESTAMP) AS ts, s.change_hash AS h,
           s.valid_until AS valid_until_atual
    FROM tcc_receita.silver_intermediate.empresas s
    CROSS JOIN params p
    WHERE s.valid_until IS NULL OR s.valid_until >= p.mes_ini
),
-- (c) A ÚNICA troca de dados (shuffle) da fase 1: todas as linhas de uma pk vão para a mesma tarefa.
--     Daqui em diante, as janelas (LAG/LEAD/ROW_NUMBER) particionam por pk e só ordenam localmente.
uniao AS (
    SELECT * FROM (
        SELECT * FROM bronze_estreita
        UNION ALL
        SELECT * FROM silver_estreita
    ) u
    DISTRIBUTE BY pk
),
-- (d) PK repetida no mesmo mês da bronze (B16): fica uma linha por (pk, mês), com o mesmo desempate da
--     versão anterior (carimbo mais recente; depois o maior hash). Versões da silver passam direto.
dedup AS (
    SELECT * FROM (
        SELECT u.*,
               ROW_NUMBER() OVER (PARTITION BY pk, mes, origem
                                  ORDER BY ts DESC NULLS LAST, h DESC) AS rn
        FROM uniao u
    ) x
    WHERE origem <> 'B' OR rn = 1
),
-- (e) Linha do tempo nova. Candidatas = versões fora da janela (S_FORA) + bronze da janela (B).
--     Uma candidata abre versão quando o hash difere do da candidata anterior (a 1ª sempre abre).
--     PARTITION BY pk, <é candidata>: o LAG ignora as linhas S_DENTRO sem precisar de nova troca de dados.
com_anterior AS (
    SELECT d.*,
           origem IN ('S_FORA', 'B') AS eh_candidata,
           LAG(h) OVER (PARTITION BY pk, origem IN ('S_FORA', 'B') ORDER BY mes) AS h_anterior
    FROM dedup d
),
com_abertura AS (
    SELECT c.*, (eh_candidata AND h_anterior IS DISTINCT FROM h) AS abre_versao
    FROM com_anterior c
),
-- valid_until = mês em que abre a próxima versão (exclusivo); NULL = versão atual.
com_vigencia AS (
    SELECT a.*,
           LEAD(mes) OVER (PARTITION BY pk, abre_versao ORDER BY mes) AS valid_until_novo
    FROM com_abertura a
),
-- (f) Pareamento no mesmo (pk, mês): versão existente dentro da janela (S_DENTRO) × versão nova (B que abre).
--     '9999-12' é só um marcador: distingue "versão nova aberta" (fim NULL) de "não há versão nova".
pareada AS (
    SELECT v.*,
        MAX(CASE WHEN origem = 'S_DENTRO' THEN h END) OVER (PARTITION BY pk, mes) AS h_existente,
        MAX(CASE WHEN origem = 'B' AND abre_versao THEN h END) OVER (PARTITION BY pk, mes) AS h_nova,
        MAX(CASE WHEN origem = 'B' AND abre_versao THEN COALESCE(valid_until_novo, '9999-12') END)
            OVER (PARTITION BY pk, mes) AS fim_nova
    FROM com_vigencia v
),
-- (g) Tabela de ações (spec §3.4). Linha sem ação = nada muda (é o caso de 98-99% das linhas).
acoes AS (
    SELECT pk, mes, origem, h, ts,
        CASE
          -- versão fora da janela que ficou redundante (mesmo hash da versão nova anterior a ela)
          WHEN origem = 'S_FORA'   AND NOT abre_versao                                              THEN 'APAGA'
          -- versão fora da janela cujo fim mudou (caso típico do mês incremental: fechar a corrente)
          WHEN origem = 'S_FORA'   AND valid_until_novo IS DISTINCT FROM valid_until_atual           THEN 'AJUSTA_FIM'
          -- versão dentro da janela sem versão nova igual no mesmo mês (reprocessamento)
          WHEN origem = 'S_DENTRO' AND (h_nova IS NULL OR h_nova <> h)                               THEN 'APAGA'
          -- versão dentro da janela que continua, mas com outro fim
          WHEN origem = 'S_DENTRO' AND NULLIF(fim_nova, '9999-12') IS DISTINCT FROM valid_until_atual THEN 'AJUSTA_FIM'
          -- versão nova que ainda não existe na tabela (se for substituição, a antiga recebe APAGA acima)
          WHEN origem = 'B' AND abre_versao AND (h_existente IS NULL OR h_existente <> h)            THEN 'INSERE'
        END AS acao,
        CASE WHEN origem = 'S_DENTRO' THEN NULLIF(fim_nova, '9999-12') ELSE valid_until_novo END AS valid_until
    FROM pareada
)
SELECT pk,
       mes                                 AS valid_since,
       acao,
       valid_until,
       valid_until IS NULL                 AS is_current,
       CASE WHEN origem <> 'B' THEN h END  AS hash_atual,   -- identifica a versão existente no MERGE
       mes                                 AS mes_origem,   -- onde buscar os atributos (INSERE)
       CASE WHEN origem = 'B' THEN h END   AS hash_origem,  -- linha exata da bronze (a vencedora do B16)
       CASE WHEN origem = 'B' THEN ts END  AS ts_origem
FROM acoes
WHERE acao IS NOT NULL;

-- ---------------------------------------------------------------------------------------------------------
-- 3. Meses densos. Um mês é "denso" quando mais de 20% das linhas da bronze daquele mês abrem versão (1º mês
--    da carga inicial, ou um mês em que todos os hashes mudaram). Mês denso entra inteiro na busca da
--    instrução 5; mês esparso entra só pelas chaves da lista difundida (broadcast).
-- ---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE tcc_receita.silver_intermediate.tmp_scd2_meses_empresas AS
WITH params AS (
    SELECT
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
),
bronze_mes AS (
    SELECT b.mes_referencia AS mes, COUNT(*) AS n_bronze
    FROM tcc_receita.bronze.empresas b
    CROSS JOIN params p
    WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
    GROUP BY b.mes_referencia
),
novas_mes AS (
    SELECT mes_origem AS mes, COUNT(*) AS n_insere
    FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_empresas
    WHERE acao = 'INSERE'
    GROUP BY mes_origem
)
SELECT b.mes, b.n_bronze, COALESCE(n.n_insere, 0) AS n_insere,
       COALESCE(n.n_insere, 0) > 0.2 * b.n_bronze AS denso
FROM bronze_mes b
LEFT JOIN novas_mes n ON b.mes = n.mes;

-- ---------------------------------------------------------------------------------------------------------
-- 4. Guarda de memória: a lista difundida (chaves dos meses esparsos) precisa caber na memória de cada
--    máquina. 25 M chaves de 8 bytes ≈ 600 MB. Se passar, a janela é grande demais: rodar em blocos menores.
--    (O texto da mensagem não pode conter as palavras I-N-S-E-R-T nem M-E-R-G-E: o run_sql.py deixaria de
--    tratar esta instrução como consulta e a asserção não rodaria.)
-- ---------------------------------------------------------------------------------------------------------
SELECT ASSERT_TRUE(
    COALESCE(SUM(CASE WHEN NOT denso THEN n_insere ELSE 0 END), 0) <= 25000000,
    CONCAT('Versoes novas em meses esparsos de empresas: ',
           CAST(SUM(CASE WHEN NOT denso THEN n_insere ELSE 0 END) AS STRING),
           ' (limite 25 M para a lista difundida). Rode a janela em blocos menores.')
) AS lista_cabe_na_memoria
FROM tcc_receita.silver_intermediate.tmp_scd2_meses_empresas;

-- ---------------------------------------------------------------------------------------------------------
-- 5. Fase 2 — um único MERGE (atômico) aplica as três ações.
--    A fonte une (1) as ações sobre versões existentes (só chave e vigência) com (2) as versões novas, já com
--    os atributos buscados na bronze e limpos. A condição ON inclui o hash da versão existente (hash_atual):
--    as linhas INSERE têm hash_atual NULL e por isso nunca casam, nem quando APAGA e INSERE têm a mesma
--    (pk, valid_since) numa substituição. Cada linha do destino casa com no máximo uma linha da fonte.
-- ---------------------------------------------------------------------------------------------------------
MERGE INTO tcc_receita.silver_intermediate.empresas AS t
USING (
    WITH params AS (
        SELECT
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
    ),
    -- Chaves (pk, mês) das versões novas dos meses esparsos, reduzidas a um inteiro de 64 bits.
    -- Colisão do xxhash64 só deixa passar uma linha a mais, que o join exato abaixo descarta.
    chaves_esparsas AS (
        SELECT DISTINCT xxhash64(a.pk, a.mes_origem) AS chave
        FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_empresas a
        JOIN tcc_receita.silver_intermediate.tmp_scd2_meses_empresas mm
          ON a.mes_origem = mm.mes AND NOT mm.denso
        WHERE a.acao = 'INSERE'
    ),
    -- Um único scan da bronze da janela: mês denso passa inteiro; mês esparso só se a chave estiver na
    -- lista difundida. Os dois joins são broadcast, então nenhuma linha larga da bronze é embaralhada aqui.
    bronze_filtrada AS (
        SELECT /*+ BROADCAST(m), BROADCAST(k) */ b.*
        FROM tcc_receita.bronze.empresas b
        CROSS JOIN params p
        JOIN tcc_receita.silver_intermediate.tmp_scd2_meses_empresas m
          ON b.mes_referencia = m.mes
        LEFT JOIN chaves_esparsas k
          ON xxhash64(b.pk, b.mes_referencia) = k.chave
        WHERE b.mes_referencia BETWEEN p.mes_ini AND p.mes_fim
          AND (m.denso OR k.chave IS NOT NULL)
    ),
    novas AS (
        SELECT * FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_empresas WHERE acao = 'INSERE'
    )
    -- (1) Ações sobre versões existentes: os atributos não importam (o UPDATE só mexe na vigência).
    SELECT a.pk, a.valid_since, a.acao, a.hash_atual, a.valid_until, a.is_current,
           CAST(NULL AS STRING)    AS cnpj_basico,
           CAST(NULL AS STRING)    AS razao_social,
           CAST(NULL AS STRING)    AS natureza_juridica,
           CAST(NULL AS STRING)    AS qualificacao_responsavel,
           CAST(NULL AS DOUBLE)    AS capital_social,
           CAST(NULL AS STRING)    AS porte_empresa,
           CAST(NULL AS STRING)    AS ente_federativo_responsavel,
           CAST(NULL AS TIMESTAMP) AS inserted_at,
           CAST(NULL AS STRING)    AS change_hash
    FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_empresas a
    WHERE a.acao IN ('APAGA', 'AJUSTA_FIM')
    UNION ALL
    -- (2) Versões novas: atributos da bronze com a mesma limpeza de sempre.
    --     O join exato por (pk, mês, hash, carimbo) pega a mesma linha que venceu o desempate da fase 1 e
    --     descarta os falsos positivos do xxhash64. (pk, mês, change_hash) é única na bronze
    --     (dropDuplicates na ingestão), então sai uma linha por versão nova; o DQ de unicidade vigia isso.
    SELECT n.pk, n.valid_since, 'INSERE' AS acao, CAST(NULL AS STRING) AS hash_atual, n.valid_until, n.is_current,
           NULLIF(TRIM(b.cnpj_basico), '') AS cnpj_basico,
           NULLIF(UPPER(TRIM(regexp_replace(b.razao_social, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS razao_social,
           NULLIF(TRIM(b.natureza_juridica), '') AS natureza_juridica,
           NULLIF(TRIM(b.qualificacao_responsavel), '') AS qualificacao_responsavel,
           TRY_CAST(REPLACE(NULLIF(TRIM(CAST(b.capital_social AS STRING)), ''), ',', '.') AS DOUBLE) AS capital_social,
           NULLIF(TRIM(b.porte_empresa), '') AS porte_empresa,
           NULLIF(TRIM(b.ente_federativo_responsavel), '') AS ente_federativo_responsavel,
           b.bronze_processed_at AS inserted_at,
           b.change_hash
    FROM bronze_filtrada b
    JOIN novas n
      ON  b.pk = n.pk
      AND b.mes_referencia = n.mes_origem
      AND b.change_hash = n.hash_origem
      AND b.bronze_processed_at <=> n.ts_origem
) AS s
ON t.pk = s.pk AND t.valid_since = s.valid_since AND t.change_hash = s.hash_atual
WHEN MATCHED AND s.acao = 'APAGA' THEN DELETE
WHEN MATCHED AND s.acao = 'AJUSTA_FIM' THEN UPDATE SET
    t.valid_until = s.valid_until,
    t.is_current  = s.is_current,
    t.updated_at  = current_timestamp()
WHEN NOT MATCHED AND s.acao = 'INSERE' THEN INSERT (
    pk, cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
    capital_social, porte_empresa, ente_federativo_responsavel,
    mes_referencia, inserted_at, updated_at, change_hash,
    valid_since, valid_until, is_current
) VALUES (
    s.pk, s.cnpj_basico, s.razao_social, s.natureza_juridica,
    s.qualificacao_responsavel, s.capital_social, s.porte_empresa,
    s.ente_federativo_responsavel,
    s.valid_since, s.inserted_at, current_timestamp(), s.change_hash,
    s.valid_since, s.valid_until, s.is_current
);

-- ---------------------------------------------------------------------------------------------------------
-- 6. Limpeza das tabelas de trabalho. Se a execução parar antes daqui, a sobra é inofensiva: a próxima
--    execução recria as duas tabelas (CREATE OR REPLACE).
-- ---------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS tcc_receita.silver_intermediate.tmp_scd2_acoes_empresas;
DROP TABLE IF EXISTS tcc_receita.silver_intermediate.tmp_scd2_meses_empresas;
