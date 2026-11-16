-- Silver Intermediate: histórico SCD2 do QSA (relação empresa-sócio).
-- Parâmetros vazios assumem o mês anterior; mes_referencia_final vazio = o inicial.
-- IMPORTANTE: para detectar saída do QSA, este SQL compara cada snapshot mensal com o snapshot
-- imediatamente anterior. A Bronze precisa conter o mês anterior a mes_ini e TODOS os meses
-- da janela, completos. Exceção (carga inicial): se mes_ini for o primeiro mês existente na Bronze
-- (2023-05, primeiro snapshot publicado pela RFB), não há mês anterior; o mês só abre as versões e
-- nenhuma saída do QSA é inferida nele. Veja README.md antes da primeira execução.
-- A PK é herdada da Bronze; nenhum CPF/CNPJ em claro é reconstruído aqui.
--
-- Como funciona (SCD2 em duas fases; spec docs/specs/features/2026-11-16-otimizacao-scd2.md, §3.6):
--   Mesmo molde de dados_simples/empresas/estabelecimentos (fase 1 = detectar ações com linhas estreitas;
--   fase 2 = um único MERGE atômico que busca os atributos só das versões novas), com uma regra a mais:
--   a AUSÊNCIA no snapshot do mês M de uma relação presente em M-1 gera uma versão vinculo_ativo = FALSE em
--   M, com os atributos e o inserted_at de M-1 e change_hash = SHA2(TO_JSON({hash de M-1, FALSE})).
--   Por isso a bronze é lida desde M-1 = mes_ini - 1 (o "mês de contexto"), que serve só para detectar
--   saídas em mes_ini e não vira versão.
--
-- Instruções: 0 DDL | 1 validação | 2 ações (fase 1) | 3 meses densos | 4 guarda de memória | 5 MERGE | 6 limpeza

-- ---------------------------------------------------------------------------------------------------------
-- 0. Tabela de destino (DDL inalterado).
-- ---------------------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate;

CREATE TABLE IF NOT EXISTS tcc_receita.silver_intermediate.socios (
    pk STRING COMMENT 'Chave primária lógica da relação entre uma empresa e um sócio, herdada da Bronze.',
    cnpj_basico STRING COMMENT 'Radical do CNPJ da empresa (primeiros 8 dígitos).',
    identificador_socio STRING COMMENT '1=PJ, 2=PF, 3=Estrangeiro, conforme domínio da fonte.',
    nome_socio_razao_social STRING COMMENT 'Nome ou razão social do sócio limpo e padronizado; acentos preservados.',
    cnpj_cpf_socio STRING COMMENT 'Documento do sócio conforme recebido, parcialmente mascarado quando assim fornecido pela origem.',
    qualificacao_socio STRING COMMENT 'Código da qualificação do sócio na sociedade.',
    data_entrada_sociedade STRING COMMENT 'Data de entrada na sociedade validada (AAAAMMDD).',
    pais STRING COMMENT 'Código do país do sócio estrangeiro.',
    representante_legal STRING COMMENT 'Documento do representante legal conforme recebido, sem desmascaramento.',
    nome_representante STRING COMMENT 'Nome do representante legal limpo e padronizado.',
    qualificacao_representante_legal STRING COMMENT 'Código da qualificação do representante legal.',
    faixa_etaria STRING COMMENT 'Código da faixa etária do sócio pessoa física.',
    vinculo_ativo BOOLEAN COMMENT 'TRUE quando a relação aparece no snapshot mensal; FALSE a partir do primeiro snapshot completo em que deixa de aparecer.',
    mes_referencia STRING COMMENT 'Mês do snapshot que originou a versão ou em que a ausência foi detectada.',
    inserted_at TIMESTAMP COMMENT 'Timestamp do processamento Bronze que originou os atributos da versão.',
    updated_at TIMESTAMP COMMENT 'Timestamp da última atualização da versão no SCD2.',
    change_hash STRING COMMENT 'SHA-256 do hash de origem combinado com o estado do vínculo (ativo/inativo).',
    valid_since STRING COMMENT 'Início da vigência da versão (YYYY-MM), inclusivo.',
    valid_until STRING COMMENT 'Fim da vigência da versão (YYYY-MM), exclusivo; NULL para versão atual.',
    is_current BOOLEAN COMMENT 'Indica se esta é a versão vigente.'
)
USING DELTA
PARTITIONED BY (is_current)
COMMENT 'Camada Silver Intermediate (SCD2): Histórico do Quadro de Sócios e Administradores e da vigência das relações societárias.';

-- ---------------------------------------------------------------------------------------------------------
-- 1. Validação, antes de qualquer escrita:
--    (a) janela no formato YYYY-MM e início <= fim;
--    (b) a bronze tem o mês anterior a mes_ini (exceto na carga inicial) e todos os meses da janela;
--    (c) nenhum snapshot parcial: cada mês tem pelo menos 95% das linhas do mês anterior (decisão D6). Um
--        snapshot pela metade geraria milhares de saídas falsas do QSA; é melhor falhar antes de gravar.
--    Agregação sem GROUP BY no fim: devolve sempre uma linha, então as asserções sempre rodam (mesmo com a
--    bronze vazia). try_to_timestamp devolve NULL (em vez de erro) para mês inválido, com ANSI ligado ou não.
-- ---------------------------------------------------------------------------------------------------------
WITH params AS (
    SELECT mes_ini, mes_fim,
           date_format(add_months(CAST(try_to_timestamp(mes_ini, 'yyyy-MM') AS DATE), -1), 'yyyy-MM') AS mes_ant,
           -- Primeiro snapshot disponível: janela que começa nele é carga inicial (sem mês anterior).
           (SELECT MIN(mes_referencia) FROM tcc_receita.bronze.socios) AS primeiro_mes_bronze
    FROM (
        SELECT
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
    )
),
linhas_mes AS (
    SELECT b.mes_referencia AS mes, COUNT(*) AS n
    FROM tcc_receita.bronze.socios b
    CROSS JOIN params p
    WHERE b.mes_referencia BETWEEN p.mes_ant AND p.mes_fim
    GROUP BY b.mes_referencia
),
com_mes_anterior AS (
    SELECT mes, n, LAG(n) OVER (ORDER BY mes) AS n_anterior   -- no máximo ~13 linhas: uma partição só
    FROM linhas_mes
),
resumo AS (
    SELECT COUNT(*) AS qtd_meses,
           COUNT_IF(n < 0.95 * n_anterior) AS meses_parciais,
           MIN(CASE WHEN n < 0.95 * n_anterior THEN mes END) AS exemplo_parcial
    FROM com_mes_anterior
)
SELECT
    ASSERT_TRUE(
        p.mes_ini RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
        AND p.mes_fim RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
        AND p.mes_ini <= p.mes_fim,
        'Janela inválida: informe mes_ini e mes_fim no formato YYYY-MM e mes_ini <= mes_fim.'
    ) AS janela_valida,
    ASSERT_TRUE(
        r.qtd_meses = CAST(months_between(CAST(try_to_timestamp(p.mes_fim, 'yyyy-MM') AS DATE),
                                          CAST(try_to_timestamp(p.mes_ini, 'yyyy-MM') AS DATE)) AS INT) + 1
                      + CASE WHEN p.mes_ini > p.primeiro_mes_bronze THEN 1 ELSE 0 END,
        CONCAT('Snapshots Bronze de socios ausentes: é necessário o mês anterior ao início (exceto na carga ',
               'inicial) e todos os meses da janela. Meses distintos encontrados na Bronze: ',
               CAST(r.qtd_meses AS STRING), '. Nada foi gravado.')
    ) AS snapshots_validos,
    ASSERT_TRUE(
        r.meses_parciais = 0,
        CONCAT('Snapshot parcial de socios: o mês ', COALESCE(r.exemplo_parcial, '?'),
               ' tem menos de 95% das linhas do mês anterior. Nada foi gravado.')
    ) AS snapshots_completos
FROM params p
CROSS JOIN resumo r;

-- ---------------------------------------------------------------------------------------------------------
-- 2. Fase 1 — detecção. Resultado: uma linha por ação, só com chaves, vigência e hash (tabela estreita).
-- ---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE tcc_receita.silver_intermediate.tmp_scd2_acoes_socios AS
WITH params AS (
    SELECT mes_ini, mes_fim,
           date_format(add_months(CAST(try_to_timestamp(mes_ini, 'yyyy-MM') AS DATE), -1), 'yyyy-MM') AS mes_ant
    FROM (
        SELECT
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
    )
),
-- (a) Bronze do mês anterior + janela: só chave, mês, carimbo e hash.
--     B_CTX = mês anterior a mes_ini (só contexto, para detectar saídas em mes_ini; não vira versão);
--     B     = meses da janela.
bronze_estreita AS (
    SELECT b.pk, b.mes_referencia AS mes,
           CASE WHEN b.mes_referencia = p.mes_ant THEN 'B_CTX' ELSE 'B' END AS origem,
           b.bronze_processed_at AS ts, b.change_hash AS h,
           CAST(NULL AS STRING) AS valid_until_atual
    FROM tcc_receita.bronze.socios b
    CROSS JOIN params p
    WHERE b.mes_referencia BETWEEN p.mes_ant AND p.mes_fim
      AND b.pk IS NOT NULL
),
-- (b) Versões existentes que a janela pode alterar: abertas, ou que terminam em mes_ini ou depois.
--     S_DENTRO = começa dentro da janela (o "antes" a comparar); S_FORA = ponto fixo da linha do tempo.
silver_estreita AS (
    SELECT s.pk, s.valid_since AS mes,
           CASE WHEN s.valid_since BETWEEN p.mes_ini AND p.mes_fim THEN 'S_DENTRO' ELSE 'S_FORA' END AS origem,
           CAST(NULL AS TIMESTAMP) AS ts, s.change_hash AS h,
           s.valid_until AS valid_until_atual
    FROM tcc_receita.silver_intermediate.socios s
    CROSS JOIN params p
    WHERE s.valid_until IS NULL OR s.valid_until >= p.mes_ini
),
-- (c) A ÚNICA troca de dados (shuffle) da fase 1: todas as linhas de uma pk vão para a mesma tarefa.
uniao AS (
    SELECT * FROM (
        SELECT * FROM bronze_estreita
        UNION ALL
        SELECT * FROM silver_estreita
    ) u
    DISTRIBUTE BY pk
),
-- (d) PK repetida no mesmo mês da bronze (B16): uma linha por (pk, mês), com o desempate de sempre
--     (carimbo mais recente; depois o maior hash, o que torna a escolha determinística mesmo com carimbos
--     iguais). Vale também para o mês de contexto: senão a inativa sairia de uma linha qualquer.
dedup AS (
    SELECT * FROM (
        SELECT u.*,
               ROW_NUMBER() OVER (PARTITION BY pk, mes, origem
                                  ORDER BY ts DESC NULLS LAST, h DESC) AS rn
        FROM uniao u
    ) x
    WHERE origem NOT IN ('B', 'B_CTX') OR rn = 1
),
-- (e) Próxima presença da pk na bronze (mês seguinte em que aparece), para detectar ausência.
presenca AS (
    SELECT d.*,
           LEAD(mes) OVER (PARTITION BY pk, origem IN ('B', 'B_CTX') ORDER BY mes) AS prox_presenca,
           date_format(add_months(CAST(try_to_timestamp(mes, 'yyyy-MM') AS DATE), 1), 'yyyy-MM') AS mes_seguinte
    FROM dedup d
),
-- (f) Pontos da linha do tempo. Cada linha vira 0, 1 ou 2 pontos:
--     - a própria linha (versões existentes e bronze da janela; o mês de contexto não é ponto);
--     - uma saída do QSA: presente em pr.mes e ausente no mês seguinte, se esse mês está na janela.
--       A inativa só nasce no 1º mês ausente (regra de sempre).
--     hash_origem/ts_origem/mes_origem apontam a linha da bronze de onde vêm os atributos (para a inativa,
--     a linha de M-1); h é o hash do ponto (para a inativa, o hash derivado).
pontos AS (
    SELECT pr.pk,
           CASE WHEN pr.origem IN ('B', 'B_CTX') THEN 'B' ELSE pr.origem END AS origem,
           pr.valid_until_atual,
           pr.h  AS hash_origem,
           pr.ts AS ts_origem,
           pt.mes, pt.h, pt.vinculo_ativo, pt.mes_origem
    FROM presenca pr
    CROSS JOIN params p
    LATERAL VIEW INLINE(FILTER(ARRAY(
        IF(pr.origem <> 'B_CTX',
           NAMED_STRUCT('mes', pr.mes, 'h', pr.h,
                        'vinculo_ativo', IF(pr.origem = 'B', TRUE, CAST(NULL AS BOOLEAN)),
                        'mes_origem', pr.mes),
           NULL),
        IF(pr.origem IN ('B', 'B_CTX') AND pr.mes < p.mes_fim
           AND (pr.prox_presenca IS NULL OR pr.prox_presenca > pr.mes_seguinte),
           NAMED_STRUCT('mes', pr.mes_seguinte,
                        'h', sha2(to_json(named_struct('source_change_hash', pr.h, 'vinculo_ativo', FALSE)), 256),
                        'vinculo_ativo', FALSE,
                        'mes_origem', pr.mes),
           NULL)
    ), x -> x IS NOT NULL)) pt
),
-- (g) Daqui em diante, igual às outras SCD2 (spec §3.3 e): candidatas = S_FORA + pontos da bronze;
--     abre versão quando o hash do ponto difere do da candidata anterior.
com_anterior AS (
    SELECT d.*,
           origem IN ('S_FORA', 'B') AS eh_candidata,
           LAG(h) OVER (PARTITION BY pk, origem IN ('S_FORA', 'B') ORDER BY mes) AS h_anterior
    FROM pontos d
),
com_abertura AS (
    SELECT c.*, (eh_candidata AND h_anterior IS DISTINCT FROM h) AS abre_versao
    FROM com_anterior c
),
com_vigencia AS (
    SELECT a.*,
           LEAD(mes) OVER (PARTITION BY pk, abre_versao ORDER BY mes) AS valid_until_novo
    FROM com_abertura a
),
-- (h) Pareamento no mesmo (pk, mês): versão existente dentro da janela × versão nova.
--     '9999-12' é só um marcador: distingue "versão nova aberta" (fim NULL) de "não há versão nova".
pareada AS (
    SELECT v.*,
        MAX(CASE WHEN origem = 'S_DENTRO' THEN h END) OVER (PARTITION BY pk, mes) AS h_existente,
        MAX(CASE WHEN origem = 'B' AND abre_versao THEN h END) OVER (PARTITION BY pk, mes) AS h_nova,
        MAX(CASE WHEN origem = 'B' AND abre_versao THEN COALESCE(valid_until_novo, '9999-12') END)
            OVER (PARTITION BY pk, mes) AS fim_nova
    FROM com_vigencia v
),
-- (i) Tabela de ações (spec §3.4).
acoes AS (
    SELECT pk, mes, origem, h, mes_origem, hash_origem, ts_origem, vinculo_ativo,
        CASE
          WHEN origem = 'S_FORA'   AND NOT abre_versao                                              THEN 'APAGA'
          WHEN origem = 'S_FORA'   AND valid_until_novo IS DISTINCT FROM valid_until_atual           THEN 'AJUSTA_FIM'
          WHEN origem = 'S_DENTRO' AND (h_nova IS NULL OR h_nova <> h)                               THEN 'APAGA'
          WHEN origem = 'S_DENTRO' AND NULLIF(fim_nova, '9999-12') IS DISTINCT FROM valid_until_atual THEN 'AJUSTA_FIM'
          WHEN origem = 'B' AND abre_versao AND (h_existente IS NULL OR h_existente <> h)            THEN 'INSERE'
        END AS acao,
        CASE WHEN origem = 'S_DENTRO' THEN NULLIF(fim_nova, '9999-12') ELSE valid_until_novo END AS valid_until
    FROM pareada
)
-- Projeção própria de socios: as colunas *_origem vêm da linha da bronze (M-1 para a inativa), nunca do ponto.
SELECT pk,
       mes                                             AS valid_since,
       acao,
       valid_until,
       valid_until IS NULL                             AS is_current,
       CASE WHEN origem <> 'B' THEN h END              AS hash_atual,     -- identifica a versão existente no MERGE
       CASE WHEN origem = 'B' THEN mes_origem END      AS mes_origem,     -- M-1 para a inativa
       CASE WHEN origem = 'B' THEN hash_origem END     AS hash_origem,    -- hash da linha da bronze, não o da inativa
       CASE WHEN origem = 'B' THEN ts_origem END       AS ts_origem,
       CASE WHEN origem = 'B' THEN vinculo_ativo END   AS vinculo_ativo
FROM acoes
WHERE acao IS NOT NULL;

-- ---------------------------------------------------------------------------------------------------------
-- 3. Meses densos (mais de 20% das linhas da bronze do mês abrem versão): entram inteiros na busca.
--    Em socios a busca também lê o mês anterior a mes_ini, de onde vêm os atributos das saídas em mes_ini.
-- ---------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE tcc_receita.silver_intermediate.tmp_scd2_meses_socios AS
WITH params AS (
    SELECT mes_ini, mes_fim,
           date_format(add_months(CAST(try_to_timestamp(mes_ini, 'yyyy-MM') AS DATE), -1), 'yyyy-MM') AS mes_ant
    FROM (
        SELECT
            COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
            COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
    )
),
bronze_mes AS (
    SELECT b.mes_referencia AS mes, COUNT(*) AS n_bronze
    FROM tcc_receita.bronze.socios b
    CROSS JOIN params p
    WHERE b.mes_referencia BETWEEN p.mes_ant AND p.mes_fim
    GROUP BY b.mes_referencia
),
novas_mes AS (
    SELECT mes_origem AS mes, COUNT(*) AS n_insere
    FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_socios
    WHERE acao = 'INSERE'
    GROUP BY mes_origem
)
SELECT b.mes, b.n_bronze, COALESCE(n.n_insere, 0) AS n_insere,
       COALESCE(n.n_insere, 0) > 0.2 * b.n_bronze AS denso
FROM bronze_mes b
LEFT JOIN novas_mes n ON b.mes = n.mes;

-- ---------------------------------------------------------------------------------------------------------
-- 4. Guarda de memória da lista difundida (ver dados_simples.sql). O texto da mensagem não pode conter as
--    palavras I-N-S-E-R-T nem M-E-R-G-E (o run_sql.py deixaria de executar a asserção).
-- ---------------------------------------------------------------------------------------------------------
SELECT ASSERT_TRUE(
    COALESCE(SUM(CASE WHEN NOT denso THEN n_insere ELSE 0 END), 0) <= 25000000,
    CONCAT('Versoes novas em meses esparsos de socios: ',
           CAST(SUM(CASE WHEN NOT denso THEN n_insere ELSE 0 END) AS STRING),
           ' (limite 25 M para a lista difundida). Rode a janela em blocos menores.')
) AS lista_cabe_na_memoria
FROM tcc_receita.silver_intermediate.tmp_scd2_meses_socios;

-- ---------------------------------------------------------------------------------------------------------
-- 5. Fase 2 — um único MERGE (atômico) aplica as três ações (detalhes em dados_simples.sql).
--    Uma mesma linha da bronze pode gerar duas versões (ativa em M-1 e inativa em M); as duas entram,
--    porque valid_since difere.
-- ---------------------------------------------------------------------------------------------------------
MERGE INTO tcc_receita.silver_intermediate.socios AS t
USING (
    WITH params AS (
        SELECT mes_ini, mes_fim,
               date_format(add_months(CAST(try_to_timestamp(mes_ini, 'yyyy-MM') AS DATE), -1), 'yyyy-MM') AS mes_ant
        FROM (
            SELECT
                COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
                COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM'))) AS mes_fim
        )
    ),
    -- Chaves (pk, mês de origem) das versões novas dos meses esparsos, como inteiros de 64 bits.
    chaves_esparsas AS (
        SELECT DISTINCT xxhash64(a.pk, a.mes_origem) AS chave
        FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_socios a
        JOIN tcc_receita.silver_intermediate.tmp_scd2_meses_socios mm
          ON a.mes_origem = mm.mes AND NOT mm.denso
        WHERE a.acao = 'INSERE'
    ),
    -- Um único scan da bronze (mês anterior + janela), filtrado por joins broadcast: sem troca de linhas largas.
    bronze_filtrada AS (
        SELECT /*+ BROADCAST(m), BROADCAST(k) */ b.*
        FROM tcc_receita.bronze.socios b
        CROSS JOIN params p
        JOIN tcc_receita.silver_intermediate.tmp_scd2_meses_socios m
          ON b.mes_referencia = m.mes
        LEFT JOIN chaves_esparsas k
          ON xxhash64(b.pk, b.mes_referencia) = k.chave
        WHERE b.mes_referencia BETWEEN p.mes_ant AND p.mes_fim
          AND (m.denso OR k.chave IS NOT NULL)
    ),
    novas AS (
        SELECT * FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_socios WHERE acao = 'INSERE'
    )
    -- (1) Ações sobre versões existentes: os atributos não importam (o UPDATE só mexe na vigência).
    SELECT a.pk, a.valid_since, a.acao, a.hash_atual, a.valid_until, a.is_current,
           CAST(NULL AS STRING)    AS cnpj_basico,
           CAST(NULL AS STRING)    AS identificador_socio,
           CAST(NULL AS STRING)    AS nome_socio_razao_social,
           CAST(NULL AS STRING)    AS cnpj_cpf_socio,
           CAST(NULL AS STRING)    AS qualificacao_socio,
           CAST(NULL AS STRING)    AS data_entrada_sociedade,
           CAST(NULL AS STRING)    AS pais,
           CAST(NULL AS STRING)    AS representante_legal,
           CAST(NULL AS STRING)    AS nome_representante,
           CAST(NULL AS STRING)    AS qualificacao_representante_legal,
           CAST(NULL AS STRING)    AS faixa_etaria,
           CAST(NULL AS BOOLEAN)   AS vinculo_ativo,
           CAST(NULL AS TIMESTAMP) AS inserted_at,
           CAST(NULL AS STRING)    AS change_hash
    FROM tcc_receita.silver_intermediate.tmp_scd2_acoes_socios a
    WHERE a.acao IN ('APAGA', 'AJUSTA_FIM')
    UNION ALL
    -- (2) Versões novas (ativas e inativas): atributos da linha da bronze de origem, com a limpeza de sempre.
    --     Join exato por (pk, mês de origem, hash, carimbo) = a linha que venceu o desempate da fase 1.
    SELECT n.pk, n.valid_since, 'INSERE' AS acao, CAST(NULL AS STRING) AS hash_atual, n.valid_until, n.is_current,
           NULLIF(TRIM(b.cnpj_basico), '') AS cnpj_basico,
           NULLIF(TRIM(b.identificador_socio), '') AS identificador_socio,
           NULLIF(UPPER(TRIM(regexp_replace(b.nome_socio_razao_social, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS nome_socio_razao_social,
           NULLIF(TRIM(b.cnpj_cpf_socio), '') AS cnpj_cpf_socio,
           NULLIF(TRIM(b.qualificacao_socio), '') AS qualificacao_socio,
           CASE
               WHEN CAST(try_to_timestamp(TRIM(b.data_entrada_sociedade), 'yyyyMMdd') AS DATE) IS NOT NULL
               THEN TRIM(b.data_entrada_sociedade)
               ELSE NULL
           END AS data_entrada_sociedade,
           NULLIF(TRIM(b.pais), '') AS pais,
           NULLIF(TRIM(b.representante_legal), '') AS representante_legal,
           NULLIF(UPPER(TRIM(regexp_replace(b.nome_representante, r'[^\p{L}\p{M}\p{N} ]', ''))), '') AS nome_representante,
           NULLIF(TRIM(b.qualificacao_representante_legal), '') AS qualificacao_representante_legal,
           NULLIF(TRIM(b.faixa_etaria), '') AS faixa_etaria,
           n.vinculo_ativo,
           b.bronze_processed_at AS inserted_at,
           -- ativa: hash da bronze; inativa: hash da bronze de M-1 combinado com o estado FALSE
           IF(n.vinculo_ativo,
              b.change_hash,
              sha2(to_json(named_struct('source_change_hash', b.change_hash, 'vinculo_ativo', FALSE)), 256)) AS change_hash
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
    pk, cnpj_basico, identificador_socio, nome_socio_razao_social,
    cnpj_cpf_socio, qualificacao_socio, data_entrada_sociedade,
    pais, representante_legal, nome_representante,
    qualificacao_representante_legal, faixa_etaria,
    vinculo_ativo, mes_referencia, inserted_at, updated_at, change_hash,
    valid_since, valid_until, is_current
) VALUES (
    s.pk, s.cnpj_basico, s.identificador_socio, s.nome_socio_razao_social,
    s.cnpj_cpf_socio, s.qualificacao_socio, s.data_entrada_sociedade,
    s.pais, s.representante_legal, s.nome_representante,
    s.qualificacao_representante_legal, s.faixa_etaria,
    s.vinculo_ativo, s.valid_since, s.inserted_at, current_timestamp(), s.change_hash,
    s.valid_since, s.valid_until, s.is_current
);

-- ---------------------------------------------------------------------------------------------------------
-- 6. Limpeza das tabelas de trabalho (sobra de execução interrompida é inofensiva: é recriada).
-- ---------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS tcc_receita.silver_intermediate.tmp_scd2_acoes_socios;
DROP TABLE IF EXISTS tcc_receita.silver_intermediate.tmp_scd2_meses_socios;
