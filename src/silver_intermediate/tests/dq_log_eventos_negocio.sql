-- DQ do log de eventos de negócio (spec 2026-11-14-semantica-eventos-e-fluxos, seção 2.4).
-- Parâmetros vazios assumem o mês anterior (mesmo comportamento de antes); com literais, usa a janela dada.
WITH params AS (
    SELECT
        COALESCE(NULLIF(TRIM('{{mes_referencia_inicial}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_ini,
        COALESCE(NULLIF(TRIM('{{mes_referencia_final}}'), ''), date_format(add_months(current_date(), -1), 'yyyy-MM')) AS mes_fim,
        -- Baseline = primeiro snapshot (carga inicial): não gera evento nenhum.
        (SELECT MIN(valid_since) FROM tcc_receita.silver_intermediate.estabelecimentos) AS baseline_est
),
janela AS (
    SELECT l.*
    FROM tcc_receita.silver_intermediate.log_eventos_negocio l
    CROSS JOIN params p
    WHERE l.mes_referencia >= p.mes_ini
      AND l.mes_referencia <= p.mes_fim
),
por_evento AS (
    SELECT
        l.evento,
        COUNT(*) AS total,
        SUM(CASE WHEN l.pk IS NULL OR TRIM(l.pk) = '' THEN 1 ELSE 0 END) AS pks_nulas,
        -- valor_novo NULL só é legítimo quando o estabelecimento remove TODOS os CNAEs secundários
        -- (lista anterior preenchida → vazia, B18). Nos demais eventos o novo estado é sempre definido.
        SUM(CASE WHEN l.evento IS NULL
                   OR (l.valor_novo IS NULL
                       AND NOT (l.evento = 'MUDOU LISTA CNAE SECUNDARIO' AND l.valor_antigo IS NOT NULL))
                 THEN 1 ELSE 0 END) AS incompletos,
        -- Nenhum evento no baseline (ou antes dele).
        SUM(CASE WHEN l.mes_referencia <= p.baseline_est THEN 1 ELSE 0 END) AS no_baseline,
        -- Estabelecimento sempre diz se é matriz; EMPRESA NASCEU é sempre da matriz.
        SUM(CASE WHEN (l.entidade_tipo = 'ESTABELECIMENTO' AND l.eh_matriz IS NULL)
                   OR (l.evento = 'EMPRESA NASCEU' AND l.eh_matriz IS DISTINCT FROM TRUE)
                 THEN 1 ELSE 0 END) AS eh_matriz_invalido,
        -- codigo_motivo só existe em MUDOU ESTADO CADASTRAL.
        SUM(CASE WHEN l.codigo_motivo IS NOT NULL AND l.evento <> 'MUDOU ESTADO CADASTRAL'
                 THEN 1 ELSE 0 END) AS motivo_fora_lugar,
        -- LGPD: em eventos de sócio, só a saída carrega informação (a qualificação). Nada de CNPJ/documento.
        SUM(CASE WHEN l.entidade_tipo = 'SOCIO' AND l.evento <> 'SOCIO SAIU DO QSA'
                  AND l.informacao_adicional IS NOT NULL
                 THEN 1 ELSE 0 END) AS socio_com_info,
        -- data_evento de EMPRESA NASCEU posterior ao fim do mês de detecção + 31 dias de folga.
        SUM(CASE WHEN l.evento = 'EMPRESA NASCEU'
                  AND l.data_evento > date_add(
                        last_day(CAST(try_to_timestamp(CONCAT(l.mes_referencia, '-01'), 'yyyy-MM-dd') AS DATE)), 31)
                 THEN 1 ELSE 0 END) AS nasceu_data_futura
    FROM janela l
    CROSS JOIN params p
    GROUP BY l.evento
),
-- Duplicatas na janela. GROUP BY trata NULL como um valor comum (NULL agrupa com NULL),
-- então a comparação já é null-safe sem COALESCE.
duplicatas AS (
    SELECT COUNT(*) AS grupos_duplicados
    FROM (
        SELECT pk, entidade_tipo, evento, mes_referencia, valor_antigo, valor_novo, codigo_motivo
        FROM janela
        GROUP BY pk, entidade_tipo, evento, mes_referencia, valor_antigo, valor_novo, codigo_motivo
        HAVING COUNT(*) > 1
    ) d
),
-- EMPRESA NASCEU no máximo uma vez por pk no histórico INTEIRO (não só na janela).
nascimentos_repetidos AS (
    SELECT COUNT(*) AS pks_nascidas_mais_de_uma_vez
    FROM (
        SELECT pk
        FROM tcc_receita.silver_intermediate.log_eventos_negocio
        WHERE evento = 'EMPRESA NASCEU'
        GROUP BY pk
        HAVING COUNT(*) > 1
    ) r
),
-- cnpj_basico preenchido em TODOS os meses do log, não só na janela (fase 3, U1). Se a migração recriou o log
-- mas a carga completa foi pulada, ou se a tabela antiga sem a coluna sobreviveu, isso aparece aqui.
-- Leitura de duas colunas da tabela inteira (barata).
cnpj_nulo AS (
    SELECT
        COUNT_IF(cnpj_basico IS NULL) AS eventos_sem_cnpj,
        COUNT(DISTINCT CASE WHEN cnpj_basico IS NULL THEN mes_referencia END) AS meses_com_cnpj_nulo
    FROM tcc_receita.silver_intermediate.log_eventos_negocio
),
-- Cobertura de meses, independente da janela: todo mês com versões novas de estabelecimentos (depois do
-- baseline) tem de ter eventos no log, e vice-versa. Pega a migração seguida de uma janela parcial (o log
-- recriado ficaria só com os meses dessa janela). Lê só a coluna de mês das duas tabelas.
-- Só até o fim da janela: numa carga em blocos, os meses depois do bloco ainda não foram processados.
meses_fonte AS (
    SELECT DISTINCT e.valid_since AS mes
    FROM tcc_receita.silver_intermediate.estabelecimentos e
    CROSS JOIN params p
    WHERE e.valid_since > p.baseline_est
      AND e.valid_since <= p.mes_fim
),
meses_log AS (
    SELECT DISTINCT l.mes_referencia AS mes
    FROM tcc_receita.silver_intermediate.log_eventos_negocio l
    CROSS JOIN params p
    WHERE l.mes_referencia <= p.mes_fim
),
cobertura AS (
    SELECT
        (SELECT COUNT(*) FROM (SELECT mes FROM meses_fonte EXCEPT SELECT mes FROM meses_log)) AS meses_faltando,
        (SELECT COUNT(*) FROM (SELECT mes FROM meses_log EXCEPT SELECT mes FROM meses_fonte)) AS meses_sobrando,
        (SELECT MIN(mes) FROM (SELECT mes FROM meses_fonte EXCEPT SELECT mes FROM meses_log)) AS exemplo_faltando
),
metricas AS (
    SELECT
        COALESCE(SUM(total), 0) AS total_eventos_janela,
        COALESCE(SUM(pks_nulas), 0) AS pks_nulas,
        COALESCE(SUM(incompletos), 0) AS eventos_incompletos,
        COALESCE(SUM(no_baseline), 0) AS eventos_no_baseline,
        COALESCE(SUM(eh_matriz_invalido), 0) AS eh_matriz_invalido,
        COALESCE(SUM(motivo_fora_lugar), 0) AS motivo_fora_lugar,
        COALESCE(SUM(socio_com_info), 0) AS socio_com_info,
        COALESCE(SUM(CASE WHEN evento = 'EMPRESA NASCEU' THEN total END), 0) AS total_nasceu,
        COALESCE(SUM(nasceu_data_futura), 0) AS nasceu_data_futura,
        -- Detalhe por tipo, para o erro dizer de onde vem o problema.
        CONCAT_WS(', ', COLLECT_LIST(CASE WHEN incompletos > 0
                                          THEN CONCAT(COALESCE(evento, 'NULL'), '=', incompletos) END)) AS detalhe
    FROM por_evento
)
SELECT
    -- Janela inteira no baseline (ou antes): zero eventos é o esperado, então o teste não se aplica.
    ASSERT_TRUE(m.total_eventos_janela > 0 OR p.mes_fim <= p.baseline_est,
        'ERRO DQ [log_eventos_negocio]: Nenhum evento gerado para o período!'),
    ASSERT_TRUE(m.pks_nulas = 0,
        CONCAT('ERRO DQ [log_eventos_negocio]: ', m.pks_nulas, ' eventos com PK nula!')),
    ASSERT_TRUE(m.eventos_incompletos = 0,
        CONCAT('ERRO DQ [log_eventos_negocio]: ', m.eventos_incompletos, ' eventos sem tipo ou valor_novo! Por tipo: ', m.detalhe)),
    ASSERT_TRUE(m.eventos_no_baseline = 0,
        CONCAT('ERRO DQ [log_eventos_negocio]: ', m.eventos_no_baseline, ' eventos com mes_referencia no baseline (', p.baseline_est, ') ou antes!')),
    ASSERT_TRUE(d.grupos_duplicados = 0,
        CONCAT('ERRO DQ [log_eventos_negocio]: ', d.grupos_duplicados, ' grupos duplicados de (pk, entidade_tipo, evento, mes_referencia, valor_antigo, valor_novo, codigo_motivo)!')),
    ASSERT_TRUE(r.pks_nascidas_mais_de_uma_vez = 0,
        CONCAT('ERRO DQ [log_eventos_negocio]: ', r.pks_nascidas_mais_de_uma_vez, ' empresas com mais de um EMPRESA NASCEU no histórico!')),
    ASSERT_TRUE(m.eh_matriz_invalido = 0,
        CONCAT('ERRO DQ [log_eventos_negocio]: ', m.eh_matriz_invalido, ' eventos de estabelecimento sem eh_matriz ou EMPRESA NASCEU com eh_matriz diferente de TRUE!')),
    ASSERT_TRUE(m.motivo_fora_lugar = 0,
        CONCAT('ERRO DQ [log_eventos_negocio]: ', m.motivo_fora_lugar, ' eventos com codigo_motivo fora de MUDOU ESTADO CADASTRAL!')),
    ASSERT_TRUE(m.socio_com_info = 0,
        CONCAT('ERRO DQ [log_eventos_negocio]: ', m.socio_com_info, ' eventos de sócio (exceto saída) com informacao_adicional preenchida!')),
    -- data_inicio_atividade é declarada pela Receita e pode ter erro de digitação isolado. Falha só se
    -- mais de 0,1% dos nascimentos da janela tiverem data posterior ao mês de detecção + 31 dias
    -- (sinal de erro sistemático, ex.: mes_referencia trocado pela data declarada).
    ASSERT_TRUE(m.nasceu_data_futura * 1000 <= m.total_nasceu,
        CONCAT('ERRO DQ [log_eventos_negocio]: ', m.nasceu_data_futura, ' de ', m.total_nasceu,
               ' EMPRESA NASCEU com data_evento após o fim do mês de detecção + 31 dias (> 0,1%)!')),
    ASSERT_TRUE(c.eventos_sem_cnpj = 0,
        CONCAT('ERRO DQ [log_eventos_negocio]: ', CAST(c.eventos_sem_cnpj AS STRING), ' eventos sem cnpj_basico em ',
               CAST(c.meses_com_cnpj_nulo AS STRING), ' mes(es) do historico (carga completa pulada?)')),
    ASSERT_TRUE(cob.meses_faltando = 0 AND cob.meses_sobrando = 0,
        CONCAT('ERRO DQ [log_eventos_negocio]: meses do log diferem dos meses com versoes novas de estabelecimentos (',
               CAST(cob.meses_faltando AS STRING), ' faltando, ex.: ', COALESCE(cob.exemplo_faltando, '?'), '; ',
               CAST(cob.meses_sobrando AS STRING), ' sobrando). Carga completa pulada?'))
FROM metricas m
CROSS JOIN params p
CROSS JOIN duplicatas d
CROSS JOIN nascimentos_repetidos r
CROSS JOIN cnpj_nulo c
CROSS JOIN cobertura cob;
