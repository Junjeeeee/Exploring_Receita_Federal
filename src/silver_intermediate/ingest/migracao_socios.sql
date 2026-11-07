-- MIGRAÇÃO OPCIONAL: execute somente se a tabela Silver socios JÁ EXISTIR.
-- Antes, rode DESCRIBE TABLE tcc_receita.silver_intermediate.socios.
-- Se uma das colunas já existir, remova-a da lista abaixo antes de executar.
-- Se a tabela ainda não existir, NÃO execute este arquivo: socios.sql já cria o schema novo.

ALTER TABLE tcc_receita.silver_intermediate.socios
ADD COLUMNS (
    qualificacao_socio STRING COMMENT 'Código da qualificação do sócio na sociedade.',
    vinculo_ativo BOOLEAN COMMENT 'TRUE quando a relação aparece no snapshot mensal; FALSE a partir do primeiro snapshot completo em que deixa de aparecer.'
);

-- O histórico preexistente não contém marcador de saída; assumimos TRUE para as versões antigas.
UPDATE tcc_receita.silver_intermediate.socios
SET vinculo_ativo = TRUE
WHERE vinculo_ativo IS NULL;


-- Recupera a qualificação histórica a partir do mesmo snapshot de origem da versão,
-- quando o campo ainda estava ausente na antiga Silver.
MERGE INTO tcc_receita.silver_intermediate.socios AS target
USING (
    SELECT
        pk,
        mes_referencia,
        MAX_BY(NULLIF(TRIM(qualificacao_socio), ''), bronze_processed_at) AS qualificacao_socio
    FROM tcc_receita.bronze.socios
    GROUP BY pk, mes_referencia
) AS origem
ON target.pk = origem.pk
AND target.valid_since = origem.mes_referencia
WHEN MATCHED
 AND target.qualificacao_socio IS NULL
 AND origem.qualificacao_socio IS NOT NULL
THEN UPDATE SET target.qualificacao_socio = origem.qualificacao_socio;
