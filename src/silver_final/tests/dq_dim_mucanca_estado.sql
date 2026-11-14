-- DQ dim_mudanca_estado (spec 2026-11-14, seção 2.3).
-- Não testa nulos de sk/nk/tipo/detalhe: são montados com COALESCE na carga e não poderiam falhar.
WITH metricas AS (
    SELECT
        COUNT(*) AS total_linhas,
        SUM(CASE WHEN sk_mudanca_estado = -1 THEN 1 ELSE 0 END) AS qtd_ghost_row,
        COUNT(DISTINCT sk_mudanca_estado) AS qtd_sk_distintas,
        COUNT(DISTINCT nk_mudanca_estado) AS qtd_nk_distintas,
        -- categoria fora da lista fechada: 12 categorias + ABERTURA + eventos que repetem o próprio nome + ghost row
        SUM(CASE WHEN COALESCE(
                    categoria_evento IN (
                        'REATIVACAO', 'FALENCIA OU LIQUIDACAO JUDICIAL', 'EXTINCAO POR OBITO (MEI)',
                        'REORGANIZACAO SOCIETARIA', 'BAIXA VOLUNTARIA', 'BAIXA DE OFICIO',
                        'BAIXA - OUTROS MOTIVOS', 'INAPTIDAO', 'SUSPENSAO', 'ANULACAO',
                        'MUDANCA DE MOTIVO', 'OUTRA MUDANCA CADASTRAL', 'ABERTURA'
                    )
                    OR (tipo_mudanca_estado NOT IN ('MUDOU ESTADO CADASTRAL', 'EMPRESA NASCEU')
                        AND categoria_evento = tipo_mudanca_estado)
                    OR sk_mudanca_estado = -1,
                    FALSE) THEN 0 ELSE 1 END) AS categorias_invalidas,
        -- estado cadastral nunca pode ficar com o nome do evento como categoria (classificação não aplicada)
        SUM(CASE WHEN tipo_mudanca_estado = 'MUDOU ESTADO CADASTRAL'
                  AND categoria_evento <=> tipo_mudanca_estado THEN 1 ELSE 0 END) AS estado_sem_classificar,
        SUM(CASE WHEN tipo_mudanca_estado = 'EMPRESA NASCEU'
                  AND NOT (categoria_evento <=> 'ABERTURA') THEN 1 ELSE 0 END) AS nascimento_sem_abertura
    FROM tcc_receita.silver_final.dim_mudanca_estado
)
SELECT
    ASSERT_TRUE(total_linhas > 1, 'ERRO DQ [dim_mudanca_estado]: Tabela sem eventos mapeados!'),
    ASSERT_TRUE(qtd_ghost_row = 1, 'ERRO DQ [dim_mudanca_estado]: Ghost row (-1) ausente ou duplicada!'),
    ASSERT_TRUE(qtd_sk_distintas = total_linhas AND qtd_nk_distintas = total_linhas,
        CONCAT('ERRO DQ [dim_mudanca_estado]: colisão/duplicidade de chave: ', total_linhas, ' linhas, ',
               qtd_sk_distintas, ' sk distintas, ', qtd_nk_distintas, ' nk distintas!')),
    ASSERT_TRUE(categorias_invalidas = 0,
        CONCAT('ERRO DQ [dim_mudanca_estado]: ', categorias_invalidas, ' linhas com categoria_evento nula ou fora da lista fechada!')),
    ASSERT_TRUE(estado_sem_classificar = 0,
        CONCAT('ERRO DQ [dim_mudanca_estado]: ', estado_sem_classificar, ' linhas de MUDOU ESTADO CADASTRAL com categoria igual ao nome do evento!')),
    ASSERT_TRUE(nascimento_sem_abertura = 0,
        CONCAT('ERRO DQ [dim_mudanca_estado]: ', nascimento_sem_abertura, ' linhas de EMPRESA NASCEU sem categoria ABERTURA!'))
FROM metricas;
