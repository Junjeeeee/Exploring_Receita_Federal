-- Migração única da fase 3 da otimização (spec docs/specs/features/2026-11-16-otimizacao-log-fato-dashboard.md, §3.6).
--
-- Por que existe: as tabelas abaixo são gravadas com CREATE TABLE IF NOT EXISTS + INSERT ... REPLACE WHERE da
-- janela. Uma mudança de schema (coluna nova) ou de forma de carga (de "recria tudo" para "só a janela") não chega
-- sozinha à tabela que já existe:
--   * log_eventos_negocio ganhou a coluna cnpj_basico (U1): a tabela antiga, sem a coluna, faria o INSERT falhar;
--     e meses antigos sem o valor deixariam o fato_mudanca_estado sem CNPJ ao reprocessá-los.
--
-- COMO RODAR (bloqueante — sem o passo 2 as séries ficam cortadas e o DQ de cobertura de meses falha):
--   1. Este script, UMA vez.
--   2. Logo em seguida, UMA execução com a janela inteira: mes_referencia_inicial = primeiro mês (2023-05) e
--      mes_referencia_final = último mês carregado nas SCD2, nas tarefas si_log_eventos_negocio e seu DQ.
--      (fato_mudanca_estado e OBT não precisam ser refeitos: os valores não mudam, só o log ganhou coluna.)
--
-- Nada aqui toca bronze nem as SCD2 (empresas, estabelecimentos, dados_simples, socios).

DROP TABLE IF EXISTS tcc_receita.silver_intermediate.log_eventos_negocio;
