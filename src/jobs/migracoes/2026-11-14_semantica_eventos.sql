-- Migração única (desenvolvimento, D-08): as quatro tabelas incrementais abaixo mudaram de schema
-- (spec docs/specs/features/2026-11-14-semantica-eventos-e-fluxos.md). Como são escritas com
-- CREATE TABLE IF NOT EXISTS + INSERT ... REPLACE WHERE, o schema antigo não se atualiza sozinho.
-- Rodar UMA vez, antes de reexecutar o Job a partir de si_log_eventos_negocio com a janela completa
-- (2023-05 até o último mês carregado). As dimensões são recriadas pelos próprios SQL (CREATE OR REPLACE).
--
-- Nada aqui toca bronze nem as SCD2 (empresas, estabelecimentos, dados_simples, socios).

DROP TABLE IF EXISTS tcc_receita.gold.fato_indicadores_setoriais_mes;
DROP TABLE IF EXISTS tcc_receita.gold.fato_empresa_mes;
DROP TABLE IF EXISTS tcc_receita.gold.obt_dashboard_eventos;
DROP TABLE IF EXISTS tcc_receita.silver_final.fato_mudanca_estado;
DROP TABLE IF EXISTS tcc_receita.silver_intermediate.log_eventos_negocio;
