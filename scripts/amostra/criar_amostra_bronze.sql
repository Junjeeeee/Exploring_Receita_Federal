-- Ambiente de amostra: cópia de 1% da bronze para validar silver/gold de ponta a ponta sem reprocessar a base.
--
-- A amostra é por CNPJ raiz (hash do cnpj_basico), então a história de cada empresa fica completa nas
-- quatro tabelas mensais: matriz, filiais, sócios e Simples do mesmo CNPJ entram juntos.
-- 1% ≈ 570 mil empresas, de todas as UFs. As tabelas de domínio são views para a bronze real (são pequenas).
--
-- Rodar com: python scripts/amostra/rodar_no_warehouse.py --criar-bronze
-- (este arquivo NÃO passa pela troca de schema: origem e destino estão escritos por extenso.)

CREATE SCHEMA IF NOT EXISTS tcc_receita.amostra_bronze;

CREATE OR REPLACE TABLE tcc_receita.amostra_bronze.empresas AS
SELECT * FROM tcc_receita.bronze.empresas
WHERE pmod(xxhash64(cnpj_basico), 100) = 0;

CREATE OR REPLACE TABLE tcc_receita.amostra_bronze.estabelecimentos AS
SELECT * FROM tcc_receita.bronze.estabelecimentos
WHERE pmod(xxhash64(cnpj_basico), 100) = 0;

CREATE OR REPLACE TABLE tcc_receita.amostra_bronze.dados_simples AS
SELECT * FROM tcc_receita.bronze.dados_simples
WHERE pmod(xxhash64(cnpj_basico), 100) = 0;

CREATE OR REPLACE TABLE tcc_receita.amostra_bronze.socios AS
SELECT * FROM tcc_receita.bronze.socios
WHERE pmod(xxhash64(cnpj_basico), 100) = 0;

CREATE OR REPLACE VIEW tcc_receita.amostra_bronze.cnaes_ibge AS SELECT * FROM tcc_receita.bronze.cnaes_ibge;
CREATE OR REPLACE VIEW tcc_receita.amostra_bronze.paises AS SELECT * FROM tcc_receita.bronze.paises;
CREATE OR REPLACE VIEW tcc_receita.amostra_bronze.municipios AS SELECT * FROM tcc_receita.bronze.municipios;
CREATE OR REPLACE VIEW tcc_receita.amostra_bronze.naturezas_juridicas AS SELECT * FROM tcc_receita.bronze.naturezas_juridicas;
CREATE OR REPLACE VIEW tcc_receita.amostra_bronze.qualificacoes_socios AS SELECT * FROM tcc_receita.bronze.qualificacoes_socios;
CREATE OR REPLACE VIEW tcc_receita.amostra_bronze.motivos_situacao AS SELECT * FROM tcc_receita.bronze.motivos_situacao;
CREATE OR REPLACE VIEW tcc_receita.amostra_bronze.ceps AS SELECT * FROM tcc_receita.bronze.ceps;
