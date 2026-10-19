from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from typing import List
from src.bronze.data_quality import validar_schema, validar_volumetria

TABELAS_BRONZE = {
    "empresas": "empresa", "estabelecimentos": "estabele", "dados_simples": "simples",
    "socios": "socio", "paises": "paises", "municipios": "municipios",
    "qualificacoes_socios": "qualificacoes", "naturezas_juridicas": "naturezas", 
    "cnaes": "cnaes", "motivos_situacao": "motivos", "cnaes_ibge": "cnaes_ibge"
}

CHAVES_PRIMARIAS = {
    "empresas": ["cnpj_basico"],
    "estabelecimentos": ["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
    "dados_simples": ["cnpj_basico"],
    "socios": [],
    "paises": ["codigo"], "municipios": ["codigo"], "qualificacoes_socios": ["codigo"],
    "naturezas_juridicas": ["codigo"], "cnaes": ["codigo"],
    "motivos_situacao": ["codigo"], "cnaes_ibge": ["codigo"]
}

DESCRICAO_TABELAS = {
    "empresas": "Camada Bronze: Dados cadastrais da raiz do CNPJ (Empresa), incluindo razão social, capital social e porte.",
    "estabelecimentos": "Camada Bronze: Dados das filiais/matrizes (Estabelecimentos), contendo morada, contactos e situação cadastral detalhada.",
    "dados_simples": "Camada Bronze: Informações sobre a adesão e exclusão de empresas aos regimes do Simples Nacional e MEI.",
    "socios": "Camada Bronze: Quadro de Sócios e Administradores (QSA), detalhando o perfil, papel e entrada de cada membro na sociedade.",
    "paises": "Camada Bronze: Tabela de domínio com os códigos e descrições dos países padrão Receita Federal.",
    "municipios": "Camada Bronze: Tabela de domínio com os códigos e descrições dos municípios padrão Receita Federal.",
    "qualificacoes_socios": "Camada Bronze: Tabela de domínio com os códigos e descrições dos papéis/qualificações dos sócios e representantes.",
    "naturezas_juridicas": "Camada Bronze: Tabela de domínio com os códigos e descrições das naturezas jurídicas corporativas.",
    "cnaes": "Camada Bronze: Tabela de domínio com os códigos e descrições da Classificação Nacional de Atividades Econômicas (CNAE).",
    "motivos_situacao": "Camada Bronze: Tabela de domínio com os códigos e descrições dos motivos da situação cadastral.",
    "cnaes_ibge": "Camada Bronze: Hierarquia oficial da CNAE extraída do IBGE (Seção, Divisão, Grupo, Classe e Subclasse)."
}

DESCRICAO_COLUNAS = {
    "change_hash": "Metadado: Hash MD5 de todas as colunas brutas para deteção rigorosa de mutação (Change Data Capture).",
    "pk": "Metadado: Chave primária lógica gerada pela concatenação dinâmica da granularidade da tabela.",
    "mes_referencia": "Metadado: Mês e ano de referência do snapshot extraído da Receita Federal (Partição).",
    "inserted_at": "Metadado: Timestamp de ingestão inicial do registo na camada Bronze.",
    "updated_at": "Metadado: Timestamp da última atualização ou reprocessamento do registo na camada Bronze.",
    "codigo": "Código identificador único do domínio (PK).",
    "descricao": "Descrição textual detalhada do domínio.",
    "secao": "Código da Seção (Grande Área de Atividade) na hierarquia CNAE IBGE.",
    "nome_secao": "Nome da Seção (Grande Área de Atividade) na hierarquia CNAE IBGE.",
    "divisao": "Código da Divisão na hierarquia CNAE IBGE.",
    "nome_divisao": "Nome da Divisão na hierarquia CNAE IBGE.",
    "grupo": "Código do Grupo na hierarquia CNAE IBGE.",
    "nome_grupo": "Nome do Grupo na hierarquia CNAE IBGE.",
    "classe": "Código da Classe na hierarquia CNAE IBGE.",
    "nome_classe": "Nome da Classe na hierarquia CNAE IBGE.",
    "cnpj_basico": "Radical do CNPJ (primeiros 8 dígitos) que identifica a raiz corporativa da entidade.",
    "razao_social": "Nome empresarial oficial (Razão Social) registado na Receita Federal.",
    "natureza_juridica": "Código da natureza jurídica (ex: Sociedade Limitada, S.A.).",
    "qualificacao_responsavel": "Código da qualificação da pessoa física responsável pela empresa.",
    "capital_social": "Valor do capital social da empresa registado em Reais (BRL).",
    "porte_empresa": "Código indicativo do porte da empresa (ex: ME, EPP, Demais).",
    "ente_federativo_responsavel": "Órgão público responsável, preenchido para entes federativos.",
    "cnpj_ordem": "Número de ordem do estabelecimento (geralmente 0001 para matrizes e sequencial para filiais).",
    "cnpj_dv": "Dígitos verificadores do CNPJ (últimos 2 dígitos).",
    "identificador_matriz_filial": "Código indicador: 1 para Matriz, 2 para Filial.",
    "nome_fantasia": "Nome comercial de fachada (Nome Fantasia) do estabelecimento.",
    "situacao_cadastral": "Código do status operacional (ex: Ativa, Inapta, Baixada).",
    "data_situacao_cadastral": "Data da última alteração de status da situação cadastral (AAAAMMDD).",
    "motivo_situacao_cadastral": "Código do motivo da situação cadastral atual.",
    "nome_cidade_exterior": "Nome da cidade para estabelecimentos localizados no exterior.",
    "pais": "Código do país de localização do estabelecimento.",
    "data_inicio_atividade": "Data de início das operações comerciais do estabelecimento (AAAAMMDD).",
    "cnae_fiscal_principal": "Código da atividade econômica primária (CNAE) do estabelecimento.",
    "cnae_fiscal_secundaria": "Lista separada por vírgulas das atividades econômicas secundárias (CNAEs).",
    "tipo_logradouro": "Tipo de via do endereço (ex: Rua, Avenida, Praça).",
    "logradouro": "Nome do logradouro ou via do endereço.",
    "numero": "Número de porta do endereço do estabelecimento.",
    "complemento": "Informações complementares do endereço (ex: Sala, Andar, Bloco).",
    "bairro": "Nome do bairro ou distrito.",
    "cep": "Código de Endereçamento Postal (CEP) brasileiro.",
    "uf": "Sigla da Unidade Federativa (Estado) no Brasil.",
    "municipio": "Código do município onde o estabelecimento está situado.",
    "ddd_1": "Código de Discagem Direta a Distância (DDD) do telefone primário.",
    "telefone_1": "Número do telefone primário de contacto.",
    "ddd_2": "Código de Discagem Direta a Distância (DDD) do telefone secundário.",
    "telefone_2": "Número do telefone secundário de contacto.",
    "ddd_fax": "Código DDD da linha de fax.",
    "fax": "Número da linha de fax.",
    "correio_eletronico": "Endereço de e-mail institucional para contacto.",
    "situacao_especial": "Descrição de situações especiais (ex: Concordata, Recuperação Judicial).",
    "data_situacao_especial": "Data em que a situação especial foi estabelecida (AAAAMMDD).",
    "opcao_pelo_simples": "Flag de adesão ao regime tributário do Simples Nacional (S/N).",
    "data_opcao_simples": "Data oficial de adesão ao Simples Nacional (AAAAMMDD).",
    "data_exclusao_simples": "Data oficial de exclusão do Simples Nacional, se aplicável (AAAAMMDD).",
    "opcao_pelo_mei": "Flag de adesão ao regime tributário de Microempreendedor Individual - MEI (S/N).",
    "data_opcao_mei": "Data oficial de adesão ao MEI (AAAAMMDD).",
    "data_exclusao_mei": "Data oficial de exclusão do MEI, se aplicável (AAAAMMDD).",
    "identificador_socio": "Código identificador do tipo de sócio (1=PJ, 2=PF, 3=Estrangeiro).",
    "nome_socio_razao_social": "Nome civil do sócio pessoa física ou Razão Social do sócio pessoa jurídica.",
    "cnpj_cpf_socio": "Documento de identificação do sócio (CPF ou CNPJ).",
    "qualificacao_socio": "Código do papel exercido na sociedade (ex: Diretor, Presidente, Sócio-Administrador).",
    "data_entrada_sociedade": "Data de integração oficial do membro na sociedade (AAAAMMDD).",
    "representante_legal": "Documento de identificação (CPF) do representante legal do sócio.",
    "nome_representante": "Nome civil do representante legal designado.",
    "qualificacao_representante_legal": "Código do papel/qualificação do representante legal.",
    "faixa_etaria": "Código da faixa etária aproximada do sócio pessoa física."
}

def configurar_ambiente_unity_catalog(spark: SparkSession):
    spark.sql("CREATE CATALOG IF NOT EXISTS tcc_receita")
    spark.sql("CREATE SCHEMA IF NOT EXISTS tcc_receita.bronze")

def aplicar_comentarios_uc(spark: SparkSession, nome_completo_tabela: str, nome_tabela_uc: str, colunas_existentes: List[str]):
    if nome_tabela_uc in DESCRICAO_TABELAS:
        comentario_tab = DESCRICAO_TABELAS[nome_tabela_uc].replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {nome_completo_tabela} IS '{comentario_tab}'")
    for col in colunas_existentes:
        if col in DESCRICAO_COLUNAS:
            comentario_col = DESCRICAO_COLUNAS[col].replace("'", "''")
            spark.sql(f"ALTER TABLE {nome_completo_tabela} ALTER COLUMN {col} COMMENT '{comentario_col}'")

def ingerir_tabela_bronze(spark: SparkSession, nome_tabela_uc: str, pasta_s3: str, mes_referencia: str):
    caminho_landing = f"s3://lakehouse-receita/landing/receita_federal/{pasta_s3}/mes_referencia={mes_referencia}/"
    nome_completo_tabela = f"tcc_receita.bronze.{nome_tabela_uc}"
    print(f"🔄 Processando {nome_tabela_uc} (Mês: {mes_referencia})...")
    
    try:
        df_raw = spark.read.parquet(caminho_landing)
        validar_schema(df_raw, nome_tabela_uc)
        validar_volumetria(spark, df_raw, nome_tabela_uc, mes_referencia, threshold_queda=0.15)
        
        colunas_raw = df_raw.columns
        expr_hash = F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in colunas_raw]))
        df_transformado = df_raw.withColumn("change_hash", expr_hash)
        
        if nome_tabela_uc == "socios":
            expr_pk = F.when(
                F.col("identificador_socio") == "1",
                F.concat_ws("-", F.coalesce(F.col("cnpj_basico").cast("string"), F.lit("NA")), F.coalesce(F.col("cnpj_cpf_socio").cast("string"), F.lit("NA")))
            ).otherwise(
                F.concat_ws("-", F.coalesce(F.col("cnpj_basico").cast("string"), F.lit("NA")), F.coalesce(F.col("cnpj_cpf_socio").cast("string"), F.lit("NA")), F.coalesce(F.col("nome_socio_razao_social").cast("string"), F.lit("NA")))
            )
            df_transformado = df_transformado.withColumn("pk", expr_pk)
        else:
            pk_cols = CHAVES_PRIMARIAS.get(nome_tabela_uc, [])
            if pk_cols:
                expr_pk = F.concat_ws("-", *[F.coalesce(F.col(c).cast("string"), F.lit("NA")) for c in pk_cols])
                df_transformado = df_transformado.withColumn("pk", expr_pk)
            
        df_bronze = df_transformado \
            .withColumn("mes_referencia", F.lit(mes_referencia)) \
            .withColumn("inserted_at", F.current_timestamp()) \
            .withColumn("updated_at", F.current_timestamp())
        
        if "ingested_at" in df_bronze.columns:
            df_bronze = df_bronze.drop("ingested_at")

        (df_bronze.write
            .format("delta")
            .mode("overwrite") 
            .option("replaceWhere", f"mes_referencia = '{mes_referencia}'")
            .option("mergeSchema", "true") 
            .partitionBy("mes_referencia")
            .saveAsTable(nome_completo_tabela))
        
        aplicar_comentarios_uc(spark, nome_completo_tabela, nome_tabela_uc, df_bronze.columns)
        print(f"✅ Ingestão e documentação finalizadas: {nome_completo_tabela}\n")
    except Exception as e:
        print(f"⚠️ Erro ao processar {nome_tabela_uc} ({mes_referencia}): {str(e)}\n")
        raise e

def executar_pipeline_bronze(spark: SparkSession, meses_para_processar: List[str]):
    configurar_ambiente_unity_catalog(spark)
    for mes in meses_para_processar:
        for nome_uc, pasta_s3 in TABELAS_BRONZE.items():
            ingerir_tabela_bronze(spark, nome_uc, pasta_s3, mes)

if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    meses_alvo = ["2023-05", "2023-06", "2023-07", "2023-08"]
    executar_pipeline_bronze(spark_session, meses_alvo)