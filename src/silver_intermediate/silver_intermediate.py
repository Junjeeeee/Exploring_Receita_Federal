from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from typing import List

TABELAS_PROCESSAMENTO = ["empresas", "estabelecimentos", "dados_simples", "socios"]

def aplicar_limpezas_silver(df, nome_tabela):
    """Aplica as regras de negócio, regex e deduplicação antes de fundir com a Silver."""
    colunas = df.columns

    # 1. Normalização de Strings Vagas (Trata vazios como NULL nativo)
    for c in colunas:
        if dict(df.dtypes)[c] == "string":
            df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.col(c)))

    # 2. Tratamento de Anomalias Temporais
    colunas_data = [c for c in colunas if c.startswith("data_")]
    for c in colunas_data:
        df = df.withColumn(c, 
            F.when((F.length(F.col(c)) < 8) | (F.col(c) == "00000000"), F.lit(None))
             .otherwise(F.col(c))
        )

    # 3. Limpeza Lexical (Regex em textos livres)
    colunas_texto = ["razao_social", "nome_fantasia", "bairro", "logradouro", "complemento", "nome_socio_razao_social"]
    for c in colunas_texto:
        if c in colunas:
            df = df.withColumn(c, F.upper(F.trim(F.regexp_replace(F.col(c), "[^a-zA-Z0-9 ]", ""))))

    # 4. Enriquecimento: Padronização de Contactos
    if "telefone_1" in colunas and "ddd_1" in colunas:
        df = df.withColumn("telefone_1_formatado", 
            F.when(F.col("ddd_1").isNotNull() & F.col("telefone_1").isNotNull() & (F.col("telefone_1") != "00000000"), 
            F.concat(F.lit("("), F.col("ddd_1"), F.lit(") "), F.col("telefone_1"))).otherwise(F.lit(None)))
    if "telefone_2" in colunas and "ddd_2" in colunas:
        df = df.withColumn("telefone_2_formatado", 
            F.when(F.col("ddd_2").isNotNull() & F.col("telefone_2").isNotNull(), 
            F.concat(F.lit("("), F.col("ddd_2"), F.lit(") "), F.col("telefone_2"))).otherwise(F.lit(None)))

    # 5. Enriquecimento: Decodificação de Domínios Inline (Mantendo Nulos e Booleans)
    if "situacao_cadastral" in colunas:
        df = df.withColumn("situacao_cadastral_desc",
            F.when(F.col("situacao_cadastral").isin("02", "2"), "ATIVA")
             .when(F.col("situacao_cadastral").isin("04", "4"), "INAPTA")
             .when(F.col("situacao_cadastral").isin("03", "3"), "SUSPENSA")
             .when(F.col("situacao_cadastral").isin("08", "8"), "BAIXADA")
             .when(F.col("situacao_cadastral").isin("01", "1"), "NULA")
             .otherwise(F.lit(None))
        )
    if "opcao_pelo_simples" in colunas:
        df = df.withColumn("opcao_pelo_simples_bool", 
            F.when(F.col("opcao_pelo_simples") == "S", F.lit(True))
             .when(F.col("opcao_pelo_simples") == "N", F.lit(False))
             .otherwise(F.lit(None).cast("boolean"))
        )
    if "opcao_pelo_mei" in colunas:
        df = df.withColumn("opcao_pelo_mei_bool", 
            F.when(F.col("opcao_pelo_mei") == "S", F.lit(True))
             .when(F.col("opcao_pelo_mei") == "N", F.lit(False))
             .otherwise(F.lit(None).cast("boolean"))
        )

    # 6. Deduplicação de Registos (Achatamento com MAX)
    expr_agg = [F.max(c).alias(c) for c in df.columns if c != "pk"]
    df_dedup = df.groupBy("pk").agg(*expr_agg)

    return df_dedup

def processar_scd2_silver(spark: SparkSession, nome_tabela: str, mes_atual: str):
    tabela_bronze = f"tcc_receita.bronze.{nome_tabela}"
    tabela_silver = f"tcc_receita.silver_intermediate.{nome_tabela}"
    
    print(f"🔄 Iniciando SCD2 para {nome_tabela} (Mês: {mes_atual})")
    
    df_bronze_bruto = spark.read.table(tabela_bronze).filter(F.col("mes_referencia") == mes_atual)
    df_bronze_limpo = aplicar_limpezas_silver(df_bronze_bruto, nome_tabela)
    
    if not spark.catalog.tableExists(tabela_silver):
        print(f"   -> Criando {tabela_silver} do zero.")
        df_bronze_limpo \
            .withColumn("valid_since", F.lit(mes_atual)) \
            .withColumn("valid_until", F.lit("9999-12-31")) \
            .withColumn("is_current", F.lit(True)) \
            .write.format("delta").partitionBy("is_current").saveAsTable(tabela_silver)
        return

    df_silver_ativa = spark.read.table(tabela_silver).filter(F.col("is_current") == True)
    
    df_mudancas = df_bronze_limpo.alias("b").join(
        df_silver_ativa.alias("s"),
        on="pk", how="inner"
    ).filter(F.col("b.change_hash") != F.col("s.change_hash")).select("b.*")
    
    stg_inserts = df_bronze_limpo.withColumn("merge_key", F.col("pk"))
    stg_updates = df_mudancas.withColumn("merge_key", F.lit(None))
    df_staging = stg_inserts.unionByName(stg_updates)
    
    tabela_alvo = DeltaTable.forName(spark, tabela_silver)
    insert_dict = {col: f"staging.{col}" for col in df_bronze_limpo.columns}
    insert_dict.update({"valid_since": f"'{mes_atual}'", "valid_until": "'9999-12-31'", "is_current": "True"})

    (tabela_alvo.alias("target").merge(df_staging.alias("staging"), "target.pk = staging.merge_key")
     .whenMatchedUpdate(
        condition="target.is_current = True AND target.change_hash != staging.change_hash",
        set={"is_current": "False", "valid_until": f"'{mes_atual}'", "updated_at": "current_timestamp()"}
     )
     .whenNotMatchedInsert(values=insert_dict)
     .execute())

def gerar_log_eventos_negocio(spark: SparkSession, mes_atual: str):
    """Avalia as 7 regras de negócio usando datas reais de negócio e salva na silver_intermediate."""
    tabela_log = "tcc_receita.silver_intermediate.log_eventos_negocio"
    anomes = mes_atual.replace("-", "")
    print(f"📊 Avaliando Eventos de Negócio (Mês: {mes_atual})...")

    df_emp = spark.read.table("tcc_receita.silver_intermediate.empresas")
    df_est = spark.read.table("tcc_receita.silver_intermediate.estabelecimentos")
    df_sim = spark.read.table("tcc_receita.silver_intermediate.dados_simples")

    df_motivos = spark.read.table("tcc_receita.bronze.motivos_situacao") \
        .filter(F.col("mes_referencia") == mes_atual) \
        .select(
            F.col("codigo").alias("cod_motivo"), 
            F.upper(F.trim(F.col("descricao"))).alias("desc_motivo")
        ).dropDuplicates(["cod_motivo"])

    def get_mudancas(df):
        old = df.filter(F.col("valid_until") == mes_atual).alias("old")
        new = df.filter(F.col("valid_since") == mes_atual).alias("new")
        return old.join(new, "pk", "inner")

    mud_emp = get_mudancas(df_emp)
    mud_est = get_mudancas(df_est)
    mud_sim = get_mudancas(df_sim)

    eventos = []

    # 1. EMPRESA NASCEU (Baseado em data_inicio_atividade da Matriz no mês de referência)
    nascimentos = df_est.filter(
        (F.col("identificador_matriz_filial") == "1") &
        (F.substring(F.col("data_inicio_atividade"), 1, 6) == anomes)
    ).select(
        F.col("cnpj_basico").alias("pk"),
        F.lit("EMPRESA NASCEU").alias("evento"),
        F.lit(None).cast("string").alias("valor_antigo"),
        F.lit("ATIVA").alias("valor_novo"),
        F.col("data_inicio_atividade").alias("informacao_adicional")
    ).dropDuplicates(["pk"])
    eventos.append(nascimentos)

    # 2. MUDOU ESTADO CADASTRAL (SCD2 para meses seguintes + data_situacao_cadastral para 2023-05)
    if mes_atual == "2023-05":
        est_estado = df_est.filter(
            (F.col("valid_since") == mes_atual) &
            (F.substring(F.col("data_situacao_cadastral"), 1, 6) == anomes) &
            (F.col("situacao_cadastral_desc") != "ATIVA")
        ).join(df_motivos, F.col("motivo_situacao_cadastral") == F.col("cod_motivo"), "left") \
        .select(
            "pk",
            F.lit("MUDOU ESTADO CADASTRAL").alias("evento"),
            F.lit(None).cast("string").alias("valor_antigo"),
            F.col("situacao_cadastral_desc").alias("valor_novo"),
            F.concat_ws(" - ", F.col("motivo_situacao_cadastral"), F.col("desc_motivo")).alias("informacao_adicional")
        )
    else:
        est_estado = mud_est.filter(
            F.coalesce(F.col("old.situacao_cadastral_desc"), F.lit("")) != F.coalesce(F.col("new.situacao_cadastral_desc"), F.lit(""))
        ).join(df_motivos, F.col("new.motivo_situacao_cadastral") == F.col("cod_motivo"), "left") \
        .select(
            "pk", 
            F.lit("MUDOU ESTADO CADASTRAL").alias("evento"), 
            F.col("old.situacao_cadastral_desc").alias("valor_antigo"), 
            F.col("new.situacao_cadastral_desc").alias("valor_novo"),
            F.concat_ws(" - ", F.col("new.motivo_situacao_cadastral"), F.col("desc_motivo")).alias("informacao_adicional")
        )
    eventos.append(est_estado)

    # 3. Saiu do MEI
    saiu_mei = mud_sim.filter((F.col("old.opcao_pelo_mei_bool") == True) & (F.col("new.opcao_pelo_mei_bool") == False)) \
        .select("pk", F.lit("SAIU DO MEI").alias("evento"), F.lit("True").alias("valor_antigo"), F.lit("False").alias("valor_novo"), F.lit(None).cast("string").alias("informacao_adicional"))
    eventos.append(saiu_mei)

    # 4. Entrou no SIMPLES
    entrou_simples = mud_sim.filter((F.col("old.opcao_pelo_simples_bool") == False) & (F.col("new.opcao_pelo_simples_bool") == True)) \
        .select("pk", F.lit("ENTROU NO SIMPLES").alias("evento"), F.lit("False").alias("valor_antigo"), F.lit("True").alias("valor_novo"), F.lit(None).cast("string").alias("informacao_adicional"))
    eventos.append(entrou_simples)

    # 5. Saiu do SIMPLES
    saiu_simples = mud_sim.filter((F.col("old.opcao_pelo_simples_bool") == True) & (F.col("new.opcao_pelo_simples_bool") == False)) \
        .select("pk", F.lit("SAIU DO SIMPLES").alias("evento"), F.lit("True").alias("valor_antigo"), F.lit("False").alias("valor_novo"), F.lit(None).cast("string").alias("informacao_adicional"))
    eventos.append(saiu_simples)

    # 6. Mudou de Natureza Jurídica
    mudou_nat = mud_emp.filter(F.coalesce(F.col("old.natureza_juridica"), F.lit("")) != F.coalesce(F.col("new.natureza_juridica"), F.lit(""))) \
        .select("pk", F.lit("MUDOU NATUREZA JURIDICA").alias("evento"), F.col("old.natureza_juridica").alias("valor_antigo"), F.col("new.natureza_juridica").alias("valor_novo"), F.lit(None).cast("string").alias("informacao_adicional"))
    eventos.append(mudou_nat)

    # 7. Mudou CNAE
    mudou_cnae = mud_est.filter(
        (F.coalesce(F.col("old.cnae_fiscal_principal"), F.lit("")) != F.coalesce(F.col("new.cnae_fiscal_principal"), F.lit(""))) | 
        (F.coalesce(F.col("old.cnae_fiscal_secundaria"), F.lit("")) != F.coalesce(F.col("new.cnae_fiscal_secundaria"), F.lit("")))
    ).select(
        "pk", 
        F.lit("MUDOU CNAE").alias("evento"), 
        F.col("old.cnae_fiscal_principal").alias("valor_antigo"), 
        F.col("new.cnae_fiscal_principal").alias("valor_novo"),
        F.col("new.cnae_fiscal_secundaria").alias("informacao_adicional")
    )
    eventos.append(mudou_cnae)

    df_eventos_finais = eventos[0]
    for df_ev in eventos[1:]:
        df_eventos_finais = df_eventos_finais.unionByName(df_ev)
    
    df_eventos_finais = df_eventos_finais.withColumn("mes_referencia", F.lit(mes_atual)).withColumn("gerado_em", F.current_timestamp())

    if not spark.catalog.tableExists(tabela_log):
        df_eventos_finais.write.format("delta").saveAsTable(tabela_log)
    else:
        spark.sql(f"DELETE FROM {tabela_log} WHERE mes_referencia = '{mes_atual}'")
        df_eventos_finais.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(tabela_log)

def executar_pipeline_silver(spark: SparkSession, meses_para_processar: List[str]):
    spark.sql("CREATE SCHEMA IF NOT EXISTS tcc_receita.silver_intermediate")
    for mes in meses_para_processar:
        for tabela in TABELAS_PROCESSAMENTO:
            processar_scd2_silver(spark, tabela, mes)
        gerar_log_eventos_negocio(spark, mes)

if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    meses_alvo = ["2023-05", "2023-06", "2023-07", "2023-08"]
    executar_pipeline_silver(spark_session, meses_alvo)