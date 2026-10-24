import argparse
from dataclasses import dataclass, field
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Optional
from pyspark.sql import Column, SparkSession
import pyspark.sql.functions as F
from src.bronze.data_quality import validar_schema, validar_volumetria

DESCRICAO_COLUNAS_METADADOS: Dict[str, str] = {
    "change_hash": "Metadado: Hash MD5 de todas as colunas brutas para deteção rigorosa de mutação (Change Data Capture).",
    "pk": "Metadado: Chave primária lógica gerada pela concatenação dinâmica da granularidade da tabela.",
    "mes_referencia": "Metadado: Mês e ano de referência do snapshot extraído da Receita Federal (Partição).",
    "inserted_at": "Metadado: Timestamp de ingestão inicial do registo na camada Bronze.",
    "updated_at": "Metadado: Timestamp da última atualização ou reprocessamento do registo na camada Bronze."
}


@dataclass
class TabelaBronzeConfig:
    nome_tabela_uc: str
    pasta_s3: str
    chaves_primarias: List[str]
    descricao_tabela: str
    descricao_colunas: Dict[str, str]
    colunas_esperadas: List[str] = field(default_factory=list)
    pk_custom_expr: Optional[Column] = None
    colunas_csv_para_ordenar: List[str] = field(default_factory=list)


def gerar_range_meses(mes_ini: Optional[str] = None, mes_fim: Optional[str] = None) -> List[str]:
    """Gera a lista ['YYYY-MM', ...] entre mes_ini e mes_fim. Se vazio, assume Mês - 1."""
    mes_padrao = (datetime.now() - relativedelta(months=1)).strftime("%Y-%m")
    ini_str = mes_ini.strip() if mes_ini and mes_ini.strip() else mes_padrao
    fim_str = mes_fim.strip() if mes_fim and mes_fim.strip() else ini_str

    dt_atual = datetime.strptime(ini_str, "%Y-%m")
    dt_fim = datetime.strptime(fim_str, "%Y-%m")

    if dt_atual > dt_fim:
        raise ValueError(f"mes_referencia_inicial ({ini_str}) maior que mes_referencia_final ({fim_str}).")

    meses = []
    while dt_atual <= dt_fim:
        meses.append(dt_atual.strftime("%Y-%m"))
        dt_atual += relativedelta(months=1)
    return meses


def configurar_ambiente_unity_catalog(spark: SparkSession):
    spark.sql("CREATE CATALOG IF NOT EXISTS tcc_receita")
    spark.sql("CREATE SCHEMA IF NOT EXISTS tcc_receita.bronze")


def aplicar_comentarios_uc(spark: SparkSession, config: TabelaBronzeConfig, colunas_existentes: List[str]):
    nome_completo_tabela = f"tcc_receita.bronze.{config.nome_tabela_uc}"
    comentario_tab = config.descricao_tabela.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {nome_completo_tabela} IS '{comentario_tab}'")

    dicionario_completo = {**config.descricao_colunas, **DESCRICAO_COLUNAS_METADADOS}
    for col in colunas_existentes:
        if col in dicionario_completo:
            comentario_col = dicionario_completo[col].replace("'", "''")
            spark.sql(f"ALTER TABLE {nome_completo_tabela} ALTER COLUMN {col} COMMENT '{comentario_col}'")


def ingerir_tabela_bronze(spark: SparkSession, config: TabelaBronzeConfig, mes_ini: Optional[str] = None, mes_fim: Optional[str] = None):
    configurar_ambiente_unity_catalog(spark)
    nome_completo_tabela = f"tcc_receita.bronze.{config.nome_tabela_uc}"
    meses = gerar_range_meses(mes_ini, mes_fim)
    colunas_finais = []

    print(f"🚀 Iniciando Ingestão Bronze: {nome_completo_tabela} | Meses: {meses}")

    for mes_referencia in meses:
        caminho_landing = f"s3://lakehouse-receita/landing/receita_federal/{config.pasta_s3}/mes_referencia={mes_referencia}/"
        print(f"🔄 Lendo e transformando {config.nome_tabela_uc} (Mês: {mes_referencia})...")

        df_raw = spark.read.parquet(caminho_landing)

        # Ordena listas separadas por vírgula (ex: CNAEs secundários) para evitar falso positivo no hash
        for col_ord in config.colunas_csv_para_ordenar:
            if col_ord in df_raw.columns:
                df_raw = df_raw.withColumn(
                    col_ord,
                    F.when(
                        F.col(col_ord).isNotNull() & (F.trim(F.col(col_ord)) != ""),
                        F.concat_ws(",", F.array_sort(F.split(F.trim(F.col(col_ord)), ",")))
                    ).otherwise(F.col(col_ord))
                )

        colunas_tecnicas = set(DESCRICAO_COLUNAS_METADADOS.keys()) | {"ingested_at"}
        colunas_negocio = [c for c in df_raw.columns if c not in colunas_tecnicas]

        # Gera Hash MD5 apenas das colunas de negócio com UPPER e TRIM
        expr_hash = F.md5(F.concat_ws("||", *[
            F.upper(F.trim(F.coalesce(F.col(c).cast("string"), F.lit(""))))
            for c in colunas_negocio
        ]))
        df_transformado = df_raw.withColumn("change_hash", expr_hash)

        # Aplica expressão customizada de PK (se existir) ou concatena chaves_primarias
        if config.pk_custom_expr is not None:
            df_transformado = df_transformado.withColumn("pk", config.pk_custom_expr)
        elif config.chaves_primarias:
            expr_pk = F.concat_ws("-", *[
                F.coalesce(F.col(c).cast("string"), F.lit("NA")) for c in config.chaves_primarias
            ])
            df_transformado = df_transformado.withColumn("pk", expr_pk)

        df_bronze = (
            df_transformado
            .withColumn("mes_referencia", F.lit(mes_referencia))
            .withColumn("inserted_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

        if "ingested_at" in df_bronze.columns:
            df_bronze = df_bronze.drop("ingested_at")

        (
            df_bronze.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"mes_referencia = '{mes_referencia}'")
            .option("mergeSchema", "true")
            .partitionBy("mes_referencia")
            .saveAsTable(nome_completo_tabela)
        )
        colunas_finais = df_bronze.columns

    if colunas_finais:
        aplicar_comentarios_uc(spark, config, colunas_finais)
    print(f"✅ Ingestão e documentação finalizadas: {nome_completo_tabela}\n")


def testar_tabela_bronze(spark: SparkSession, config: TabelaBronzeConfig, mes_ini: Optional[str] = None, mes_fim: Optional[str] = None):
    nome_completo_tabela = f"tcc_receita.bronze.{config.nome_tabela_uc}"
    meses = gerar_range_meses(mes_ini, mes_fim)
    print(f"🧪 Executando Data Quality: {nome_completo_tabela} | Meses: {meses}")

    df_tabela = spark.read.table(nome_completo_tabela)
    colunas_tecnicas = set(DESCRICAO_COLUNAS_METADADOS.keys())
    colunas_raw = [c for c in df_tabela.columns if c not in colunas_tecnicas]

    for mes_referencia in meses:
        df_mes = df_tabela.filter(F.col("mes_referencia") == mes_referencia)

        # 1. Validação de Schema local da config (garante suporte a novas tabelas) e do módulo data_quality
        if config.colunas_esperadas:
            faltantes = set(config.colunas_esperadas) - set(colunas_raw)
            if faltantes:
                raise ValueError(f"❌ Colunas ausentes em {nome_completo_tabela} ({mes_referencia}): {faltantes}")

        validar_schema(df_mes.select(*colunas_raw), config.nome_tabela_uc)

        # 2. Validação de Volumetria
        validar_volumetria(spark, df_mes, config.nome_tabela_uc, mes_referencia, threshold_queda=0.15)

        # 3. Validação de Hash e PK não nulos
        nulos_hash = df_mes.filter(F.col("change_hash").isNull()).count()
        if nulos_hash > 0:
            raise ValueError(f"❌ {nulos_hash} registos com change_hash nulo em {nome_completo_tabela} ({mes_referencia})!")

        if config.chaves_primarias or config.pk_custom_expr is not None:
            nulos_pk = df_mes.filter(F.col("pk").isNull() | (F.trim(F.col("pk")) == "")).count()
            if nulos_pk > 0:
                raise ValueError(f"❌ {nulos_pk} registos com PK nula/vazia em {nome_completo_tabela} ({mes_referencia})!")

    print(f"✅ Data Quality aprovado com sucesso: {nome_completo_tabela}\n")


def executar_ingestao_cli(config: TabelaBronzeConfig):
    """Ponto de entrada exclusivo para as tasks de Ingestão."""
    parser = argparse.ArgumentParser(description=f"Ingestão Bronze: {config.nome_tabela_uc}")
    parser.add_argument("--mes_referencia_inicial", default="", help="Mês inicial (YYYY-MM). Vazio = Mês-1")
    parser.add_argument("--mes_referencia_final", default="", help="Mês final (YYYY-MM). Vazio = Mês-1")
    args, _ = parser.parse_known_args()

    spark = SparkSession.builder.getOrCreate()
    ingerir_tabela_bronze(spark, config, args.mes_referencia_inicial, args.mes_referencia_final)


def executar_dq_cli(config: TabelaBronzeConfig):
    """Ponto de entrada exclusivo para as tasks de Data Quality (Testes)."""
    parser = argparse.ArgumentParser(description=f"DQ Bronze: {config.nome_tabela_uc}")
    parser.add_argument("--mes_referencia_inicial", default="", help="Mês inicial (YYYY-MM). Vazio = Mês-1")
    parser.add_argument("--mes_referencia_final", default="", help="Mês final (YYYY-MM). Vazio = Mês-1")
    args, _ = parser.parse_known_args()

    spark = SparkSession.builder.getOrCreate()
    testar_tabela_bronze(spark, config, args.mes_referencia_inicial, args.mes_referencia_final)