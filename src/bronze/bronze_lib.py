import argparse
import os
from dataclasses import dataclass, field
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Optional
from pyspark.sql import Column, SparkSession
import pyspark.sql.functions as F
from src.bronze.data_quality import validar_schema, validar_volumetria

DESCRICAO_COLUNAS_METADADOS: Dict[str, str] = {
    "change_hash": "Metadado: Hash SHA-256 serializado dos atributos de negócio usados na deteção de mudanças.",
    "pk": "Metadado: Chave primária lógica gerada pela concatenação dinâmica da granularidade da tabela.",
    "mes_referencia": "Metadado: Mês e ano de referência do snapshot extraído da Receita Federal (Partição).",
    "bronze_processed_at": "Metadado: Timestamp de ingestão/reprocessamento do registo na camada Bronze.",
    "ingested_at": "Metadado: Timestamp de extração na Landing.",
    "source_reference": "Metadado: Identificador da versão/referência da fonte de origem."
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
    limite_queda_volumetria: float = 0.15
    # False para dimensões estáticas armazenadas em .../<pasta_s3_landing>/dados.parquet.
    is_snapshot_mensal: bool = True
    # Permite que o nome físico na Landing difira do nome lógico na Bronze.
    pasta_s3_landing: Optional[str] = None
    # Lista explícita recomendada; se vazia, usa todas as colunas de negócio da origem.
    colunas_hash: List[str] = field(default_factory=list)
    # Obrigatório quando a PK customizada depende de campos específicos.
    colunas_pk_custom_obrigatorias: List[str] = field(default_factory=list)


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


def obter_bucket_landing() -> str:
    """Retorna o mesmo bucket usado pela Landing.

    Configure AWS_BUCKET_NAME no ambiente do job Databricks se o bucket não
    for o padrão histórico `lakehouse-receita`.
    """
    bucket = (os.getenv("AWS_BUCKET_NAME") or "lakehouse-receita").strip()
    if not bucket:
        raise EnvironmentError("AWS_BUCKET_NAME está vazio.")
    return bucket


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


def _obter_pasta_landing(config: TabelaBronzeConfig) -> str:
    """Resolve o caminho Landing; permite override explícito por configuração."""
    if config.pasta_s3_landing:
        return config.pasta_s3_landing
    aliases = {
        "motivos": "motivos_situacao",
        "naturezas": "naturezas_juridicas",
        "qualificacoes": "qualificacoes_socios",
    }
    return aliases.get(config.pasta_s3, config.pasta_s3)


def _colunas_de_negocio(df_raw, config: TabelaBronzeConfig) -> List[str]:
    tecnicas = set(DESCRICAO_COLUNAS_METADADOS) | {
        "ingested_at", "landing_ingested_at", "source_reference"
    }
    disponiveis = [c for c in df_raw.columns if c not in tecnicas]
    if config.colunas_hash:
        faltantes = set(config.colunas_hash) - set(df_raw.columns)
        if faltantes:
            raise ValueError(f"Colunas de hash ausentes em {config.nome_tabela_uc}: {sorted(faltantes)}")
        return config.colunas_hash
    return disponiveis


def _normalizar_lista_csv(df, coluna: str):
    """Normaliza espaços e remove itens vazios antes de ordenar uma lista CSV."""
    return df.withColumn(
        coluna,
        F.when(
            F.col(coluna).isNotNull() & (F.trim(F.col(coluna)) != ""),
            F.concat_ws(",", F.array_sort(F.filter(
                F.transform(F.split(F.col(coluna), ","), lambda x: F.trim(x)),
                lambda x: x != ""
            )))
        ).otherwise(F.col(coluna))
    )


def _validar_componentes_pk(df, config: TabelaBronzeConfig):
    if config.pk_custom_expr is not None and not config.colunas_pk_custom_obrigatorias:
        raise ValueError(
            f"A tabela {config.nome_tabela_uc} usa pk_custom_expr, mas não declarou "
            "colunas_pk_custom_obrigatorias. Declare os campos mínimos que impedem "
            "a criação de chaves artificiais a partir de valores ausentes."
        )
    obrigatorias = (
        config.colunas_pk_custom_obrigatorias
        if config.pk_custom_expr is not None
        else config.chaves_primarias
    )
    faltantes = set(obrigatorias) - set(df.columns)
    if faltantes:
        raise ValueError(f"Componentes de PK ausentes em {config.nome_tabela_uc}: {sorted(faltantes)}")
    for coluna in obrigatorias:
        df = df.filter(
            F.col(coluna).isNotNull() & (F.trim(F.col(coluna).cast("string")) != "")
        )
    return df


def _adicionar_pk(df, config: TabelaBronzeConfig):
    if config.pk_custom_expr is not None:
        df = df.withColumn("pk", config.pk_custom_expr)
    elif config.chaves_primarias:
        partes = [F.trim(F.col(c).cast("string")) for c in config.chaves_primarias]
        # JSON mantém separação inequívoca e preserva a distinção entre campos.
        df = df.withColumn("pk", F.to_json(F.struct(*[
            parte.alias(f"k{i}") for i, parte in enumerate(partes)
        ])))
    if config.chaves_primarias or config.pk_custom_expr is not None:
        df = df.filter(F.col("pk").isNotNull() & (F.trim(F.col("pk")) != ""))
    return df


def _remover_duplicatas_ou_falhar(df, nome_tabela: str):
    """Elimina cópias exatas; não escolhe arbitrariamente entre linhas conflitantes."""
    df = df.dropDuplicates()
    if "pk" not in df.columns:
        return df
    conflitos = (
        df.groupBy("pk")
        .agg(F.countDistinct("change_hash").alias("qtd_hashes"))
        .filter(F.col("qtd_hashes") > 1)
        .limit(10)
        .collect()
    )
    if conflitos:
        amostra = [r["pk"] for r in conflitos]
        raise ValueError(
            f"PK duplicada com conteúdo divergente em {nome_tabela}. "
            f"Amostra de PKs: {amostra}. Nenhum registro foi descartado."
        )
    # Duplicatas com mesmo hash representam a mesma versão lógica da linha.
    return df.dropDuplicates(["pk"])


def ingerir_tabela_bronze(
    spark: SparkSession,
    config: TabelaBronzeConfig,
    mes_ini: Optional[str] = None,
    mes_fim: Optional[str] = None,
):
    configurar_ambiente_unity_catalog(spark)
    nome_completo_tabela = f"tcc_receita.bronze.{config.nome_tabela_uc}"
    pasta_landing = _obter_pasta_landing(config)
    meses = gerar_range_meses(mes_ini, mes_fim)
    colunas_finais = []
    bucket = obter_bucket_landing()

    if config.is_snapshot_mensal:
        iteracoes = [(mes, f"s3://{bucket}/landing/receita_federal/{pasta_landing}/mes_referencia={mes}/") for mes in meses]
    else:
        # Fonte estática tem um único objeto, sem partição mes_referencia.
        iteracoes = [(None, f"s3://{bucket}/landing/receita_federal/{pasta_landing}/dados.parquet")]

    print(f"🚀 Iniciando Ingestão Bronze: {nome_completo_tabela}")
    for mes_referencia, caminho_landing in iteracoes:
        rotulo = mes_referencia or "estática"
        print(f"🔄 Lendo e transformando {config.nome_tabela_uc} ({rotulo})...")
        df_raw = spark.read.parquet(caminho_landing)

        if config.colunas_esperadas:
            faltantes = set(config.colunas_esperadas) - set(df_raw.columns)
            if faltantes:
                raise ValueError(
                    f"Schema incompatível em {caminho_landing}: colunas ausentes {sorted(faltantes)}"
                )

        for coluna in config.colunas_csv_para_ordenar:
            if coluna in df_raw.columns:
                df_raw = _normalizar_lista_csv(df_raw, coluna)

        df_transformado = _validar_componentes_pk(df_raw, config)
        colunas_negocio = _colunas_de_negocio(df_transformado, config)
        if not colunas_negocio:
            raise ValueError(f"Nenhuma coluna de negócio disponível para hash em {config.nome_tabela_uc}.")

        # JSON estruturado evita colisões por delimitadores e diferencia NULL de string vazia.
        payload_hash = F.to_json(
            F.struct(*[F.col(c).cast("string").alias(c) for c in colunas_negocio]),
            options={"ignoreNullFields": "false"},
        )
        df_transformado = df_transformado.withColumn("change_hash", F.sha2(payload_hash, 256))
        df_transformado = _adicionar_pk(df_transformado, config)
        df_transformado = _remover_duplicatas_ou_falhar(df_transformado, nome_completo_tabela)

        if mes_referencia is not None:
            df_bronze = df_transformado.withColumn("mes_referencia", F.lit(mes_referencia))
        else:
            df_bronze = df_transformado
        df_bronze = df_bronze.withColumn("bronze_processed_at", F.current_timestamp())

        # Preserva source_reference para rastrear a versão de origem; remove apenas timestamp técnico da Landing.
        for coluna_tecnica in ("ingested_at", "landing_ingested_at"):
            if coluna_tecnica in df_bronze.columns:
                df_bronze = df_bronze.drop(coluna_tecnica)

        writer = df_bronze.write.format("delta").mode("overwrite").option("mergeSchema", "true")
        if config.is_snapshot_mensal:
            writer = writer.option("replaceWhere", f"mes_referencia = '{mes_referencia}'").partitionBy("mes_referencia")
        writer.saveAsTable(nome_completo_tabela)
        colunas_finais = df_bronze.columns

    if colunas_finais:
        # Usa o schema efetivamente persistido, não o último DataFrame em memória.
        colunas_persistidas = spark.read.table(nome_completo_tabela).columns
        aplicar_comentarios_uc(spark, config, colunas_persistidas)
    print(f"✅ Ingestão e documentação finalizadas: {nome_completo_tabela}\n")

def testar_tabela_bronze(spark: SparkSession, config: TabelaBronzeConfig, mes_ini: Optional[str] = None, mes_fim: Optional[str] = None):
    nome_completo_tabela = f"tcc_receita.bronze.{config.nome_tabela_uc}"
    meses = gerar_range_meses(mes_ini, mes_fim)
    print(f"🧪 Executando Data Quality: {nome_completo_tabela} | Meses: {meses}")

    df_tabela = spark.read.table(nome_completo_tabela)
    colunas_tecnicas = set(DESCRICAO_COLUNAS_METADADOS.keys()) | {"source_reference", "ingested_at", "landing_ingested_at"}
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
        validar_volumetria(spark, df_mes, config.nome_tabela_uc, mes_referencia, threshold_queda=config.limite_queda_volumetria)

        # 3. Validação de Hash e PK não nulos
        # Utilizando agregação única para evitar múltiplos counts
        res = df_mes.select(
            F.sum(F.when(F.col("change_hash").isNull(), 1).otherwise(0)).alias("nulos_hash"),
            F.sum(F.when(F.col("pk").isNull() | (F.trim(F.col("pk")) == ""), 1).otherwise(0)).alias("nulos_pk")
        ).collect()[0]

        if res["nulos_hash"] > 0:
            raise ValueError(f"❌ {res['nulos_hash']} registos com change_hash nulo em {nome_completo_tabela} ({mes_referencia})!")

        if config.chaves_primarias or config.pk_custom_expr is not None:
            if res["nulos_pk"] > 0:
                raise ValueError(f"❌ {res['nulos_pk']} registos com PK nula/vazia em {nome_completo_tabela} ({mes_referencia})!")

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