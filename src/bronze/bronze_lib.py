import argparse
import os
from dataclasses import dataclass, field
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Optional
from pyspark.sql import Column, SparkSession
import pyspark.sql.functions as F
from src.bronze.data_quality import validar_schema, validar_volumetria
from src.bronze.optimize_write import CONF_SESSAO, VARIAVEL_AMBIENTE, resolver_modo_optimize_write

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
    # O catálogo é pré-requisito (criado uma vez, com managed location no S3): o local de armazenamento
    # depende da conta e o Job não deve ter permissão de CREATE CATALOG. Ver src/jobs/README.md.
    spark.sql("CREATE SCHEMA IF NOT EXISTS tcc_receita.bronze")


def comentarios_pendentes(
    descricao_colunas: Dict[str, str], comentarios_atuais: Dict[str, Optional[str]]
) -> Dict[str, str]:
    """Colunas cujo comentário desejado difere do gravado (só colunas que existem na tabela)."""
    return {
        col: comentario
        for col, comentario in descricao_colunas.items()
        if col in comentarios_atuais and comentarios_atuais[col] != comentario
    }


def aplicar_comentarios_uc(spark: SparkSession, config: TabelaBronzeConfig):
    """Aplica os comentários de tabela e de coluna no Unity Catalog só quando diferem do que já está gravado.

    Cada COMMENT ON / ALTER COLUMN ... COMMENT é um commit Delta (+ atualização no UC). Antes, todos eram
    reaplicados a cada execução, coluna a coluna (B20). A escrita em overwrite sem `overwriteSchema`
    preserva o schema da tabela, comentários incluídos; então, depois da 1ª execução, nada é reaplicado,
    e um comentário editado no config é aplicado sozinho na execução seguinte.
    """
    nome_completo_tabela = f"tcc_receita.bronze.{config.nome_tabela_uc}"

    comentario_tabela_atual = spark.catalog.getTable(nome_completo_tabela).description
    if comentario_tabela_atual != config.descricao_tabela:
        comentario_tab = config.descricao_tabela.replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {nome_completo_tabela} IS '{comentario_tab}'")

    # Schema efetivamente persistido; o comentário de coluna fica em metadata["comment"] do campo.
    comentarios_atuais = {
        campo.name: campo.metadata.get("comment")
        for campo in spark.read.table(nome_completo_tabela).schema.fields
    }
    dicionario_completo = {**config.descricao_colunas, **DESCRICAO_COLUNAS_METADADOS}
    pendentes = comentarios_pendentes(dicionario_completo, comentarios_atuais)
    for col, comentario in pendentes.items():
        comentario_col = comentario.replace("'", "''")
        spark.sql(f"ALTER TABLE {nome_completo_tabela} ALTER COLUMN {col} COMMENT '{comentario_col}'")
    print(f"  Comentários de coluna aplicados: {len(pendentes)} (demais já estavam gravados).")


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


def _remover_copias_exatas(df):
    """Remove só cópias idênticas. PKs repetidas com conteúdo divergente (a Receita publica algumas)
    são MANTIDAS: escolher uma versão é regra de negócio e fica na Silver (ROW_NUMBER por pk/mês com
    desempate por change_hash). Ver backlog B16.

    A contagem dessas PKs repetidas (só um aviso) fica na DQ (`testar_tabela_bronze`), lendo só a coluna
    `pk` da tabela já gravada. Aqui ela era uma ação Spark a mais por mês que, sem cache, refazia leitura +
    hash + dropDuplicates só para imprimir o aviso (B17; P1 de docs/specs/features/2026-11-16-otimizacao-bronze.md).
    """
    return df.dropDuplicates()


def _gravar_sem_optimize_write_na_sessao(spark: SparkSession, writer, nome_completo_tabela: str):
    """Desliga o optimized write na sessão só durante esta escrita e restaura o valor anterior (modo "sessao")."""
    anterior = spark.conf.get(CONF_SESSAO, None)
    spark.conf.set(CONF_SESSAO, "false")
    try:
        writer.saveAsTable(nome_completo_tabela)
    finally:
        if anterior is None:
            spark.conf.unset(CONF_SESSAO)
        else:
            spark.conf.set(CONF_SESSAO, anterior)


def ingerir_tabela_bronze(
    spark: SparkSession,
    config: TabelaBronzeConfig,
    mes_ini: Optional[str] = None,
    mes_fim: Optional[str] = None,
    modo_optimize_write: Optional[str] = None,
):
    """Ingestão mês a mês. `modo_optimize_write` ("nao" | "writer" | "sessao") controla o optimized write só na
    escrita das tabelas mensais; None = lê a variável de ambiente BRONZE_DESLIGAR_OPTIMIZE_WRITE; vazio = "nao"
    (comportamento anterior). Ver src/bronze/optimize_write.py."""
    configurar_ambiente_unity_catalog(spark)
    nome_completo_tabela = f"tcc_receita.bronze.{config.nome_tabela_uc}"
    pasta_landing = _obter_pasta_landing(config)
    meses = gerar_range_meses(mes_ini, mes_fim)
    colunas_finais = []
    bucket = obter_bucket_landing()
    modo_ow = resolver_modo_optimize_write(
        modo_optimize_write if modo_optimize_write is not None else os.getenv(VARIAVEL_AMBIENTE)
    )
    if config.is_snapshot_mensal and modo_ow != "nao":
        print(f"  Optimized write desligado na escrita mensal (modo '{modo_ow}').")

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
        df_transformado = _remover_copias_exatas(df_transformado)

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
            if modo_ow == "writer":
                writer = writer.option("optimizeWrite", "false")
        if config.is_snapshot_mensal and modo_ow == "sessao":
            _gravar_sem_optimize_write_na_sessao(spark, writer, nome_completo_tabela)
        else:
            writer.saveAsTable(nome_completo_tabela)
        colunas_finais = df_bronze.columns

    if colunas_finais:
        aplicar_comentarios_uc(spark, config)
    print(f"✅ Ingestão e documentação finalizadas: {nome_completo_tabela}\n")

def testar_tabela_bronze(spark: SparkSession, config: TabelaBronzeConfig, mes_ini: Optional[str] = None, mes_fim: Optional[str] = None):
    nome_completo_tabela = f"tcc_receita.bronze.{config.nome_tabela_uc}"
    # Tabelas estáticas não têm a coluna mes_referencia: validam a tabela inteira uma vez.
    meses = gerar_range_meses(mes_ini, mes_fim) if config.is_snapshot_mensal else [None]
    print(f"🧪 Executando Data Quality: {nome_completo_tabela} | Meses: {meses}")

    df_tabela = spark.read.table(nome_completo_tabela)
    colunas_tecnicas = set(DESCRICAO_COLUNAS_METADADOS.keys()) | {"source_reference", "ingested_at", "landing_ingested_at"}
    colunas_raw = [c for c in df_tabela.columns if c not in colunas_tecnicas]

    for mes_referencia in meses:
        if mes_referencia is None:
            df_mes = df_tabela
            mes_referencia = "estatica"
        else:
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
        # Tabelas sem PK (ex.: ceps) não têm a coluna pk.
        tem_pk = "pk" in df_mes.columns
        nulos_pk_expr = (
            F.sum(F.when(F.col("pk").isNull() | (F.trim(F.col("pk")) == ""), 1).otherwise(0))
            if tem_pk else F.lit(0)
        )
        res = df_mes.select(
            F.sum(F.when(F.col("change_hash").isNull(), 1).otherwise(0)).alias("nulos_hash"),
            nulos_pk_expr.alias("nulos_pk"),
        ).collect()[0]

        if res["nulos_hash"] > 0:
            raise ValueError(f"❌ {res['nulos_hash']} registos com change_hash nulo em {nome_completo_tabela} ({mes_referencia})!")

        if config.chaves_primarias or config.pk_custom_expr is not None:
            if res["nulos_pk"] > 0:
                raise ValueError(f"❌ {res['nulos_pk']} registos com PK nula/vazia em {nome_completo_tabela} ({mes_referencia})!")

        # 4. Aviso (não falha): PKs repetidas com conteúdo divergente, mantidas na bronze (B16).
        # Antes contado na ingestão; aqui lê só a coluna pk da tabela gravada (P1, B17).
        if tem_pk:
            qtd = df_mes.select("pk").groupBy("pk").count().filter("count > 1").count()
            if qtd:
                print(f"  ⚠️ {qtd} PK(s) repetidas com conteúdo divergente em {nome_completo_tabela} "
                      f"({mes_referencia}) (mantidas; a Silver desempata).")

    print(f"✅ Data Quality aprovado com sucesso: {nome_completo_tabela}\n")


def executar_ingestao_cli(config: TabelaBronzeConfig):
    """Ponto de entrada exclusivo para as tasks de Ingestão."""
    parser = argparse.ArgumentParser(description=f"Ingestão Bronze: {config.nome_tabela_uc}")
    parser.add_argument("--mes_referencia_inicial", default="", help="Mês inicial (YYYY-MM). Vazio = Mês-1")
    parser.add_argument("--mes_referencia_final", default="", help="Mês final (YYYY-MM). Vazio = Mês-1")
    args, extras = parser.parse_known_args()
    if extras:
        parser.error(f"argumentos desconhecidos: {extras}")

    spark = SparkSession.builder.getOrCreate()
    ingerir_tabela_bronze(spark, config, args.mes_referencia_inicial, args.mes_referencia_final)


def executar_dq_cli(config: TabelaBronzeConfig):
    """Ponto de entrada exclusivo para as tasks de Data Quality (Testes)."""
    parser = argparse.ArgumentParser(description=f"DQ Bronze: {config.nome_tabela_uc}")
    parser.add_argument("--mes_referencia_inicial", default="", help="Mês inicial (YYYY-MM). Vazio = Mês-1")
    parser.add_argument("--mes_referencia_final", default="", help="Mês final (YYYY-MM). Vazio = Mês-1")
    args, extras = parser.parse_known_args()
    if extras:
        parser.error(f"argumentos desconhecidos: {extras}")

    spark = SparkSession.builder.getOrCreate()
    testar_tabela_bronze(spark, config, args.mes_referencia_inicial, args.mes_referencia_final)