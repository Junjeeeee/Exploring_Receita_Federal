import pyspark.sql.functions as F
from src.bronze.bronze_lib import TabelaBronzeConfig

PK_SOCIOS_EXPR = F.when(
    F.col("identificador_socio") == "1",
    F.concat_ws(
        "-",
        F.coalesce(F.col("cnpj_basico").cast("string"), F.lit("NA")),
        F.coalesce(F.col("cnpj_cpf_socio").cast("string"), F.lit("NA"))
    )
).otherwise(
    F.concat_ws(
        "-",
        F.coalesce(F.col("cnpj_basico").cast("string"), F.lit("NA")),
        F.coalesce(F.col("cnpj_cpf_socio").cast("string"), F.lit("NA")),
        F.coalesce(F.col("nome_socio_razao_social").cast("string"), F.lit("NA"))
    )
)

CONFIG = TabelaBronzeConfig(
    nome_tabela_uc="socios",
    pasta_s3="socio",
    chaves_primarias=["cnpj_basico", "cnpj_cpf_socio", "nome_socio_razao_social"],
    pk_custom_expr=PK_SOCIOS_EXPR,
    descricao_tabela="Camada Bronze: Quadro de Sócios e Administradores (QSA), detalhando o perfil, papel e entrada de cada membro na sociedade.",
    colunas_esperadas=[
        "cnpj_basico", "identificador_socio", "nome_socio_razao_social",
        "cnpj_cpf_socio", "qualificacao_socio", "data_entrada_sociedade",
        "pais", "representante_legal", "nome_representante",
        "qualificacao_representante_legal", "faixa_etaria"
    ],
    descricao_colunas={
        "cnpj_basico": "Radical do CNPJ (primeiros 8 dígitos) que identifica a raiz corporativa da entidade.",
        "identificador_socio": "Código identificador do tipo de sócio (1=PJ, 2=PF, 3=Estrangeiro).",
        "nome_socio_razao_social": "Nome civil do sócio pessoa física ou Razão Social do sócio pessoa jurídica.",
        "cnpj_cpf_socio": "Documento de identificação do sócio (CPF ou CNPJ).",
        "qualificacao_socio": "Código do papel exercido na sociedade (ex: Diretor, Presidente, Sócio-Administrador).",
        "data_entrada_sociedade": "Data de integração oficial do membro na sociedade (AAAAMMDD).",
        "pais": "Código do país de nacionalidade/origem do sócio estrangeiro.",
        "representante_legal": "Documento de identificação (CPF) do representante legal do sócio.",
        "nome_representante": "Nome civil do representante legal designado.",
        "qualificacao_representante_legal": "Código do papel/qualificação do representante legal.",
        "faixa_etaria": "Código da faixa etária aproximada do sócio pessoa física."
    }
)