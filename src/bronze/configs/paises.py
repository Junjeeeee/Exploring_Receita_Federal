from src.bronze.bronze_lib import TabelaBronzeConfig

CONFIG = TabelaBronzeConfig(
    nome_tabela_uc="paises",
    pasta_s3="paises",
    chaves_primarias=["codigo"],
    descricao_tabela="Camada Bronze: Tabela de domínio com os códigos e descrições dos países padrão Receita Federal.",
    colunas_esperadas=["codigo", "descricao"],
    descricao_colunas={
        "codigo": "Código identificador único do país (PK).",
        "descricao": "Nome oficial do país."
    }
)
