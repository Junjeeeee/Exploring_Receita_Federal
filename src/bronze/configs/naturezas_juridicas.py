from src.bronze.bronze_lib import TabelaBronzeConfig

CONFIG = TabelaBronzeConfig(
    nome_tabela_uc="naturezas_juridicas",
    pasta_s3="naturezas",
    chaves_primarias=["codigo"],
    descricao_tabela="Camada Bronze: Tabela de domínio com os códigos e descrições das naturezas jurídicas corporativas.",
    colunas_esperadas=["codigo", "descricao"],
    descricao_colunas={
        "codigo": "Código identificador único da natureza jurídica (PK).",
        "descricao": "Descrição oficial da natureza jurídica."
    }
)
