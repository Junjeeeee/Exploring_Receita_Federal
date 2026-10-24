from src.bronze.bronze_lib import TabelaBronzeConfig

CONFIG = TabelaBronzeConfig(
    nome_tabela_uc="municipios",
    pasta_s3="municipios",
    chaves_primarias=["codigo"],
    descricao_tabela="Camada Bronze: Tabela de domínio com os códigos e descrições dos municípios padrão Receita Federal.",
    colunas_esperadas=["codigo", "descricao"],
    descricao_colunas={
        "codigo": "Código identificador único do município SIAFI/RFB (PK).",
        "descricao": "Nome oficial do município."
    }
)
