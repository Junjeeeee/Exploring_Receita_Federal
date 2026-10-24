from src.bronze.bronze_lib import TabelaBronzeConfig

CONFIG = TabelaBronzeConfig(
    nome_tabela_uc="cnaes",
    pasta_s3="cnaes",
    chaves_primarias=["codigo"],
    descricao_tabela="Camada Bronze: Tabela de domínio com os códigos e descrições da Classificação Nacional de Atividades Econômicas (CNAE).",
    colunas_esperadas=["codigo", "descricao"],
    descricao_colunas={
        "codigo": "Código identificador único da subclasse CNAE na Receita Federal (PK).",
        "descricao": "Descrição textual da atividade econômica."
    }
)
