from src.bronze.bronze_lib import TabelaBronzeConfig

CONFIG = TabelaBronzeConfig(
    nome_tabela_uc="qualificacoes_socios",
    pasta_s3="qualificacoes",
    chaves_primarias=["codigo"],
    descricao_tabela="Camada Bronze: Tabela de domínio com os códigos e descrições dos papéis/qualificações dos sócios e representantes.",
    colunas_esperadas=["codigo", "descricao"],
    descricao_colunas={
        "codigo": "Código identificador único da qualificação do sócio ou responsável (PK).",
        "descricao": "Descrição textual do papel/qualificação na sociedade."
    }
)
