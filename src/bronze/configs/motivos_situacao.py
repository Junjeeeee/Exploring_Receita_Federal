from src.bronze.bronze_lib import TabelaBronzeConfig

CONFIG = TabelaBronzeConfig(
    nome_tabela_uc="motivos_situacao",
    pasta_s3="motivos",
    chaves_primarias=["codigo"],
    descricao_tabela="Camada Bronze: Tabela de domínio com os códigos e descrições dos motivos da situação cadastral.",
    colunas_esperadas=["codigo", "descricao"],
    descricao_colunas={
        "codigo": "Código identificador único do motivo da situação cadastral (PK).",
        "descricao": "Descrição textual do motivo de transição cadastral (ex: Falência, Liquidação Voluntária)."
    }
)
