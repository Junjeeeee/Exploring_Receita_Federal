from src.landing.landing_lib import TabelaLandingConfig

CONFIG_LANDING = TabelaLandingConfig(
    nome_tabela="naturezas_juridicas",
    pasta_s3="naturezas_juridicas",
    origem="RFB_ZIP",
    prefixo_zip="Naturezas",
    is_snapshot_mensal=False,
    colunas=["codigo", "descricao"],
    min_linhas_esperadas=70
)