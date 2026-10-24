from src.landing.landing_lib import TabelaLandingConfig

CONFIG_LANDING = TabelaLandingConfig(
    nome_tabela="paises",
    pasta_s3="paises",
    origem="RFB_ZIP",
    prefixo_zip="Paises",
    is_snapshot_mensal=False,
    colunas=["codigo", "descricao"],
    min_linhas_esperadas=200
)