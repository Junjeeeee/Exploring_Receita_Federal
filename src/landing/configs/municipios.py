from src.landing.landing_lib import TabelaLandingConfig

CONFIG_LANDING = TabelaLandingConfig(
    nome_tabela="municipios",
    pasta_s3="municipios",
    origem="RFB_ZIP",
    prefixo_zip="Municipios",
    is_snapshot_mensal=False,
    colunas=["codigo", "descricao"],
    min_linhas_esperadas=500
)