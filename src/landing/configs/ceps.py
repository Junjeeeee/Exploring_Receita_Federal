from src.landing.landing_lib import TabelaLandingConfig

CONFIG_LANDING = TabelaLandingConfig(
    nome_tabela="ceps",
    pasta_s3="ceps",
    origem="BQ_CEPS",
    is_snapshot_mensal=False,
    colunas=["cep", "uf", "cidade", "localidade", "codigo_ibge", "latitude", "longitude"],
    colunas_float=["latitude", "longitude"],
    min_linhas_esperadas=900_000
)