from src.landing.landing_lib import TabelaLandingConfig

CONFIG_LANDING = TabelaLandingConfig(
    nome_tabela="motivos_situacao",
    pasta_s3="motivos_situacao",
    origem="RFB_ZIP",
    prefixo_zip="Motivos",
    is_snapshot_mensal=False,
    colunas=["codigo", "descricao"],
    min_linhas_esperadas=50
)