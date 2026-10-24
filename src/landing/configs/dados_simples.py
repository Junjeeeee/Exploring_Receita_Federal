from src.landing.landing_lib import TabelaLandingConfig

CONFIG_LANDING = TabelaLandingConfig(
    nome_tabela="dados_simples",
    pasta_s3="simples",
    origem="RFB_ZIP",
    prefixo_zip="Simples",
    is_snapshot_mensal=True,
    colunas=[
        "cnpj_basico", "opcao_pelo_simples", "data_opcao_simples",
        "data_exclusao_simples", "opcao_pelo_mei", "data_opcao_mei",
        "data_exclusao_mei"
    ],
    min_linhas_esperadas=30_000_000
)