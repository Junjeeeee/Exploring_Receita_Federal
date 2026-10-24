from src.landing.landing_lib import TabelaLandingConfig

CONFIG_LANDING = TabelaLandingConfig(
    nome_tabela="cnaes_ibge",
    pasta_s3="cnaes_ibge",
    origem="API_IBGE",
    is_snapshot_mensal=False,
    colunas=[
        "codigo", "descricao", "secao", "nome_secao",
        "divisao", "nome_divisao", "grupo", "nome_grupo",
        "classe", "nome_classe"
    ],
    min_linhas_esperadas=1_300
)