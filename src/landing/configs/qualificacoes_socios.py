from src.landing.landing_lib import TabelaLandingConfig

CONFIG_LANDING = TabelaLandingConfig(
    nome_tabela="qualificacoes_socios",
    pasta_s3="qualificacoes_socios",
    origem="RFB_ZIP",
    prefixo_zip="Qualificacoes",
    is_snapshot_mensal=False,
    colunas=["codigo", "descricao"],
    min_linhas_esperadas=40
)