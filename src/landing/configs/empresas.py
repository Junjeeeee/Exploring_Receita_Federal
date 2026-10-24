from src.landing.landing_lib import TabelaLandingConfig, limpar_chunk_empresas_mei

CONFIG_LANDING = TabelaLandingConfig(
    nome_tabela="empresas",
    pasta_s3="empresa",
    origem="RFB_ZIP",
    prefixo_zip="Empresas",
    is_snapshot_mensal=True,
    colunas=[
        "cnpj_basico", "razao_social", "natureza_juridica",
        "qualificacao_responsavel", "capital_social", "porte_empresa",
        "ente_federativo_responsavel"
    ],
    colunas_float=["capital_social"],
    min_linhas_esperadas=40_000_000,
    transform_chunk_fn=limpar_chunk_empresas_mei
)