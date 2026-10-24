from src.landing.landing_lib import TabelaLandingConfig

CONFIG_LANDING = TabelaLandingConfig(
    nome_tabela="socios",
    pasta_s3="socio",
    origem="RFB_ZIP",
    prefixo_zip="Socios",
    is_snapshot_mensal=True,
    colunas=[
        "cnpj_basico", "identificador_socio", "nome_socio_razao_social",
        "cnpj_cpf_socio", "qualificacao_socio", "data_entrada_sociedade",
        "pais", "representante_legal", "nome_representante",
        "qualificacao_representante_legal", "faixa_etaria"
    ],
    min_linhas_esperadas=20_000_000
)