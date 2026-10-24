from src.bronze.bronze_lib import TabelaBronzeConfig

CONFIG = TabelaBronzeConfig(
    nome_tabela_uc="dados_simples",
    pasta_s3="simples",
    chaves_primarias=["cnpj_basico"],
    descricao_tabela="Camada Bronze: Informações sobre a adesão e exclusão de empresas aos regimes do Simples Nacional e MEI.",
    colunas_esperadas=[
        "cnpj_basico", "opcao_pelo_simples", "data_opcao_simples",
        "data_exclusao_simples", "opcao_pelo_mei", "data_opcao_mei",
        "data_exclusao_mei"
    ],
    descricao_colunas={
        "cnpj_basico": "Radical do CNPJ (primeiros 8 dígitos) que identifica a raiz corporativa da entidade.",
        "opcao_pelo_simples": "Flag de adesão ao regime tributário do Simples Nacional (S/N).",
        "data_opcao_simples": "Data oficial de adesão ao Simples Nacional (AAAAMMDD).",
        "data_exclusao_simples": "Data oficial de exclusão do Simples Nacional, se aplicável (AAAAMMDD).",
        "opcao_pelo_mei": "Flag de adesão ao regime tributário de Microempreendedor Individual - MEI (S/N).",
        "data_opcao_mei": "Data oficial de adesão ao MEI (AAAAMMDD).",
        "data_exclusao_mei": "Data oficial de exclusão do MEI, se aplicável (AAAAMMDD)."
    }
)
