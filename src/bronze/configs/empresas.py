from src.bronze.bronze_lib import TabelaBronzeConfig

CONFIG = TabelaBronzeConfig(
    nome_tabela_uc="empresas",
    pasta_s3="empresa",
    chaves_primarias=["cnpj_basico"],
    descricao_tabela="Camada Bronze: Dados cadastrais da raiz do CNPJ (Empresa), incluindo razão social, capital social e porte.",
    colunas_esperadas=[
        "cnpj_basico", "razao_social", "natureza_juridica",
        "qualificacao_responsavel", "capital_social", "porte_empresa",
        "ente_federativo_responsavel"
    ],
    descricao_colunas={
        "cnpj_basico": "Radical do CNPJ (primeiros 8 dígitos) que identifica a raiz corporativa da entidade.",
        "razao_social": "Nome empresarial oficial (Razão Social) registado na Receita Federal.",
        "natureza_juridica": "Código da natureza jurídica (ex: Sociedade Limitada, S.A.).",
        "qualificacao_responsavel": "Código da qualificação da pessoa física responsável pela empresa.",
        "capital_social": "Valor do capital social da empresa registado em Reais (BRL).",
        "porte_empresa": "Código indicativo do porte da empresa (ex: ME, EPP, Demais).",
        "ente_federativo_responsavel": "Órgão público responsável, preenchido para entes federativos."
    }
)