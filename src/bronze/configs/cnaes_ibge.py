from src.bronze.bronze_lib import TabelaBronzeConfig

CONFIG = TabelaBronzeConfig(
    nome_tabela_uc="cnaes_ibge",
    pasta_s3="cnaes_ibge",
    chaves_primarias=["codigo"],
    descricao_tabela="Camada Bronze: Hierarquia oficial da CNAE extraída do IBGE (Seção, Divisão, Grupo, Classe e Subclasse).",
    colunas_esperadas=[
        "codigo", "descricao", "secao", "nome_secao",
        "divisao", "nome_divisao", "grupo", "nome_grupo",
        "classe", "nome_classe"
    ],
    descricao_colunas={
        "codigo": "Código identificador único da subclasse CNAE (PK).",
        "descricao": "Descrição textual detalhada da subclasse CNAE.",
        "secao": "Código da Seção (Grande Área de Atividade) na hierarquia CNAE IBGE.",
        "nome_secao": "Nome da Seção (Grande Área de Atividade) na hierarquia CNAE IBGE.",
        "divisao": "Código da Divisão na hierarquia CNAE IBGE.",
        "nome_divisao": "Nome da Divisão na hierarquia CNAE IBGE.",
        "grupo": "Código do Grupo na hierarquia CNAE IBGE.",
        "nome_grupo": "Nome do Grupo na hierarquia CNAE IBGE.",
        "classe": "Código da Classe na hierarquia CNAE IBGE.",
        "nome_classe": "Nome da Classe na hierarquia CNAE IBGE."
    }
)
