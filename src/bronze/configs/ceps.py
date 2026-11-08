from src.bronze.bronze_lib import TabelaBronzeConfig

# Sem chave primária de propósito: a base de CEPs (BigQuery) pode ter o mesmo CEP repetido com
# atributos diferentes. A Bronze não escolhe entre eles (sem regra de negócio); a deduplicação
# determinística acontece em silver_final.dim_empresa (ROW_NUMBER por CEP).
CONFIG = TabelaBronzeConfig(
    nome_tabela_uc="ceps",
    pasta_s3="ceps",
    chaves_primarias=[],
    is_snapshot_mensal=False,
    descricao_tabela="Camada Bronze: Base de CEPs (BigQuery, dados públicos) com UF, cidade, localidade, código IBGE e coordenadas.",
    colunas_esperadas=["cep", "uf", "cidade", "localidade", "codigo_ibge", "latitude", "longitude"],
    descricao_colunas={
        "cep": "Código de Endereçamento Postal (8 dígitos). Pode repetir; sem PK na Bronze.",
        "uf": "Sigla da Unidade da Federação.",
        "cidade": "Nome da cidade.",
        "localidade": "Bairro ou localidade do CEP.",
        "codigo_ibge": "Código IBGE do município (7 dígitos).",
        "latitude": "Latitude aproximada do CEP.",
        "longitude": "Longitude aproximada do CEP.",
    },
)
