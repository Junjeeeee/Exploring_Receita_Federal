import pytest
import pandas as pd

# Importando as funções do seu código principal
# Ajuste o caminho do import conforme a estrutura real da sua pasta src/
from cnpj_pipeline.extract.extractor import obter_proximo_mes, aplicar_mascara_mei

def test_obter_proximo_mes():
    """Garante que a transição de meses e anos está correta."""
    assert obter_proximo_mes(None) == "2024-01"
    assert obter_proximo_mes("2024-05") == "2024-06"
    assert obter_proximo_mes("2024-12") == "2025-01"

def test_aplicar_mascara_mei_com_cpf_pontuado():
    """Valida se o CPF padrão (com pontos e traço) é mascarado na tabela EMPRESA."""
    dados = {
        "cnpj_basico": ["12345678", "87654321"],
        "razao_social": ["123.456.789-00 JOAO SILVA", "EMPRESA NORMAL LTDA"]
    }
    df = pd.DataFrame(dados)
    
    df_processado = aplicar_mascara_mei(df, "EMPRESA")
    
    assert df_processado.loc[0, "razao_social"] == "12345678 JOAO SILVA"
    assert df_processado.loc[1, "razao_social"] == "EMPRESA NORMAL LTDA"

def test_aplicar_mascara_mei_com_cpf_apenas_numeros():
    """Valida se o CPF sem pontuação é mascarado corretamente."""
    dados = {
        "cnpj_basico": ["11111111"],
        "razao_social": ["MARIA 09876543211 SOUZA"]
    }
    df = pd.DataFrame(dados)
    
    df_processado = aplicar_mascara_mei(df, "EMPRESA")
    
    assert df_processado.loc[0, "razao_social"] == "MARIA 11111111 SOUZA"

def test_nao_aplica_mascara_em_outras_tabelas():
    """Garante que a regra do MEI não quebra outras tabelas, como ESTABELE."""
    dados = {
        "cnpj_basico": ["12345678"],
        "razao_social": ["123.456.789-00 JOAO SILVA"] # O nome da coluna pode diferir, simulando contexto genérico
    }
    df = pd.DataFrame(dados)
    
    df_processado = aplicar_mascara_mei(df, "ESTABELE")
    
    # O valor deve permanecer intacto
    assert df_processado.loc[0, "razao_social"] == "123.456.789-00 JOAO SILVA"