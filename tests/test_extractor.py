import pytest
import pandas as pd

# Importando as funções do seu código principal
# Ajuste o caminho do import conforme a estrutura real da sua pasta src/
from cnpj_pipeline.extract.extractor import obter_proximo_mes, aplicar_mascara_mei, SCHEMAS_CNPJ

def test_obter_proximo_mes():
    """Garante que a transição de meses e anos está correta."""
    assert obter_proximo_mes(None) == "2023-05"
    assert obter_proximo_mes("2024-05") == "2024-06"
    assert obter_proximo_mes("2024-12") == "2025-01"

def test_aplicar_mascara_mei_com_cpf_pontuado():
    """Valida se o CPF padrão (com pontos e traço) é mascarado na tabela EMPRESA."""
    dados = {
        "cnpj_basico": ["12345678", "87654321"],
        "razao_social": ["123.456.789-00 JOAO SILVA", "EMPRESA NORMAL LTDA"],
        "porte_empresa": ["01", "05"],           # 01 = MEI, 05 = Outros
        "natureza_juridica": ["2135", "2062"]    # 2135 = Empresário Individual, 2062 = Sociedade
    }
    df = pd.DataFrame(dados)
    df_processado = aplicar_mascara_mei(df, "EMPRESA")
    
    # O MEI (linha 0) deve ser mascarado com o CNPJ no início
    assert df_processado.loc[0, "razao_social"] == "12.345.678 JOAO SILVA"
    
    # A empresa normal (linha 1) não deve ser tocada pelo filtro
    assert df_processado.loc[1, "razao_social"] == "EMPRESA NORMAL LTDA"

def test_aplicar_mascara_mei_com_cpf_apenas_numeros():
    """Valida se o CPF sem pontuação é mascarado corretamente e posicionado no início."""
    dados = {
        "cnpj_basico": ["11111111"],
        "razao_social": ["MARIA 09876543211 SOUZA"],
        "porte_empresa": ["01"],
        "natureza_juridica": ["2135"]
    }
    df = pd.DataFrame(dados)
    df_processado = aplicar_mascara_mei(df, "EMPRESA")
    
    assert df_processado.loc[0, "razao_social"] == "11.111.111 MARIA SOUZA"

def test_aplicar_mascara_mei_protecao_contra_nulos():
    """Valida se a função lida corretamente com dados ausentes."""
    dados = {
        "cnpj_basico": ["22222222", "33333333"],
        "razao_social": [None, "nan"],
        "porte_empresa": ["01", "01"],
        "natureza_juridica": ["2135", "2135"]
    }
    df = pd.DataFrame(dados)
    df_processado = aplicar_mascara_mei(df, "EMPRESA")
    
    # Se for nulo, deve retornar apenas o CNPJ formatado
    assert df_processado.loc[0, "razao_social"] == "22.222.222"
    assert df_processado.loc[1, "razao_social"] == "33.333.333"
    
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


def test_schemas_estao_corretos():
    """Valida se todos os schemas esperados estão configurados corretamente."""
    chaves_esperadas = {"EMPRESA", "ESTABELE", "SIMPLES", "SOCIO", "DOMINIO"}
    
    # Verifica se todas as chaves existem
    assert set(SCHEMAS_CNPJ.keys()) == chaves_esperadas
    
    # Verifica a estrutura específica do novo schema de domínio
    assert SCHEMAS_CNPJ["DOMINIO"] == ["codigo", "descricao"]

    # Verifica se os schemas principais continuam íntegros
    assert "cnpj_basico" in SCHEMAS_CNPJ["EMPRESA"]
    assert "razao_social" in SCHEMAS_CNPJ["EMPRESA"]