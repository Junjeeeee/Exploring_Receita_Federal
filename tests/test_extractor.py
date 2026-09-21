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
        "razao_social": ["123.456.789-00 JOAO SILVA", "EMPRESA NORMAL LTDA"]
    }
    df = pd.DataFrame(dados)
    df_processado = aplicar_mascara_mei(df, "EMPRESA")
    
    # Atualizado para esperar a pontuação no CNPJ
    assert df_processado.loc[0, "razao_social"] == "12.345.678 JOAO SILVA"

def test_aplicar_mascara_mei_com_cpf_apenas_numeros():
    """Valida se o CPF sem pontuação é mascarado corretamente e posicionado no início."""
    dados = {
        "cnpj_basico": ["11111111"],
        "razao_social": ["MARIA 09876543211 SOUZA"]
    }
    df = pd.DataFrame(dados)
    df_processado = aplicar_mascara_mei(df, "EMPRESA")
    
    # Atualizado: CNPJ formatado no início + Nome sem o espaço "engolido"
    assert df_processado.loc[0, "razao_social"] == "11.111.111 MARIA SOUZA"

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