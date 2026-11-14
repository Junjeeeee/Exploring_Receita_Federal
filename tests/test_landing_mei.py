"""Testes da máscara de CPF dos MEIs (limpar_chunk_empresas_mei) e de gerar_range_meses."""
import pandas as pd
import pytest

from src.landing.landing_lib import gerar_range_meses, limpar_chunk_empresas_mei


def _chunk(cnpjs, razoes, portes, naturezas, capital=None):
    dados = {
        "cnpj_basico": cnpjs,
        "razao_social": razoes,
        "porte_empresa": portes,
        "natureza_juridica": naturezas,
    }
    if capital is not None:
        dados["capital_social"] = capital
    return pd.DataFrame(dados)


def test_cpf_pontuado_e_mascarado_so_no_mei():
    # porte "01" = ME (microempresa); natureza "2135" = empresário individual.
    # A máscara vale para porte "01" + natureza "2135" (regra do projeto).
    df = _chunk(
        ["12345678", "87654321"],
        ["123.456.789-00 JOAO SILVA", "EMPRESA NORMAL LTDA"],
        ["01", "05"],
        ["2135", "2062"],  # 2062 = sociedade empresária limitada
    )
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "razao_social"] == "12.345.678 JOAO SILVA"
    assert out.loc[1, "razao_social"] == "EMPRESA NORMAL LTDA"


def test_cpf_so_digitos_no_meio_do_nome():
    df = _chunk(["11111111"], ["MARIA 09876543211 SOUZA"], ["01"], ["2135"])
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "razao_social"] == "11.111.111 MARIA SOUZA"


def test_razao_nula_ou_nan_vira_so_cnpj_formatado():
    df = _chunk(["22222222", "33333333"], [None, "nan"], ["01", "01"], ["2135", "2135"])
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "razao_social"] == "22.222.222"
    assert out.loc[1, "razao_social"] == "33.333.333"


def test_cnpj_basico_curto_e_preenchido_com_zeros():
    df = _chunk(["123"], [None], ["01"], ["2135"])
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "razao_social"] == "00.000.123"


def test_porte_diferente_de_01_nao_e_tocado():
    df = _chunk(["12345678"], ["123.456.789-00 JOAO SILVA"], ["03"], ["2135"])
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "razao_social"] == "123.456.789-00 JOAO SILVA"


def test_natureza_diferente_de_2135_nao_e_tocada():
    df = _chunk(["12345678"], ["123.456.789-00 JOAO SILVA"], ["01"], ["2062"])
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "razao_social"] == "123.456.789-00 JOAO SILVA"


def test_capital_social_formato_brasileiro_e_invalido():
    df = _chunk(
        ["1", "2", "3"], ["A", "B", "C"], ["05"] * 3, ["2062"] * 3,
        capital=["1.234,50", "100,5", "abc"],
    )
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "capital_social"] == 1234.5
    assert out.loc[1, "capital_social"] == 100.5
    assert pd.isna(out.loc[2, "capital_social"])  # inválido vira nulo, não zero


def test_gerar_range_meses_virada_de_ano():
    assert gerar_range_meses("2023-11", "2024-02") == ["2023-11", "2023-12", "2024-01", "2024-02"]


def test_gerar_range_meses_sem_fim_usa_so_o_inicial():
    assert gerar_range_meses("2024-05") == ["2024-05"]


def test_gerar_range_meses_formato_invalido_e_intervalo_invertido():
    with pytest.raises(ValueError):
        gerar_range_meses("2024-13", "2024-14")
    with pytest.raises(ValueError):
        gerar_range_meses("2024-05", "2024-04")
