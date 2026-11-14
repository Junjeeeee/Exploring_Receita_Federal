"""Testes da máscara de CPF de empresário individual (limpar_chunk_empresas_mei) e de gerar_range_meses.

Todos os CPFs aqui são sequências fictícias (123.456.789-00, 98765432100 etc.).
"""
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
    # natureza "2135" = empresário individual; a máscara vale para toda 2135.
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


@pytest.mark.parametrize("porte", ["00", "01", "03", "05", None])
def test_2135_e_mascarado_em_qualquer_porte(porte):
    # Antes (até 2026-11-14) só porte "01" era mascarado; 03/05 ficavam em claro (D-a).
    df = _chunk(["12345678"], ["123.456.789-00 JOAO SILVA"], [porte], ["2135"])
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "razao_social"] == "12.345.678 JOAO SILVA"


def test_2135_porte_05_cpf_so_digitos_no_fim():
    df = _chunk(["44444444"], ["MARIA SOUZA 98765432100"], ["05"], ["2135"])
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "razao_social"] == "44.444.444 MARIA SOUZA"


@pytest.mark.parametrize("natureza", ["2135", " 2135 ", "02135", 2135, 2135.0])
def test_natureza_como_str_ou_numero(natureza):
    df = _chunk(["12345678"], ["123.456.789-00 JOAO SILVA"], ["03"], [natureza])
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "razao_social"] == "12.345.678 JOAO SILVA"


@pytest.mark.parametrize("natureza", [None, "", "abc", "2134", "21350"])
def test_natureza_nula_invalida_ou_vizinha_nao_e_tocada(natureza):
    df = _chunk(["12345678"], ["123.456.789-00 JOAO SILVA"], ["01"], [natureza])
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "razao_social"] == "123.456.789-00 JOAO SILVA"


def test_pj_com_11_digitos_no_nome_nao_e_mascarada():
    # Natureza != 2135 é pessoa jurídica: 11 dígitos podem ser número legítimo
    # (registro, telefone...). Escolha conservadora: não tocar (ver spec D-a).
    df = _chunk(
        ["55555555", "66666666"],
        ["CONDOMINIO 11122233344 LTDA", "ASSOCIACAO 00011122233"],
        ["05", "03"],
        ["2062", "3999"],
    )
    out = limpar_chunk_empresas_mei(df)
    assert out.loc[0, "razao_social"] == "CONDOMINIO 11122233344 LTDA"
    assert out.loc[1, "razao_social"] == "ASSOCIACAO 00011122233"


def test_chunk_misto_so_mascara_as_linhas_2135():
    df = _chunk(
        ["10000001", "10000002", "10000003", "10000004"],
        ["ANA 111.222.333-44", "BETO 55566677788", "PADARIA 12345678901 LTDA", "CARLA 99988877766"],
        ["01", "03", "01", "05"],
        ["2135", "2135", "2062", 2135],
    )
    out = limpar_chunk_empresas_mei(df)
    assert out["razao_social"].tolist() == [
        "10.000.001 ANA",
        "10.000.002 BETO",
        "PADARIA 12345678901 LTDA",
        "10.000.004 CARLA",
    ]


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
