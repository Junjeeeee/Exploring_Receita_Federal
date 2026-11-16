"""Testes puros (sem Spark) dos auxiliares da otimização da bronze
(docs/specs/features/2026-11-16-otimizacao-bronze.md)."""

import pytest

from src.bronze.data_quality import checar_volumetria, media_volumetria_anterior
from src.bronze.optimize_write import MODO_PADRAO, resolver_modo_optimize_write


# --- P2: modo do optimized write -------------------------------------------------------------------------

@pytest.mark.parametrize("valor", [None, "", "   "])
def test_modo_vazio_preserva_comportamento_atual(valor):
    assert resolver_modo_optimize_write(valor) == MODO_PADRAO == "nao"


@pytest.mark.parametrize("valor,esperado", [("writer", "writer"), (" SESSAO ", "sessao"), ("Nao", "nao")])
def test_modo_normalizado(valor, esperado):
    assert resolver_modo_optimize_write(valor) == esperado


def test_modo_invalido_derruba():
    with pytest.raises(ValueError, match="inválido"):
        resolver_modo_optimize_write("false")


# --- P3: volumetria com histórico lido uma vez ------------------------------------------------------------

def _media_sql_antiga(log, tabela, mes):
    """Reprodução em Python da consulta antiga de validar_volumetria (uma consulta por mês):
    última linha por mês (verificado_em DESC), só meses < mes, ORDER BY mes DESC LIMIT 3, AVG (ignora NULL)."""
    ultima = {}
    for t, m, contagem, ts in log:
        if t == tabela and m is not None and m < mes and (m not in ultima or ts > ultima[m][1]):
            ultima[m] = (contagem, ts)
    top3 = sorted(ultima, reverse=True)[:3]
    valores = [ultima[m][0] for m in top3 if ultima[m][0] is not None]
    return sum(valores) / len(valores) if valores else 0


def _historico_novo(log, tabela):
    """O que ler_historico_volumetria devolve: última contagem por mês (rn = 1), todos os meses."""
    ultima = {}
    for t, m, contagem, ts in log:
        if t == tabela and (m not in ultima or ts > ultima[m][1]):
            ultima[m] = (contagem, ts)
    return {m: c for m, (c, _) in ultima.items()}


LOG_EXEMPLO = [
    ("empresas", "2023-01", 100, 1),
    ("empresas", "2023-02", 110, 2),
    ("empresas", "2023-02", 90, 5),      # reverificado: vale a mais recente
    ("empresas", "2023-03", None, 3),    # NULL: AVG ignora
    ("empresas", "2023-04", 130, 4),
    ("empresas", "2023-07", 999, 6),     # mês futuro (backfill fora de ordem): não entra
    ("socios", "2023-04", 1, 7),         # outra tabela: não entra
    ("empresas", None, 5, 8),            # mês nulo: não entra
]


@pytest.mark.parametrize("mes", ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05", "2023-06", "2023-08", "estatica"])
def test_media_igual_a_consulta_antiga(mes):
    historico = _historico_novo(LOG_EXEMPLO, "empresas")
    assert media_volumetria_anterior(historico, mes) == _media_sql_antiga(LOG_EXEMPLO, "empresas", mes)


def _executar_em_ordem(contagens, log_inicial, antigo):
    """Simula a volumetria de uma execução mês a mês; devolve (médias usadas, mês que falhou, log final)."""
    log = list(log_inicial)
    ts = max(r[3] for r in log) + 1
    historico = _historico_novo(log, "empresas")
    medias = []
    for mes, contagem in contagens:
        media = _media_sql_antiga(log, "empresas", mes) if antigo else media_volumetria_anterior(historico, mes)
        medias.append(media)
        try:
            checar_volumetria(contagem, media, "empresas", mes)
        except ValueError:
            return medias, mes, log
        log.append(("empresas", mes, contagem, ts))
        ts += 1
        historico[mes] = contagem
    return medias, None, log


@pytest.mark.parametrize("contagens", [
    [("2023-05", 120), ("2023-06", 125), ("2023-07", 130), ("2023-08", 131)],
    [("2023-05", 120), ("2023-06", 125), ("2023-07", 50), ("2023-08", 131)],  # queda no 3º mês
    [("2023-02", 95), ("2023-03", 0)],                                         # mês vazio
])
def test_ordem_da_volumetria_igual_a_antiga(contagens, capsys):
    antigo = _executar_em_ordem(contagens, LOG_EXEMPLO, antigo=True)
    novo = _executar_em_ordem(contagens, LOG_EXEMPLO, antigo=False)
    assert novo == antigo


def test_queda_forcada_falha_no_mesmo_mes_com_a_mesma_mensagem():
    historico = {"2023-05": 100, "2023-06": 100}
    with pytest.raises(ValueError) as erro:
        checar_volumetria(80, media_volumetria_anterior(historico, "2023-07"), "empresas", "2023-07",
                          threshold_queda=0.15)
    assert str(erro.value) == (
        "🚨 ANOMALIA VOLUMÉTRICA em empresas!\n"
        "Contagem (80) caiu mais de 15.0% em relação à média histórica (100)."
    )


def test_mes_vazio_mensagem():
    with pytest.raises(ValueError, match=r"^🚨 FALHA: O arquivo de socios no mês 2023-06 está vazio\.$"):
        checar_volumetria(0, 0, "socios", "2023-06")
