"""Testes puros (sem Spark) dos auxiliares da otimização da bronze
(docs/specs/features/2026-11-16-otimizacao-bronze.md)."""

import pytest

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
