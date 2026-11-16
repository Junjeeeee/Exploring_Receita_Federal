"""Testes de scripts/checar_sql_cluster.py (compatibilidade dos SQL com o cluster do Job, DBR 15.4)."""

from scripts.checar_sql_cluster import RAIZ, checar_arquivos, checar_texto, main, mascarar


def _regras(sql: str, no_job: bool = True) -> list[tuple[int, str]]:
    return [(linha, regra) for linha, regra, _ in checar_texto(sql, no_job=no_job)]


def test_try_to_date_detectado_com_linha():
    sql = "SELECT 1;\nSELECT try_to_date(x, 'yyyyMMdd') FROM t;"
    assert _regras(sql) == [(2, "try_to_date")]


def test_try_to_timestamp_permitido():
    assert _regras("SELECT CAST(TRY_TO_TIMESTAMP(x, 'yyyyMMdd') AS DATE) FROM t;") == []


def test_zorder_em_varias_linhas():
    sql = "OPTIMIZE tcc_receita.gold.t\n  ZORDER BY (uf);"
    assert _regras(sql) == [(2, "zorder")]


def test_optimize_simples_permitido():
    assert _regras("OPTIMIZE tcc_receita.gold.t;") == []


def test_pipe_sql_detectado():
    assert _regras("FROM t\n|> WHERE a = 1;") == [(2, "pipe_sql")]


def test_begin_end_detectado_so_no_inicio_de_statement():
    assert _regras("SELECT 1;\nBEGIN\n  SELECT 2;\nEND;") == [(2, "sql_scripting")]
    assert _regras("SELECT begin_date FROM t;") == []


def test_ignora_comentarios_de_linha_e_de_bloco():
    sql = "-- TRY_TO_DATE( não existe\n/* OPTIMIZE t ZORDER BY (a)\n |> */ SELECT 1;"
    assert _regras(sql) == []


def test_ignora_strings():
    sql = "SELECT 'try_to_date(x)', \"ZORDER BY a\", 'it''s |>', 'a\\'b TRY_TO_DATE(' FROM t;"
    assert _regras(sql) == []


def test_mascarar_preserva_linhas_e_tamanho():
    sql = "SELECT 'a\nb' -- c\n/* d\ne */ FROM t"
    mascarado = mascarar(sql)
    assert len(mascarado) == len(sql)
    assert mascarado.count("\n") == sql.count("\n")
    assert "FROM t" in mascarado and "a" not in mascarado.split("FROM")[0].replace("SELECT", "")


def test_excecao_por_linha_exige_motivo():
    assert _regras("SELECT try_to_date(x) FROM t; -- compat: ok roda só no warehouse") == []
    assert _regras("SELECT try_to_date(x) FROM t; -- compat: ok") == [(1, "try_to_date")]


def test_parametro_de_dashboard_so_nos_arquivos_do_job():
    sql = "SELECT * FROM t WHERE uf = :param_uf AND ts = '12:30' AND v::int > 0 AND j:campo IS NOT NULL;"
    assert _regras(sql, no_job=True) == [(1, "parametro_dashboard")]
    assert _regras(sql, no_job=False) == []


def test_arquivo_com_problema_falha_com_arquivo_e_linha(tmp_path, capsys):
    arquivo = tmp_path / "x.sql"
    arquivo.write_text("SELECT 1;\nSELECT TRY_TO_DATE(a) FROM t;\n", encoding="utf-8")
    assert main([str(arquivo)]) == 1
    saida = capsys.readouterr().out
    assert "x.sql:2: [try_to_date]" in saida


def test_repositorio_atual_passa():
    assert checar_arquivos(sorted((RAIZ / "src").rglob("*.sql"))) == []
