"""Compara, widget a widget, o que o Lakeview mostraria com dois JSON de dashboard (antigo × novo) na amostra.

Para cada widget, monta a consulta que o Lakeview faz sobre o dataset — `SELECT <campos> FROM (<SQL do dataset>)
WHERE <filtros> GROUP BY <campos sem agregação>` — com o SQL do JSON antigo e o do novo, roda as duas no SQL
warehouse e compara os resultados: contagens e somas exatas, AVG e PERCENTILE com tolerância 1e-9. Para cada
filtro, compara também a lista de valores do dropdown. Spec: 2026-11-16-otimizacao-log-fato-dashboard.md, §5.3.

    python scripts/amostra/comparar_widgets.py --antigo antigo.lvdash.json --novo src/gold/dashboard/tcc_receita.lvdash.json \\
        --schema-antigo amostra_f3_ref --schema-novo amostra_f3

`tcc_receita.gold.X` vira `tcc_receita.<schema>.X` em cada JSON. Não imprime linhas de dados, só contagens de
diferenças e o primeiro exemplo divergente (agregados, nunca CNPJ).
"""

import argparse
import json
import math
import os
import re
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ))

from scripts.amostra.rodar_no_warehouse import _api  # noqa: E402

AGREGADOS = ("SUM(", "AVG(", "PERCENTILE(", "COUNT(")


def consultar(warehouse: str, sql: str) -> list[list]:
    """Roda e devolve todas as linhas (busca todos os pedaços do resultado)."""
    r = _api("post", "/api/2.0/sql/statements", {"warehouse_id": warehouse, "statement": sql,
                                                 "wait_timeout": "50s", "on_wait_timeout": "CONTINUE",
                                                 "disposition": "INLINE", "format": "JSON_ARRAY"})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = _api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(r["status"].get("error", {}).get("message", r["status"]["state"])[:1000])
    linhas = list(r.get("result", {}).get("data_array", []) or [])
    total = r["manifest"].get("total_chunk_count", 1)
    for i in range(1, total):
        pedaco = _api("get", f"/api/2.0/sql/statements/{r['statement_id']}/result/chunks/{i}")
        linhas += pedaco.get("data_array", []) or []
    return linhas


def iguais(a, b) -> bool:
    if a == b:
        return True
    try:
        x, y = float(a), float(b)
    except (TypeError, ValueError):
        return False
    return math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-9)


def comparar_linhas(antigas, novas) -> tuple[int, str]:
    """Ordena pelas colunas de texto e compara posição a posição (com tolerância nos números)."""
    def chave(l):
        return [("" if v is None else str(v)) for v in l]
    a, n = sorted(antigas, key=chave), sorted(novas, key=chave)
    if len(a) != len(n):
        return abs(len(a) - len(n)) or 1, f"linhas: antigo={len(a)} novo={len(n)}"
    difs = [(x, y) for x, y in zip(a, n) if not all(iguais(u, v) for u, v in zip(x, y))]
    return len(difs), (f"ex.: antigo={difs[0][0]} novo={difs[0][1]}" if difs else "")


def datasets(caminho: Path, schema: str) -> dict[str, str]:
    dash = json.loads(caminho.read_text(encoding="utf-8"))
    return {d["name"]: re.sub(r"\btcc_receita\.gold\.", f"tcc_receita.{schema}.", "".join(d["queryLines"]))
            for d in dash["datasets"]}, dash


def sql_widget(sql_dataset: str, campos: list[dict], desagregado: bool, filtros: dict[str, list[str]]) -> str:
    selecao = ", ".join(f"{c['expression']} AS `{c['name']}`" for c in campos)
    onde = " AND ".join(
        f"`{col}` IN ({', '.join(repr(v) for v in vals)})" for col, vals in filtros.items()) or "TRUE"
    # Agregado com categorias (barras, linhas): GROUP BY nas categorias. Só agregados (contador): sem GROUP BY.
    # Desagregado (tabela) ou só categorias: as linhas como estão.
    grupo = [c["expression"] for c in campos if not c["expression"].startswith(AGREGADOS)]
    sufixo = " GROUP BY " + ", ".join(grupo) if not desagregado and grupo and len(grupo) < len(campos) else ""
    return f"SELECT {selecao} FROM ({sql_dataset}) d WHERE {onde}{sufixo}"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--warehouse", default=os.getenv("DATABRICKS_WAREHOUSE_ID", ""))
    p.add_argument("--antigo", required=True)
    p.add_argument("--novo", required=True)
    p.add_argument("--schema-antigo", required=True)
    p.add_argument("--schema-novo", required=True)
    p.add_argument("--paginas", default="", help="nomes de página separados por vírgula (padrão: todas)")
    a = p.parse_args()
    ds_ant, _ = datasets(Path(a.antigo), a.schema_antigo)
    ds_nov, dash = datasets(Path(a.novo), a.schema_novo)

    # Valores para os cenários de filtro, tirados dos dados novos (os mais frequentes).
    def mais_frequente(sql, col, onde="TRUE", peso="COUNT(*)"):
        r = consultar(a.warehouse, f"SELECT `{col}` FROM ({sql}) d WHERE {onde} GROUP BY 1 ORDER BY {peso} DESC LIMIT 1")
        return r[0][0] if r else None

    fl = ds_nov["fluxos_empresa"]
    area = mais_frequente(fl, "grande_area_cnae")
    merc = ds_nov["mercado"]
    uf5 = mais_frequente(merc, "uf", peso="SUM(empresas_ativas)")
    mun = mais_frequente(merc, "municipio", f"uf = '{uf5}'", "SUM(empresas_ativas)")
    bai = mais_frequente(merc, "bairro", f"uf = '{uf5}' AND municipio = '{mun}'", "SUM(empresas_ativas)")
    cnae = mais_frequente(merc, "nome_cnae", f"uf = '{uf5}' AND municipio = '{mun}'", "SUM(empresas_ativas)")
    cenarios_fluxo = [{}, {"uf": ["SP"]}, {"uf": ["RJ", "MG"]}, {"porte_empresa": ["MEI"]},
                      {"grande_area_cnae": [area]}, {"uf": ["SP"], "porte_empresa": ["MEI"], "grande_area_cnae": [area]}]
    cenarios_mercado = [{}, {"uf": [uf5]}, {"uf": [uf5], "municipio": [mun]},
                        {"uf": [uf5], "municipio": [mun], "bairro": [bai]},
                        {"uf": [uf5], "municipio": [mun], "bairro": [bai], "nome_cnae": [cnae]},
                        {"nome_cnae": [cnae]}]

    paginas = [x for x in a.paginas.split(",") if x]
    total_consultas, total_difs = 0, 0
    for pagina in dash["pages"]:
        if paginas and pagina["name"] not in paginas:
            continue
        widgets = [i["widget"] for i in pagina["layout"] if "queries" in i["widget"]]
        # campo do filtro -> datasets em que ele atua (um filtro só vale nos datasets das suas queries)
        alvo_filtro: dict[str, set[str]] = {}
        for w in widgets:
            if w["spec"]["widgetType"].startswith("filter"):
                for q in w["queries"]:
                    alvo_filtro.setdefault(q["query"]["fields"][0]["name"], set()).add(q["query"]["datasetName"])
        filtros_pag = set(alvo_filtro)
        base = cenarios_mercado if "municipio" in filtros_pag else cenarios_fluxo
        cenarios = [{k: v for k, v in c.items() if k in filtros_pag} for c in base]
        cenarios = [c for i, c in enumerate(cenarios) if c not in cenarios[:i]] or [{}]
        print(f"\n### página {pagina['name']} — filtros {sorted(filtros_pag) or 'nenhum'}; {len(cenarios)} cenário(s)")
        for w in widgets:
            for q in w["queries"]:
                d = q["query"]["datasetName"]
                campos = q["query"]["fields"]
                eh_filtro = w["spec"]["widgetType"].startswith("filter")
                lista = [{}] if eh_filtro else cenarios
                # só os filtros que atuam neste dataset; cenários que ficam iguais rodam uma vez
                lista = [{k: v for k, v in c.items() if d in alvo_filtro.get(k, ())} for c in lista]
                lista = [c for i, c in enumerate(lista) if c not in lista[:i]]
                for filtros in lista:
                    if eh_filtro:  # lista de valores do dropdown
                        col = campos[0]["name"]
                        sa = f"SELECT DISTINCT `{col}` FROM ({ds_ant[d]}) d"
                        sn = f"SELECT DISTINCT `{col}` FROM ({ds_nov[d]}) d"
                    else:
                        sa = sql_widget(ds_ant[d], campos, q["query"]["disaggregated"], filtros)
                        sn = sql_widget(ds_nov[d], campos, q["query"]["disaggregated"], filtros)
                    try:
                        n, ex = comparar_linhas(consultar(a.warehouse, sa), consultar(a.warehouse, sn))
                    except RuntimeError as e:
                        n, ex = 1, f"ERRO {e}"
                    total_consultas += 1
                    total_difs += 1 if n else 0
                    rotulo = "dropdown" if eh_filtro else (filtros or "sem filtro")
                    print(f"  {'OK ' if not n else 'DIF'} {w['name']} [{d}] {rotulo} {ex if n else ''}", flush=True)
    print(f"\n{total_consultas} comparações, {total_difs} com diferença.")
    sys.exit(1 if total_difs else 0)


if __name__ == "__main__":
    main()
