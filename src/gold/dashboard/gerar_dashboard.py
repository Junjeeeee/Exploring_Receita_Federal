"""Gera `tcc_receita.lvdash.json` (dashboard AI/BI do Databricks) a partir da gold.

    python -m src.gold.dashboard.gerar_dashboard

Importar: Dashboards → Create → Import dashboard from file. Requer um SQL warehouse (ver README).
Os datasets são consultas pequenas e já agregadas; os filtros de página (UF, porte, grande área,
município, CNAE) são filtros de campo aplicados a todos os datasets que têm a coluna.
Spec: docs/specs/features/dashboard-v0.md.
"""

import json
import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[3]
SAIDA = RAIZ / "src/gold/dashboard/tcc_receita.lvdash.json"
LARGURA_GRADE = 12  # grade do workspace (GRID_V1); o repo usava 6

# 2023-05 é a carga inicial (baseline): no primeiro mês não há fluxo POR CONSTRUÇÃO, porque não existe
# snapshot anterior para comparar (nenhuma abertura, baixa ou reativação é detectada). Por isso ele sai das
# séries de fluxo. Limitação (C-13): se um dia a carga começar em outro mês, o MIN passa a acompanhá-la.
SEM_CARGA_INICIAL = "mes_referencia > (SELECT MIN(mes_referencia) FROM tcc_receita.gold.fato_empresa_mes)"

# Regra única de "aberturas": só o `painel` (fato_indicadores_setoriais_mes) conta abertura (C-13).
# A OBT (`eventos`, `situacao`) não é usada para contar abertura.
DATASETS = {
    "painel": ("Painel mês × UF × porte × grande área", f"""
SELECT
    mes_referencia,
    data_referencia,
    uf,
    porte_empresa,
    grande_area_cnae,
    SUM(estoque_inicial_ativas)  AS estoque_inicial_ativas,
    SUM(qtd_aberturas)           AS qtd_aberturas,
    SUM(qtd_baixas)              AS qtd_baixas,
    SUM(qtd_falencias)           AS qtd_falencias,
    SUM(qtd_inaptidoes)          AS qtd_inaptidoes,
    SUM(qtd_reativacoes)         AS qtd_reativacoes,
    SUM(qtd_entradas_estoque)    AS qtd_entradas_estoque,
    SUM(qtd_saidas_estoque)      AS qtd_saidas_estoque,
    SUM(qtd_baixas_de_ativas)    AS qtd_baixas_de_ativas,
    SUM(qtd_suspensoes)          AS qtd_suspensoes,
    SUM(crescimento_liquido)     AS crescimento_liquido
FROM tcc_receita.gold.fato_indicadores_setoriais_mes
WHERE {SEM_CARGA_INICIAL}
GROUP BY ALL
"""),
    "estoque": ("Empresas ativas no último mês", """
SELECT uf, porte_empresa, grande_area_cnae, SUM(estoque_final_ativas) AS empresas_ativas
FROM tcc_receita.gold.fato_indicadores_setoriais_mes
WHERE mes_referencia = (SELECT MAX(mes_referencia) FROM tcc_receita.gold.fato_indicadores_setoriais_mes)
GROUP BY ALL
"""),
    "taxas_setor": ("Taxas mensais por 1.000 ativas, por grande área (Brasil)", f"""
SELECT
    grande_area_cnae,
    ROUND(1000.0 * SUM(qtd_aberturas)        / NULLIF(SUM(estoque_inicial_ativas), 0), 2) AS taxa_abertura_por_mil,
    ROUND(1000.0 * SUM(qtd_baixas_de_ativas) / NULLIF(SUM(estoque_inicial_ativas), 0), 2) AS taxa_fechamento_por_mil
FROM tcc_receita.gold.fato_indicadores_setoriais_mes
WHERE {SEM_CARGA_INICIAL}
GROUP BY grande_area_cnae
"""),
    # Crescimento líquido por 1.000 ativas, por UF. O widget não faz razão de somas, então a taxa já vem pronta
    # (por isso só o filtro de UF reage; porte e grande área não existem neste dataset).
    "taxas_uf": ("Crescimento líquido por 1.000 ativas, por UF (média mensal do período)", f"""
SELECT
    uf,
    ROUND(1000.0 * SUM(crescimento_liquido) / NULLIF(SUM(estoque_inicial_ativas), 0), 2) AS taxa_crescimento_por_mil
FROM tcc_receita.gold.fato_indicadores_setoriais_mes
WHERE {SEM_CARGA_INICIAL}
GROUP BY uf
"""),
    # OBT: eventos de todos os tipos (empresa, sócio, CNAE...). NÃO é fonte de aberturas.
    "eventos": ("Eventos por tipo e categoria", """
SELECT
    data_referencia,
    tipo_evento,
    categoria_macro_evento,
    entidade_tipo,
    eh_matriz,
    uf,
    porte_empresa,
    grande_area_cnae,
    COUNT(*) AS qtd_eventos
FROM tcc_receita.gold.obt_dashboard_eventos
GROUP BY ALL
"""),
    # Só mudança de situação cadastral da matriz, contada em empresas distintas (C-13).
    "situacao": ("Empresas com mudança de situação cadastral (matriz)", """
SELECT
    data_referencia,
    categoria_macro_evento,
    uf,
    porte_empresa,
    grande_area_cnae,
    COUNT(DISTINCT cnpj_raiz) AS qtd_empresas
FROM tcc_receita.gold.obt_dashboard_eventos
WHERE tipo_evento = 'MUDOU ESTADO CADASTRAL' AND eh_matriz = TRUE
GROUP BY ALL
"""),
    "top_cnaes": ("CNAEs com mais aberturas no período (Brasil)", f"""
SELECT
    nome_cnae,
    grande_area_cnae,
    SUM(qtd_aberturas)       AS aberturas,
    SUM(qtd_baixas)          AS baixas,
    SUM(crescimento_liquido) AS saldo
FROM tcc_receita.gold.fato_indicadores_setoriais_mes
WHERE {SEM_CARGA_INICIAL}
GROUP BY ALL
ORDER BY aberturas DESC
LIMIT 20
"""),
    "macro": ("Selic e IPCA no período", """
SELECT m.data_referencia, m.selic_anualizada_media_pct, m.ipca_acumulado_12m_pct
FROM tcc_receita.gold.fato_indicadores_macro_mes m
WHERE m.mes_referencia IN (SELECT DISTINCT mes_referencia FROM tcc_receita.gold.fato_empresa_mes)
"""),
    # Página 2. Sem razão social: MEI costuma ter nome de pessoa física na razão social.
    "mercado": ("Mercado ativo (sem identificação)", """
SELECT uf, municipio, bairro, porte_empresa, grande_area_cnae, nome_cnae, COUNT(*) AS empresas_ativas
FROM tcc_receita.gold.dashboard_mercado_ativo
GROUP BY ALL
"""),
    # 20 maiores bairros de cada município (o widget não tem top-N). Sem CNAE: o filtro de CNAE não vale aqui.
    "mercado_bairros": ("Mercado ativo: 20 maiores bairros por município", """
SELECT uf, municipio, bairro, empresas_ativas
FROM (
    SELECT
        uf, municipio, bairro,
        COUNT(*) AS empresas_ativas,
        ROW_NUMBER() OVER (PARTITION BY uf, municipio ORDER BY COUNT(*) DESC) AS posicao
    FROM tcc_receita.gold.dashboard_mercado_ativo
    GROUP BY uf, municipio, bairro
)
WHERE posicao <= 20
"""),
    # LGPD: `exibir_razao_social` (gold) é FALSE para MEI, empresário individual (2135) e razão social com
    # 11 dígitos seguidos. Sem endereço além de município/bairro. LIMIT: não devolver milhões de linhas.
    "mercado_empresas": ("Maiores empresas ativas (razão social liberada)", """
SELECT razao_social, nome_fantasia, porte_empresa, capital_social, data_inicio_atividade,
       uf, municipio, bairro, grande_area_cnae, nome_cnae
FROM tcc_receita.gold.dashboard_mercado_ativo
WHERE exibir_razao_social
ORDER BY capital_social DESC
LIMIT 1000
"""),
}

# Colunas que cada dataset entrega (usado só na validação; conferido também contra o texto do SELECT).
COLUNAS = {
    "painel": ["mes_referencia", "data_referencia", "uf", "porte_empresa", "grande_area_cnae",
               "estoque_inicial_ativas", "qtd_aberturas", "qtd_baixas", "qtd_falencias", "qtd_inaptidoes",
               "qtd_reativacoes", "qtd_entradas_estoque", "qtd_saidas_estoque", "qtd_baixas_de_ativas",
               "qtd_suspensoes", "crescimento_liquido"],
    "estoque": ["uf", "porte_empresa", "grande_area_cnae", "empresas_ativas"],
    "taxas_setor": ["grande_area_cnae", "taxa_abertura_por_mil", "taxa_fechamento_por_mil"],
    "taxas_uf": ["uf", "taxa_crescimento_por_mil"],
    "eventos": ["data_referencia", "tipo_evento", "categoria_macro_evento", "entidade_tipo", "eh_matriz",
                "uf", "porte_empresa", "grande_area_cnae", "qtd_eventos"],
    "situacao": ["data_referencia", "categoria_macro_evento", "uf", "porte_empresa", "grande_area_cnae",
                 "qtd_empresas"],
    "top_cnaes": ["nome_cnae", "grande_area_cnae", "aberturas", "baixas", "saldo"],
    "macro": ["data_referencia", "selic_anualizada_media_pct", "ipca_acumulado_12m_pct"],
    "mercado": ["uf", "municipio", "bairro", "porte_empresa", "grande_area_cnae", "nome_cnae",
                "empresas_ativas"],
    "mercado_bairros": ["uf", "municipio", "bairro", "empresas_ativas"],
    "mercado_empresas": ["razao_social", "nome_fantasia", "porte_empresa", "capital_social",
                         "data_inicio_atividade", "uf", "municipio", "bairro", "grande_area_cnae", "nome_cnae"],
}


# --- campos -------------------------------------------------------------------------------------

def soma(coluna):
    return {"name": f"sum({coluna})", "expression": f"SUM(`{coluna}`)"}


def coluna(nome):
    return {"name": nome, "expression": f"`{nome}`"}


def mes(nome="data_referencia"):
    return {"name": f"monthly({nome})", "expression": f'DATE_TRUNC("MONTH", `{nome}`)'}


# --- widgets ------------------------------------------------------------------------------------

def _query(dataset, campos, desagregado=False, nome="main_query"):
    return {"name": nome, "query": {"datasetName": dataset, "fields": campos, "disaggregated": desagregado}}


def _frame(titulo, descricao=None):
    frame = {"showTitle": True, "title": titulo}
    if descricao:
        frame.update(showDescription=True, description=descricao)
    return frame


def texto(nome, linhas):
    # O Databricks junta os itens de `lines` numa string só; sem "\n" no fim de cada item o título cola no parágrafo.
    return {"name": nome, "multilineTextboxSpec": {"lines": [l + "\n" for l in linhas]}}


def contador(nome, titulo, dataset, campo):
    return {
        "name": nome,
        "queries": [_query(dataset, [campo])],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": campo["name"], "displayName": titulo}},
                 "frame": _frame(titulo)},
    }


def linha(nome, titulo, dataset, x, ys, descricao=None):
    """ys: lista de (campo, rótulo)."""
    return {
        "name": nome,
        "queries": [_query(dataset, [x] + [c for c, _ in ys])],
        "spec": {"version": 3, "widgetType": "line",
                 "encodings": {
                     "x": {"fieldName": x["name"], "scale": {"type": "temporal"}, "displayName": "Mês"},
                     "y": {"scale": {"type": "quantitative"},
                           "fields": [{"fieldName": c["name"], "displayName": r} for c, r in ys]},
                 },
                 "frame": _frame(titulo, descricao)},
    }


def barras(nome, titulo, dataset, categoria, valor, rotulos, cor=None, horizontal=False, temporal=False,
           descricao=None):
    """rotulos: (rótulo da categoria, rótulo do valor). horizontal = categoria no eixo y, ordenada por valor."""
    campos = [categoria, valor] + ([cor] if cor else [])
    tipo_cat = "temporal" if temporal else "categorical"
    eixo_cat = {"fieldName": categoria["name"], "scale": {"type": tipo_cat}, "displayName": rotulos[0]}
    eixo_val = {"fieldName": valor["name"], "scale": {"type": "quantitative"}, "displayName": rotulos[1]}
    if horizontal:
        eixo_cat["scale"]["sort"] = {"by": "x-reversed"}
        encodings = {"x": eixo_val, "y": eixo_cat}
    else:
        encodings = {"x": eixo_cat, "y": eixo_val}
    if cor:
        encodings["color"] = {"fieldName": cor["name"], "scale": {"type": "categorical"}, "displayName": "Categoria"}
    return {
        "name": nome,
        "queries": [_query(dataset, campos)],
        "spec": {"version": 3, "widgetType": "bar", "encodings": encodings, "frame": _frame(titulo, descricao)},
    }


def tabela(nome, titulo, dataset, colunas, descricao=None):
    """colunas: lista de (coluna, rótulo)."""
    return {
        "name": nome,
        "queries": [_query(dataset, [coluna(c) for c, _ in colunas], desagregado=True)],
        "spec": {"version": 2, "widgetType": "table",
                 "encodings": {"columns": [{"fieldName": c, "displayName": r} for c, r in colunas]},
                 "frame": _frame(titulo, descricao)},
    }


def filtro(nome, titulo, campo, datasets, multiplo=True):
    """Filtro de campo aplicado a todos os `datasets` (todos precisam ter a coluna)."""
    queries = [_query(d, [coluna(campo)], nome=f"filtro_{nome}_{d}") for d in datasets]
    return {
        "name": nome,
        "queries": queries,
        "spec": {"version": 2,
                 "widgetType": "filter-multi-select" if multiplo else "filter-single-select",
                 "encodings": {"fields": [{"fieldName": campo, "displayName": titulo, "queryName": q["name"]}
                                          for q in queries]},
                 "frame": _frame(titulo)},
    }


def pos(x, y, largura, altura):
    return {"x": x, "y": y, "width": largura, "height": altura}


# --- páginas (grade de 12 colunas, como o workspace regrava: layoutVersion GRID_V1) ------------------

PAGINA_VISAO_GERAL = [
    (texto("titulo", [
        "# Dinâmica empresarial no Brasil — CNPJ / Receita Federal",
        "Aberturas, fechamentos e mudanças cadastrais derivadas de snapshots mensais (SCD2 + log de eventos). "
        "Grão: empresa (matriz do CNPJ raiz). **Protótipo** — janela de teste; 2023-05 é carga inicial e não "
        "entra nos fluxos.",
    ]), pos(0, 0, 12, 3)),
    (filtro("f_uf", "UF", "uf", ["painel", "estoque", "eventos", "situacao", "taxas_uf"]), pos(0, 3, 4, 1)),
    (filtro("f_porte", "Porte", "porte_empresa", ["painel", "estoque", "eventos", "situacao"]), pos(4, 3, 4, 1)),
    (filtro("f_area", "Grande área (provisório)", "grande_area_cnae",
            ["painel", "estoque", "eventos", "situacao"]), pos(8, 3, 4, 1)),

    (contador("c_ativas", "Empresas ativas (último mês)", "estoque", soma("empresas_ativas")), pos(0, 4, 4, 3)),
    (contador("c_aberturas", "Aberturas no período", "painel", soma("qtd_aberturas")), pos(4, 4, 4, 3)),
    (contador("c_baixas", "Baixas no período", "painel", soma("qtd_baixas")), pos(8, 4, 4, 3)),
    (contador("c_reativacoes", "Reativações no período", "painel", soma("qtd_reativacoes")), pos(0, 7, 6, 3)),
    (contador("c_saldo", "Saldo do período (entradas − saídas do estoque de ativas)", "painel",
              soma("crescimento_liquido")), pos(6, 7, 6, 3)),

    (linha("l_fluxos", "Fluxos mensais: aberturas, baixas, reativações e inaptidões", "painel", mes(), [
        (soma("qtd_aberturas"), "Aberturas"), (soma("qtd_baixas"), "Baixas"),
        (soma("qtd_reativacoes"), "Reativações"), (soma("qtd_inaptidoes"), "Inaptidões"),
    ], descricao="Por mês de detecção no snapshot da Receita."), pos(0, 10, 8, 6)),
    (linha("l_macro", "Contexto macro: Selic e IPCA 12m (% a.a.)", "macro", mes(), [
        (soma("selic_anualizada_media_pct"), "Selic média"), (soma("ipca_acumulado_12m_pct"), "IPCA 12m"),
    ], descricao="Seed do BCB em fato_indicadores_macro_mes."), pos(8, 10, 4, 6)),

    (barras("b_saldo_uf", "Crescimento líquido por 1.000 ativas, por UF", "taxas_uf", coluna("uf"),
            soma("taxa_crescimento_por_mil"), ("UF", "Por mil ativas"), horizontal=True,
            descricao="(Entradas − saídas) ÷ ativas no início do mês, média do período. Só o filtro de UF reage."),
     pos(0, 16, 6, 8)),
    (barras("b_taxa_area", "Taxa mensal de fechamento por 1.000 ativas — Brasil", "taxas_setor",
            coluna("grande_area_cnae"), soma("taxa_fechamento_por_mil"), ("Grande área", "Por mil"),
            horizontal=True, descricao="Baixas de empresas ativas ÷ ativas no início do mês. Não reage aos filtros."),
     pos(6, 16, 6, 8)),

    (barras("b_situacao", "Mudanças de situação cadastral por categoria", "situacao", mes(),
            soma("qtd_empresas"), ("Mês", "Empresas"), cor=coluna("categoria_macro_evento"), temporal=True,
            descricao="Só matrizes; empresas distintas por mês de detecção. Não inclui abertura."),
     pos(0, 24, 6, 7)),
    (barras("b_tipo", "Eventos por tipo no período", "eventos", coluna("tipo_evento"), soma("qtd_eventos"),
            ("Tipo de evento", "Eventos"), horizontal=True,
            descricao="Todos os eventos do log (inclui filiais e sócios); abertura de empresa vem do painel."),
     pos(6, 24, 6, 7)),

    (tabela("t_cnaes", "Top 20 CNAEs por aberturas — Brasil", "top_cnaes", [
        ("nome_cnae", "CNAE principal"), ("grande_area_cnae", "Grande área"),
        ("aberturas", "Aberturas"), ("baixas", "Baixas"), ("saldo", "Saldo"),
    ], descricao="Não reage aos filtros."), pos(0, 31, 12, 8)),

    (texto("notas", [
        "### Notas de leitura",
        "- Os fluxos são por **mês de detecção** no snapshot da Receita (extraído por volta do dia 10): "
        "o mês M cobre, aproximadamente, de meados de M−1 a meados de M.",
        "- **Empresa** = matriz do CNPJ raiz; eventos de filial não entram nos indicadores de empresa.",
        "- **2023-05** é carga inicial (baseline): não há fluxo nesse mês.",
        "- **Saldo** = entradas − saídas do estoque de ativas: inclui reativações, inaptidões e suspensões, "
        "não só aberturas e baixas.",
        "- Grande área é um agrupamento provisório (D-10). Sem nomes de sócios nem endereços (LGPD).",
    ]), pos(0, 39, 12, 5)),
]

COM_FILTROS_P2 = ["mercado", "mercado_bairros", "mercado_empresas"]

PAGINA_MERCADO = [
    (texto("titulo_mercado", [
        "# Mercado ativo — concorrência local",
        "Matrizes ativas na versão mais recente. Escolha UF, município e CNAE para ver o tamanho do mercado.",
    ]), pos(0, 0, 12, 3)),
    (filtro("f2_uf", "UF", "uf", COM_FILTROS_P2), pos(0, 3, 4, 1)),
    (filtro("f2_municipio", "Município", "municipio", COM_FILTROS_P2), pos(4, 3, 4, 1)),
    # Bairros não têm CNAE: o filtro de CNAE vale só para `mercado` e `mercado_empresas`.
    (filtro("f2_cnae", "CNAE principal", "nome_cnae", ["mercado", "mercado_empresas"]), pos(8, 3, 4, 1)),

    (contador("c2_ativas", "Empresas ativas", "mercado", soma("empresas_ativas")), pos(0, 4, 4, 4)),
    (barras("b2_porte", "Ativas por porte", "mercado", coluna("porte_empresa"), soma("empresas_ativas"),
            ("Porte", "Empresas")), pos(4, 4, 8, 4)),
    (barras("b2_bairro", "Ativas por bairro (20 maiores de cada município)", "mercado_bairros", coluna("bairro"),
            soma("empresas_ativas"), ("Bairro", "Empresas"), horizontal=True,
            descricao="Escolha um município. O filtro de CNAE não reage neste gráfico."), pos(0, 8, 6, 8)),
    (barras("b2_area", "Ativas por grande área", "mercado", coluna("grande_area_cnae"), soma("empresas_ativas"),
            ("Grande área", "Empresas"), horizontal=True), pos(6, 8, 6, 8)),
    (tabela("t2_empresas", "Maiores empresas ativas (1.000 maiores por capital social)", "mercado_empresas", [
        ("razao_social", "Razão social"), ("nome_fantasia", "Nome fantasia"), ("porte_empresa", "Porte"),
        ("capital_social", "Capital social (R$)"), ("data_inicio_atividade", "Início"),
        ("municipio", "Município"), ("bairro", "Bairro"), ("nome_cnae", "CNAE"),
    ], descricao="Fora MEI, empresário individual e razão social com 11 dígitos seguidos (LGPD). "
                 "Os filtros atuam sobre as 1.000 maiores do Brasil."), pos(0, 16, 12, 9)),
]


def _pagina(nome, titulo, widgets):
    return {"name": nome, "displayName": titulo,
            "layout": [{"widget": w, "position": p} for w, p in widgets],
            "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1"}


def montar_dashboard() -> dict:
    datasets = [
        {"name": nome, "displayName": titulo, "queryLines": [l + "\n" for l in sql.strip().splitlines()]}
        for nome, (titulo, sql) in DATASETS.items()
    ]
    return {
        "datasets": datasets,
        "pages": [
            _pagina("visao_geral", "Visão geral", PAGINA_VISAO_GERAL),
            _pagina("mercado_ativo", "Mercado ativo", PAGINA_MERCADO),
        ],
    }


# --- validação local ----------------------------------------------------------------------------

def validar(dash: dict) -> None:
    """Falha (SystemExit) com mensagem clara se o JSON tiver referência quebrada ou widgets sobrepostos."""
    erros = []

    # 1. o dicionário COLUNAS tem de bater com o texto do SELECT (cada coluna aparece como palavra inteira)
    for nome, (_, sql) in DATASETS.items():
        if nome not in COLUNAS:
            erros.append(f"dataset '{nome}' sem entrada em COLUNAS")
            continue
        for c in COLUNAS[nome]:
            if not re.search(rf"\b{c}\b", sql):
                erros.append(f"dataset '{nome}': coluna '{c}' não aparece no SELECT")

    for pagina in dash["pages"]:
        retangulos = []
        for item in pagina["layout"]:
            w, p = item["widget"], item["position"]
            nome = w["name"]
            retangulos.append((nome, p))
            if p["x"] < 0 or p["x"] + p["width"] > LARGURA_GRADE:
                erros.append(f"{pagina['name']}/{nome}: fora da grade de {LARGURA_GRADE} colunas")
            campos_por_query = {}
            for q in w.get("queries", []):
                d = q["query"]["datasetName"]
                if d not in COLUNAS:
                    erros.append(f"{nome}: dataset '{d}' não existe")
                    continue
                campos_por_query[q["name"]] = {f["name"] for f in q["query"]["fields"]}
                for f in q["query"]["fields"]:
                    for col in re.findall(r"`(\w+)`", f["expression"]):
                        if col not in COLUNAS[d]:
                            erros.append(f"{nome}: dataset '{d}' não tem a coluna '{col}'")
            if "queries" in w:
                erros += _validar_encodings(nome, w["spec"]["encodings"], campos_por_query)

        for i, (n1, a) in enumerate(retangulos):
            for n2, b in retangulos[i + 1:]:
                if (a["x"] < b["x"] + b["width"] and b["x"] < a["x"] + a["width"]
                        and a["y"] < b["y"] + b["height"] and b["y"] < a["y"] + a["height"]):
                    erros.append(f"{pagina['name']}: '{n1}' e '{n2}' se sobrepõem")

    if erros:
        raise SystemExit("❌ Validação do dashboard falhou:\n  - " + "\n  - ".join(erros))
    print("✅ Validação ok: datasets, colunas, filtros e grade consistentes.")


def _validar_encodings(widget, encodings, campos_por_query):
    """Todo fieldName dos encodings tem de existir nos fields da query (filtros: na query indicada)."""
    erros = []
    todos = set().union(*campos_por_query.values()) if campos_por_query else set()

    def confere(enc):
        if isinstance(enc, dict):
            if "fieldName" in enc:
                alvo = campos_por_query.get(enc.get("queryName"), todos)
                if enc["fieldName"] not in alvo:
                    erros.append(f"{widget}: fieldName '{enc['fieldName']}' não está nos fields da query")
            for v in enc.values():
                confere(v)
        elif isinstance(enc, list):
            for v in enc:
                confere(v)

    confere(encodings)
    return erros


def main() -> None:
    dash = montar_dashboard()
    validar(dash)
    SAIDA.write_text(json.dumps(dash, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"✅ {SAIDA.relative_to(RAIZ)} gerado.")


if __name__ == "__main__":
    main()
