"""Gera `tcc_receita.lvdash.json` (dashboard AI/BI do Databricks) a partir da gold.

    python -m src.gold.dashboard.gerar_dashboard

Importar: Dashboards → Create → Import dashboard from file. Requer um SQL warehouse (ver README).
Os datasets são consultas pequenas e já agregadas; os filtros de página (UF, porte, grande área,
município, CNAE) são filtros de campo aplicados a todos os datasets que têm a coluna.
Spec: docs/specs/features/dashboard-v0.md.
"""

import json
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[3]
SAIDA = RAIZ / "src/gold/dashboard/tcc_receita.lvdash.json"

# 2023-05 é a carga inicial (sem eventos nem estoque inicial): fica fora das séries de fluxo.
SEM_CARGA_INICIAL = "mes_referencia > (SELECT MIN(mes_referencia) FROM tcc_receita.gold.fato_empresa_mes)"

DATASETS = {
    "painel": ("Painel mês × UF × porte × grande área", f"""
SELECT
    mes_referencia,
    data_referencia,
    uf,
    porte_empresa,
    grande_area_cnae,
    SUM(estoque_inicial_ativas) AS estoque_inicial_ativas,
    SUM(qtd_aberturas)          AS qtd_aberturas,
    SUM(qtd_baixas)             AS qtd_baixas,
    SUM(qtd_falencias)          AS qtd_falencias,
    SUM(qtd_inaptidoes)         AS qtd_inaptidoes,
    SUM(crescimento_liquido)    AS crescimento_liquido
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
    ROUND(1000.0 * SUM(qtd_aberturas) / NULLIF(SUM(estoque_inicial_ativas), 0), 2) AS taxa_abertura_por_mil,
    ROUND(1000.0 * SUM(qtd_baixas)    / NULLIF(SUM(estoque_inicial_ativas), 0), 2) AS taxa_fechamento_por_mil
FROM tcc_receita.gold.fato_indicadores_setoriais_mes
WHERE {SEM_CARGA_INICIAL}
GROUP BY grande_area_cnae
"""),
    "eventos": ("Eventos por tipo e categoria", """
SELECT
    data_referencia,
    tipo_evento,
    categoria_macro_evento,
    uf,
    porte_empresa,
    grande_area_cnae,
    COUNT(*) AS qtd_eventos
FROM tcc_receita.gold.obt_dashboard_eventos
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
    # LGPD: só não-MEI e sem endereço (docs/tcc.md § Ética/LGPD).
    "mercado_empresas": ("Maiores empresas ativas (não MEI)", """
SELECT razao_social, nome_fantasia, porte_empresa, capital_social, data_inicio_atividade,
       uf, municipio, bairro, grande_area_cnae, nome_cnae
FROM tcc_receita.gold.dashboard_mercado_ativo
WHERE NOT COALESCE(opcao_pelo_mei, FALSE) AND porte_empresa <> 'MEI'
"""),
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
    return {"name": nome, "multilineTextboxSpec": {"lines": linhas}}


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


# --- páginas (grade de 6 colunas) ---------------------------------------------------------------

COM_FILTROS_P1 = ["painel", "estoque", "eventos"]

PAGINA_VISAO_GERAL = [
    (texto("titulo", [
        "# Dinâmica empresarial no Brasil — CNPJ / Receita Federal",
        "Aberturas, fechamentos e mudanças cadastrais derivadas de snapshots mensais (SCD2 + log de eventos). "
        "Grão: empresa (CNPJ raiz). **Protótipo** — janela de teste; 2023-05 é carga inicial e não entra nos fluxos.",
    ]), pos(0, 0, 6, 2)),
    (filtro("f_uf", "UF", "uf", COM_FILTROS_P1), pos(0, 2, 2, 1)),
    (filtro("f_porte", "Porte", "porte_empresa", COM_FILTROS_P1), pos(2, 2, 2, 1)),
    (filtro("f_area", "Grande área (provisório)", "grande_area_cnae", COM_FILTROS_P1), pos(4, 2, 2, 1)),

    (contador("c_ativas", "Empresas ativas (último mês)", "estoque", soma("empresas_ativas")), pos(0, 3, 2, 3)),
    (contador("c_aberturas", "Aberturas no período", "painel", soma("qtd_aberturas")), pos(2, 3, 2, 3)),
    (contador("c_baixas", "Baixas no período", "painel", soma("qtd_baixas")), pos(4, 3, 2, 3)),

    (linha("l_fluxos", "Aberturas × baixas por mês", "painel", mes(), [
        (soma("qtd_aberturas"), "Aberturas"), (soma("qtd_baixas"), "Baixas"),
        (soma("qtd_inaptidoes"), "Inaptidões"),
    ]), pos(0, 6, 4, 6)),
    (linha("l_macro", "Contexto macro: Selic e IPCA 12m (% a.a.)", "macro", mes(), [
        (soma("selic_anualizada_media_pct"), "Selic média"), (soma("ipca_acumulado_12m_pct"), "IPCA 12m"),
    ], descricao="Seed do BCB em fato_indicadores_macro_mes."), pos(4, 6, 2, 6)),

    (barras("b_saldo_uf", "Saldo (aberturas − baixas) por UF", "painel", coluna("uf"),
            soma("crescimento_liquido"), ("UF", "Saldo"), horizontal=True), pos(0, 12, 3, 8)),
    (barras("b_taxa_area", "Taxa mensal de fechamento por 1.000 ativas — Brasil", "taxas_setor",
            coluna("grande_area_cnae"), soma("taxa_fechamento_por_mil"), ("Grande área", "Por mil"),
            horizontal=True, descricao="Baixas ÷ ativas no início do mês. Não reage aos filtros."),
     pos(3, 12, 3, 8)),

    (barras("b_situacao", "Mudanças de situação cadastral por categoria", "eventos", mes(),
            soma("qtd_eventos"), ("Mês", "Eventos"), cor=coluna("categoria_macro_evento"), temporal=True,
            descricao="Inclui abertura e fechamentos (categoria derivada do motivo da situação)."),
     pos(0, 20, 3, 7)),
    (barras("b_tipo", "Eventos por tipo no período", "eventos", coluna("tipo_evento"), soma("qtd_eventos"),
            ("Tipo de evento", "Eventos"), horizontal=True), pos(3, 20, 3, 7)),

    (tabela("t_cnaes", "Top 20 CNAEs por aberturas — Brasil", "top_cnaes", [
        ("nome_cnae", "CNAE principal"), ("grande_area_cnae", "Grande área"),
        ("aberturas", "Aberturas"), ("baixas", "Baixas"), ("saldo", "Saldo"),
    ], descricao="Não reage aos filtros."), pos(0, 27, 6, 8)),

    (texto("notas", [
        "### Notas de leitura",
        "- **Empresa** = CNPJ raiz (8 dígitos); situação e endereço vêm da matriz.",
        "- **Aberturas** e **baixas** vêm do log de eventos (diferença entre snapshots consecutivos).",
        "- **Grande área** é um agrupamento provisório das seções CNAE (decisão D-10 em aberto).",
        "- Sem nomes de sócios nem endereços (LGPD).",
    ]), pos(0, 35, 6, 3)),
]

COM_FILTROS_P2 = ["mercado", "mercado_empresas"]

PAGINA_MERCADO = [
    (texto("titulo_mercado", [
        "# Mercado ativo — concorrência local",
        "Matrizes ativas na versão mais recente. Escolha UF, município e CNAE para ver o tamanho do mercado.",
    ]), pos(0, 0, 6, 2)),
    (filtro("f2_uf", "UF", "uf", COM_FILTROS_P2), pos(0, 2, 2, 1)),
    (filtro("f2_municipio", "Município", "municipio", COM_FILTROS_P2), pos(2, 2, 2, 1)),
    (filtro("f2_cnae", "CNAE principal", "nome_cnae", COM_FILTROS_P2), pos(4, 2, 2, 1)),

    (contador("c2_ativas", "Empresas ativas", "mercado", soma("empresas_ativas")), pos(0, 3, 2, 4)),
    (barras("b2_porte", "Ativas por porte", "mercado", coluna("porte_empresa"), soma("empresas_ativas"),
            ("Porte", "Empresas")), pos(2, 3, 4, 4)),
    (barras("b2_bairro", "Ativas por bairro", "mercado", coluna("bairro"), soma("empresas_ativas"),
            ("Bairro", "Empresas"), horizontal=True), pos(0, 7, 3, 8)),
    (barras("b2_area", "Ativas por grande área", "mercado", coluna("grande_area_cnae"), soma("empresas_ativas"),
            ("Grande área", "Empresas"), horizontal=True), pos(3, 7, 3, 8)),
    (tabela("t2_empresas", "Empresas ativas (não MEI)", "mercado_empresas", [
        ("razao_social", "Razão social"), ("nome_fantasia", "Nome fantasia"), ("porte_empresa", "Porte"),
        ("capital_social", "Capital social (R$)"), ("data_inicio_atividade", "Início"),
        ("municipio", "Município"), ("bairro", "Bairro"), ("nome_cnae", "CNAE"),
    ], descricao="MEI excluído (razão social com nome de pessoa física). Ordene por capital social."),
     pos(0, 15, 6, 9)),
]


def _pagina(nome, titulo, widgets):
    return {"name": nome, "displayName": titulo, "pageType": "PAGE_TYPE_CANVAS",
            "layout": [{"widget": w, "position": p} for w, p in widgets]}


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


def main() -> None:
    SAIDA.write_text(json.dumps(montar_dashboard(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"✅ {SAIDA.relative_to(RAIZ)} gerado.")


if __name__ == "__main__":
    main()
