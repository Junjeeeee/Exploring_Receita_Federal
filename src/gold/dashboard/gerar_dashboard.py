"""Gera `tcc_receita.lvdash.json` (dashboard AI/BI do Databricks) a partir da gold.

    python -m src.gold.dashboard.gerar_dashboard

Importar: Dashboards → Create → Import dashboard from file. Requer um SQL warehouse (ver README).
Seis páginas, uma pergunta por página (spec: docs/specs/features/2026-11-14-dashboard-v2-narrativa.md):
Resumo → Quem → Onde → Como fecha → Mercado ativo → Sobre os dados.

Como os números respeitam os filtros (filtro de campo = WHERE nas linhas do dataset, antes do widget agregar):
- "último mês" e "Δ vs. mês anterior" são colunas que o dataset já entrega por linha; o widget só soma;
- percentuais são AVG de um indicador 0/100 num dataset de uma linha por empresa (AVG respeita o filtro);
- taxas por 1.000 ativas são razões de somas: vêm prontas do dataset e só reagem aos filtros que o dataset tem.

Datasets grandes (páginas 2, 4 e 5) leem tabelas gold `dash_*` que o Job já grava no formato do gráfico
(src/gold/dashboard_agregados.sql; spec 2026-11-16-otimizacao-log-fato-dashboard.md §3.5). Motivo: no Lakeview,
dataset com mais de 100 mil linhas roda a consulta de novo no warehouse a CADA clique; com a conta pronta na
tabela, cada clique só lê uma tabela estreita. Os números são os mesmos (mesmo SQL, gravado pelo Job).
"""

import argparse
import json
import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[3]
SAIDA = RAIZ / "src/gold/dashboard/tcc_receita.lvdash.json"
LARGURA_GRADE = 12  # grade do workspace (GRID_V1)

PAINEL = "tcc_receita.gold.fato_indicadores_setoriais_mes"
FATO = "tcc_receita.gold.fato_empresa_mes"

# 2023-05 é a carga inicial (baseline): no primeiro mês não há fluxo POR CONSTRUÇÃO, porque não existe
# snapshot anterior para comparar. Por isso ele sai das séries de fluxo. Limitação (C-13): se um dia a carga
# começar em outro mês, o MIN passa a acompanhá-la.
SEM_CARGA_INICIAL = f"mes_referencia > (SELECT MIN(mes_referencia) FROM {FATO})"

# Tabelas gold no formato de cada gráfico (gravadas pelo Job em src/gold/dashboard_agregados.sql). A regra do
# bairro do "Mercado ativo" (nulo ou vazio vira 'NAO INFORMADO') e o corte da carga inicial em dash_fluxos_mes
# ficam lá, no SQL que grava as tabelas.
DASH = "tcc_receita.gold.dash_"

# Ordem das séries de cor por porte. Fixa porque a hierarquia é estável (amostra jun–ago/2023:
# MEI > ME > DEMAIS > EPP, nas aberturas e nas baixas). No Lakeview o PRIMEIRO valor da lista vai para o TOPO da
# pilha (conferido na tela pelo autor), por isso a lista vai do menor para o maior: MEI fica na base.
ORDEM_PORTE = ["NAO IDENTIFICADO", "NAO INFORMADO", "EPP", "DEMAIS", "ME", "MEI"]
# Faixas de idade: ordem natural (da mais nova para a mais velha), não por volume.
ORDEM_FAIXA_IDADE = ["00-01 ano", "01-02 anos", "02-05 anos", "05-10 anos", "10+ anos", "sem data"]

# Piso de volume dos rankings: grupo com média mensal de ativas no início do mês abaixo disso não é ranqueado
# (taxa de grupo pequeno é ruído). Na base real todas as UFs e grandes áreas passam.
PISO_ATIVAS = 100

# Taxas mensais por 1.000 ativas no início do mês, por grupo. SUM(numerador) / SUM(denominador) somados nos
# meses de fluxo = média mensal ponderada pelo estoque.
_TAXAS_POR = """
WITH taxas AS (
    SELECT
        {grupo},
        SUM(estoque_inicial_ativas) / COUNT(DISTINCT mes_referencia) AS ativas_inicio_media_mensal,
        ROUND(1000.0 * SUM(qtd_aberturas)        / NULLIF(SUM(estoque_inicial_ativas), 0), 2) AS taxa_abertura_por_mil,
        ROUND(1000.0 * SUM(qtd_baixas_de_ativas) / NULLIF(SUM(estoque_inicial_ativas), 0), 2) AS taxa_fechamento_por_mil,
        ROUND(1000.0 * SUM(crescimento_liquido)  / NULLIF(SUM(estoque_inicial_ativas), 0), 2) AS {taxa_saldo}
    FROM """ + PAINEL + """
    WHERE """ + SEM_CARGA_INICIAL + """
      AND {excluir}
    GROUP BY {grupo}
    HAVING SUM(estoque_inicial_ativas) / COUNT(DISTINCT mes_referencia) >= """ + str(PISO_ATIVAS) + """
)"""


def _taxas(grupo, taxa_saldo, excluir):
    return _TAXAS_POR.format(grupo=grupo, taxa_saldo=taxa_saldo, excluir=excluir)


# Rótulos de "não sei" não entram em ranking: CNAE não localizado / não especificado; UF não identificada (NI)
# e exterior (EX).
TAXAS_SETOR = _taxas("grande_area_cnae", "taxa_saldo_por_mil",
                     "grande_area_cnae NOT IN ('OUTROS / NAO ESPECIFICADO', 'CODIGO NAO LOCALIZADO')")
TAXAS_UF = _taxas("uf", "taxa_crescimento_por_mil", "uf NOT IN ('NI', 'EX')")


def _extremos(cte, grupo, taxa):
    """Duas linhas: o grupo com a maior e o com a menor taxa de saldo (empate: ordem alfabética)."""
    return cte + f""",
ordenado AS (
    SELECT
        {grupo}, {taxa},
        ROW_NUMBER() OVER (ORDER BY {taxa} DESC, {grupo}) AS posicao_maior,
        ROW_NUMBER() OVER (ORDER BY {taxa} ASC, {grupo})  AS posicao_menor
    FROM taxas
)
SELECT
    CASE WHEN posicao_maior = 1 THEN 'Maior crescimento' ELSE 'Menor crescimento' END AS posicao,
    {grupo},
    {taxa}
FROM ordenado
WHERE posicao_maior = 1 OR posicao_menor = 1
"""


# Uma linha por UF × porte × grande área com os valores do último mês e a diferença para o mês anterior.
# Δ = valor(M) − valor(M−1) é linear: a soma dos Δ dos grupos filtrados é o Δ do total filtrado.
RESUMO_MES_SQL = f"""
WITH meses AS (
    -- Último mês carregado e o anterior (globais: não dependem dos filtros).
    SELECT
        MAX(mes_referencia) AS mes_atual,
        DATE_FORMAT(ADD_MONTHS(TO_DATE(CONCAT(MAX(mes_referencia), '-01')), -1), 'yyyy-MM') AS mes_anterior
    FROM {PAINEL}
),
por_grupo AS (
    SELECT
        s.uf,
        s.porte_empresa,
        s.grande_area_cnae,
        SUM(IF(s.mes_referencia = m.mes_atual,    s.estoque_final_ativas, 0)) AS ativas_atual,
        SUM(IF(s.mes_referencia = m.mes_anterior, s.estoque_final_ativas, 0)) AS ativas_anterior,
        SUM(IF(s.mes_referencia = m.mes_atual,    s.qtd_aberturas, 0))        AS aberturas_atual,
        SUM(IF(s.mes_referencia = m.mes_anterior, s.qtd_aberturas, 0))        AS aberturas_anterior,
        SUM(IF(s.mes_referencia = m.mes_atual,    s.qtd_baixas, 0))           AS baixas_atual,
        SUM(IF(s.mes_referencia = m.mes_anterior, s.qtd_baixas, 0))           AS baixas_anterior,
        SUM(IF(s.mes_referencia = m.mes_atual,    s.crescimento_liquido, 0))  AS saldo_atual,
        SUM(IF(s.mes_referencia = m.mes_anterior, s.crescimento_liquido, 0))  AS saldo_anterior
    FROM {PAINEL} s
    CROSS JOIN meses m
    WHERE s.mes_referencia IN (m.mes_atual, m.mes_anterior)
    GROUP BY s.uf, s.porte_empresa, s.grande_area_cnae
)
SELECT
    uf,
    porte_empresa,
    grande_area_cnae,
    ativas_atual                        AS ativas_ultimo_mes,
    ativas_atual - ativas_anterior      AS ativas_delta,
    aberturas_atual                     AS aberturas_ultimo_mes,
    aberturas_atual - aberturas_anterior AS aberturas_delta,
    baixas_atual                        AS baixas_ultimo_mes,
    baixas_anterior                     AS baixas_mes_anterior,
    aberturas_anterior                  AS aberturas_mes_anterior,
    baixas_atual - baixas_anterior      AS baixas_delta,
    saldo_atual                         AS saldo_ultimo_mes,
    saldo_atual - saldo_anterior        AS saldo_delta
FROM por_grupo
"""

# Regra única de "aberturas": vêm de fato_empresa_mes / painel (primeira aparição da matriz), nunca da OBT.
DATASETS = {
    # --- página 1: resumo -------------------------------------------------------------------------
    "resumo_mes": ("Último mês e variação contra o mês anterior", RESUMO_MES_SQL),
    # Diz a qual mês os contadores se referem (caixa de texto não mostra valor calculado; tabela mostra).
    "ultimo_mes": ("Mês dos contadores", f"""
SELECT
    MAX(mes_referencia) AS mes_dos_contadores,
    DATE_FORMAT(ADD_MONTHS(TO_DATE(CONCAT(MAX(mes_referencia), '-01')), -1), 'yyyy-MM') AS comparado_com
FROM {PAINEL}
"""),
    # Razão "aberturas por baixa" calculada AQUI, no SQL. O Lakeview avalia errado, em silêncio, expressão composta
    # no campo do widget (SUM(a) / SUM(b) mostrou só SUM(a); try_divide nem desenhou). Uma linha, Brasil: por
    # isso não reage aos filtros da página.
    "aberturas_por_baixa": ("Aberturas por baixa, Brasil (último mês e anterior)", f"""
WITH resumo AS ({RESUMO_MES_SQL})
SELECT
    ROUND(SUM(aberturas_ultimo_mes)   / NULLIF(SUM(baixas_ultimo_mes), 0), 2)   AS aberturas_por_baixa_ultimo_mes,
    ROUND(SUM(aberturas_mes_anterior) / NULLIF(SUM(baixas_mes_anterior), 0), 2) AS aberturas_por_baixa_mes_anterior
FROM resumo
"""),
    # Estoque inclui o baseline: é o ponto de partida da série (só fluxo é que não existe nele).
    "estoque_mes": ("Empresas ativas no fim de cada mês", f"""
SELECT mes_referencia, data_referencia, uf, porte_empresa, grande_area_cnae,
       SUM(estoque_final_ativas) AS estoque_final_ativas
FROM {PAINEL}
GROUP BY ALL
"""),
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
FROM {PAINEL}
WHERE {SEM_CARGA_INICIAL}
GROUP BY ALL
"""),

    # --- páginas 2 e 4: uma linha por empresa × mês × (abertura | baixa) -----------------------------
    # Os percentuais são AVG das colunas pct_* (100 = sim, 0 = não, NULL = não se aplica e fica fora da média).
    # Sem CNPJ nem nome: só os atributos usados pelos filtros e pelos indicadores.
    # Empresa que abre e já aparece BAIXADA (D-17) tem as duas flags no mesmo mês: gera DUAS linhas (uma em
    # 'Aberturas', outra em 'Baixas'), e as colunas de baixa só são preenchidas na linha 'Baixas'.
    # Tabela dash_fluxos_mes = o SQL que este dataset tinha (fato × {Aberturas, Baixas}, sem a carga inicial),
    # gravado pelo Job: mesmas linhas, então AVG, mediana e somas dão o mesmo resultado para qualquer filtro.
    "fluxos_empresa": ("Aberturas e baixas, uma linha por empresa e tipo", f"""
SELECT
    mes_referencia,
    data_referencia,
    uf,
    porte_empresa,
    grande_area_cnae,
    fluxo,
    qtd,
    pct_mei_abertura,
    pct_mei_baixa,
    pct_falencia_baixa,
    idade_baixa_anos,
    pct_baixa_menos_2_anos
FROM {DASH}fluxos_mes
"""),
    # As três abaixo vêm de tabelas por mês; o dataset soma os meses (nenhum widget usa o mês), e soma de somas
    # é a mesma soma. Regras de população (porte de baixa D-17b, motivo pela OBT): ver dashboard_agregados.sql.
    "baixas_idade": ("Baixas por faixa de idade na baixa", f"""
SELECT uf, porte_empresa, grande_area_cnae, faixa_idade_baixa, SUM(qtd_baixas) AS qtd_baixas
FROM {DASH}baixas_idade_mes
GROUP BY ALL
"""),
    "baixas_motivo": ("Baixas de matriz por categoria de motivo", f"""
SELECT uf, porte_empresa, grande_area_cnae, categoria_macro_evento, SUM(qtd_empresas) AS qtd_empresas
FROM {DASH}baixas_motivo_mes
GROUP BY ALL
"""),
    # OBT: eventos de todos os tipos (empresa, sócio, CNAE...). NÃO é fonte de aberturas.
    "eventos": ("Eventos por tipo", f"""
SELECT tipo_evento, uf, porte_empresa, grande_area_cnae, SUM(qtd_eventos) AS qtd_eventos
FROM {DASH}eventos_mes
GROUP BY ALL
"""),

    # --- páginas 2 e 3: taxas por 1.000 ativas (Brasil por grupo, com piso de volume) ------------------
    "taxas_setor": ("Taxas mensais por 1.000 ativas, por grande área (Brasil)",
                    TAXAS_SETOR + "\nSELECT * FROM taxas\n"),
    "extremos_setor": ("Grande área com maior e menor crescimento líquido",
                       _extremos(TAXAS_SETOR, "grande_area_cnae", "taxa_saldo_por_mil")),
    "taxas_uf": ("Taxas mensais por 1.000 ativas, por UF (Brasil)", TAXAS_UF + "\nSELECT * FROM taxas\n"),
    "extremos_uf": ("UF com maior e menor crescimento líquido",
                    _extremos(TAXAS_UF, "uf", "taxa_crescimento_por_mil")),
    "top_cnaes": ("CNAEs com mais aberturas no período (Brasil)", f"""
SELECT
    nome_cnae,
    grande_area_cnae,
    SUM(qtd_aberturas)       AS aberturas,
    SUM(qtd_baixas)          AS baixas,
    SUM(crescimento_liquido) AS saldo
FROM {PAINEL}
WHERE {SEM_CARGA_INICIAL}
GROUP BY ALL
ORDER BY aberturas DESC
LIMIT 20
"""),

    # --- página 5: mercado ativo -------------------------------------------------------------------
    # Os três datasets da página leem tabelas dash_mercado* (dashboard_agregados.sql), todas com o MESMO bairro
    # normalizado, para o filtro de bairro casar. Sem razão social em dash_mercado: MEI costuma ter nome de pessoa
    # física na razão social. dash_mercado_bairros: 20 maiores bairros de cada município, por CNAE (o widget não
    # tem top-N). dash_mercado_empresas: 1.000 maiores por capital social, só onde exibir_razao_social (LGPD).
    "mercado": ("Mercado ativo (sem identificação)", f"""
SELECT uf, municipio, bairro, porte_empresa, grande_area_cnae, nome_cnae, empresas_ativas
FROM {DASH}mercado
"""),
    "mercado_bairros": ("Mercado ativo: 20 maiores bairros por município, por CNAE", f"""
SELECT uf, municipio, bairro, nome_cnae, empresas_ativas
FROM {DASH}mercado_bairros
"""),
    "mercado_empresas": ("Maiores empresas ativas (razão social liberada)", f"""
SELECT razao_social, nome_fantasia, porte_empresa, capital_social, data_inicio_atividade,
       uf, municipio, bairro, grande_area_cnae, nome_cnae
FROM {DASH}mercado_empresas
ORDER BY capital_social DESC
"""),

    # --- página 6: contexto ------------------------------------------------------------------------
    "macro": ("Selic e IPCA no período", f"""
SELECT m.data_referencia, m.selic_anualizada_media_pct, m.ipca_acumulado_12m_pct
FROM tcc_receita.gold.fato_indicadores_macro_mes m
WHERE m.mes_referencia IN (SELECT DISTINCT mes_referencia FROM {FATO})
"""),
}

# Colunas que cada dataset entrega (usado só na validação; conferido também contra o texto do SELECT).
_TAXAS_COLS = ["ativas_inicio_media_mensal", "taxa_abertura_por_mil", "taxa_fechamento_por_mil"]
COLUNAS = {
    "resumo_mes": ["uf", "porte_empresa", "grande_area_cnae", "ativas_ultimo_mes", "ativas_delta",
                   "aberturas_ultimo_mes", "aberturas_delta", "baixas_ultimo_mes", "baixas_mes_anterior",
                   "aberturas_mes_anterior", "baixas_delta", "saldo_ultimo_mes", "saldo_delta"],
    "ultimo_mes": ["mes_dos_contadores", "comparado_com"],
    "aberturas_por_baixa": ["aberturas_por_baixa_ultimo_mes", "aberturas_por_baixa_mes_anterior"],
    "estoque_mes": ["mes_referencia", "data_referencia", "uf", "porte_empresa", "grande_area_cnae",
                    "estoque_final_ativas"],
    "painel": ["mes_referencia", "data_referencia", "uf", "porte_empresa", "grande_area_cnae",
               "estoque_inicial_ativas", "qtd_aberturas", "qtd_baixas", "qtd_falencias", "qtd_inaptidoes",
               "qtd_reativacoes", "qtd_entradas_estoque", "qtd_saidas_estoque", "qtd_baixas_de_ativas",
               "qtd_suspensoes", "crescimento_liquido"],
    "fluxos_empresa": ["mes_referencia", "data_referencia", "uf", "porte_empresa", "grande_area_cnae", "fluxo",
                       "qtd", "pct_mei_abertura", "pct_mei_baixa", "pct_falencia_baixa", "idade_baixa_anos",
                       "pct_baixa_menos_2_anos"],
    "baixas_idade": ["uf", "porte_empresa", "grande_area_cnae", "faixa_idade_baixa", "qtd_baixas"],
    "baixas_motivo": ["uf", "porte_empresa", "grande_area_cnae", "categoria_macro_evento", "qtd_empresas"],
    "eventos": ["tipo_evento", "uf", "porte_empresa", "grande_area_cnae", "qtd_eventos"],
    "taxas_setor": ["grande_area_cnae", "taxa_saldo_por_mil"] + _TAXAS_COLS,
    "extremos_setor": ["posicao", "grande_area_cnae", "taxa_saldo_por_mil"],
    "taxas_uf": ["uf", "taxa_crescimento_por_mil"] + _TAXAS_COLS,
    "extremos_uf": ["posicao", "uf", "taxa_crescimento_por_mil"],
    "top_cnaes": ["nome_cnae", "grande_area_cnae", "aberturas", "baixas", "saldo"],
    "mercado": ["uf", "municipio", "bairro", "porte_empresa", "grande_area_cnae", "nome_cnae",
                "empresas_ativas"],
    "mercado_bairros": ["uf", "municipio", "bairro", "nome_cnae", "empresas_ativas"],
    "mercado_empresas": ["razao_social", "nome_fantasia", "porte_empresa", "capital_social",
                         "data_inicio_atividade", "uf", "municipio", "bairro", "grande_area_cnae", "nome_cnae"],
    "macro": ["data_referencia", "selic_anualizada_media_pct", "ipca_acumulado_12m_pct"],
}


# --- campos -------------------------------------------------------------------------------------

def soma(coluna):
    return {"name": f"sum({coluna})", "expression": f"SUM(`{coluna}`)"}


def media(coluna):
    return {"name": f"avg({coluna})", "expression": f"AVG(`{coluna}`)"}


def mediana(coluna):
    # Mesma forma que a Databricks usa nos dashboards de exemplo exportados: PERCENTILE(`col`, 0.5).
    return {"name": f"median({coluna})", "expression": f"PERCENTILE(`{coluna}`, 0.5)"}


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


def contador(nome, titulo, dataset, campo, descricao=None):
    return {
        "name": nome,
        "queries": [_query(dataset, [campo])],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": campo["name"], "displayName": titulo}},
                 "frame": _frame(titulo, descricao)},
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


def _ordem(valores):
    # Mesma forma de ordem customizada usada nos dashboards de exemplo da Databricks (dbdemos).
    return {"by": "custom-order", "orderedValues": valores}


def barras(nome, titulo, dataset, categoria, valor, rotulos, cor=None, horizontal=False, temporal=False,
           descricao=None, rotulo_cor="Categoria", ordem_cor=None, ordem_categoria=None):
    """rotulos: (rótulo da categoria, rótulo do valor).
    Ordem das categorias: `ordem_categoria` (lista) se dada; senão por valor, maior primeiro (no topo nas
    horizontais, à esquerda nas verticais). `ordem_cor`: ordem das séries de cor (pilha e legenda)."""
    campos = [categoria, valor] + ([cor] if cor else [])
    tipo_cat = "temporal" if temporal else "categorical"
    eixo_cat = {"fieldName": categoria["name"], "scale": {"type": tipo_cat}, "displayName": rotulos[0]}
    eixo_val = {"fieldName": valor["name"], "scale": {"type": "quantitative"}, "displayName": rotulos[1]}
    if not temporal:
        eixo_cat["scale"]["sort"] = (_ordem(ordem_categoria) if ordem_categoria
                                     else {"by": "x-reversed" if horizontal else "y-reversed"})
    encodings = {"x": eixo_val, "y": eixo_cat} if horizontal else {"x": eixo_cat, "y": eixo_val}
    if cor:
        encodings["color"] = {"fieldName": cor["name"], "scale": {"type": "categorical"}, "displayName": rotulo_cor}
        if ordem_cor:
            encodings["color"]["scale"]["sort"] = _ordem(ordem_cor)
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


def abertura(nome, pergunta, como_ler):
    """Texto de abertura de toda página: a pergunta e como ler (só texto fixo, nunca número)."""
    return texto(nome, [f"## {pergunta}", como_ler])


DELTA = "Variação em relação ao mês anterior"

# --- páginas (grade de 12 colunas) --------------------------------------------------------------
# Texto exibido em português direto, para quem nunca viu o projeto: nada de "estoque", "saldo" ou "fluxo" nos
# títulos. "Crescimento líquido" (= passaram a estar ativas − deixaram de estar ativas) é definido onde aparece.

AREA = "Setor (grande área)"
CRESC_MIL = "Crescimento líquido por 1.000 ativas"
DEF_CRESC = ("Crescimento líquido = empresas que passaram a estar ativas (aberturas e reativações) menos as que "
             "deixaram de estar ativas (baixas, inaptidões, suspensões).")
# D-17: quem abre e fecha entre duas publicações mensais já aparece baixada; conta nos dois números.
DEF_ABRE_FECHA = ("Inclui empresas que abriram e foram baixadas antes de aparecer no cadastro mensal: contam como "
                  "abertura e como baixa no mesmo mês.")

# 1. Resumo -------------------------------------------------------------------------------------
P1 = ["resumo_mes", "estoque_mes", "painel"]
PAGINA_RESUMO = [
    (abertura("p1_texto", "O número de empresas ativas está crescendo?",
              "Os números grandes são do **último mês disponível** (indicado ao lado); embaixo de cada um, a variação "
              "em relação ao mês anterior. Empresa = matriz do CNPJ. Cada mudança é contada no mês em que apareceu "
              "pela primeira vez no cadastro mensal da Receita."), pos(0, 0, 12, 3)),
    (filtro("f1_uf", "UF", "uf", P1), pos(0, 3, 3, 2)),
    (filtro("f1_porte", "Porte", "porte_empresa", P1), pos(3, 3, 3, 2)),
    (filtro("f1_area", AREA, "grande_area_cnae", P1), pos(6, 3, 3, 2)),
    (tabela("t1_mes", "Mês dos números", "ultimo_mes",
            [("mes_dos_contadores", "Último mês"), ("comparado_com", "Comparado com")]), pos(9, 3, 3, 2)),

    (contador("c1_ativas", "Empresas ativas no fim do último mês", "resumo_mes", soma("ativas_ultimo_mes")),
     pos(0, 5, 3, 3)),
    (contador("c1_aberturas", "Aberturas no último mês", "resumo_mes", soma("aberturas_ultimo_mes")),
     pos(3, 5, 2, 3)),
    (contador("c1_baixas", "Baixas no último mês", "resumo_mes", soma("baixas_ultimo_mes"), descricao=DEF_ABRE_FECHA),
     pos(5, 5, 2, 3)),
    (contador("c1_saldo", "Crescimento líquido no último mês", "resumo_mes", soma("saldo_ultimo_mes"),
              descricao=DEF_CRESC), pos(7, 5, 2, 3)),
    (contador("c1_ab_por_baixa", "Aberturas por baixa no último mês (Brasil, não muda com os filtros)",
              "aberturas_por_baixa", soma("aberturas_por_baixa_ultimo_mes"),
              descricao="Acima de 1: abrem mais empresas do que fecham."), pos(9, 5, 3, 3)),

    (contador("c1_ativas_d", DELTA, "resumo_mes", soma("ativas_delta"),
              descricao="Sem filtro, é igual ao crescimento líquido do mês. Com filtro pode diferir um pouco: "
                        "empresas que mudam de UF, porte ou setor trocam de grupo."), pos(0, 8, 3, 2)),
    (contador("c1_aberturas_d", DELTA, "resumo_mes", soma("aberturas_delta")), pos(3, 8, 2, 2)),
    (contador("c1_baixas_d", DELTA, "resumo_mes", soma("baixas_delta")), pos(5, 8, 2, 2)),
    (contador("c1_saldo_d", DELTA, "resumo_mes", soma("saldo_delta")), pos(7, 8, 2, 2)),
    (contador("c1_ab_por_baixa_ant", "Aberturas por baixa no mês anterior (Brasil)", "aberturas_por_baixa",
              soma("aberturas_por_baixa_mes_anterior")), pos(9, 8, 3, 2)),

    (linha("l1_estoque", "O número de empresas ativas cresce mês a mês?", "estoque_mes", mes(),
           [(soma("estoque_final_ativas"), "Empresas ativas")],
           descricao="Matrizes ativas no fim de cada mês. Maio/2023 é o ponto de partida (primeira carga)."),
     pos(0, 10, 6, 7)),
    (linha("l1_entradas_saidas", "Mais empresas passam a estar ativas do que deixam de estar?", "painel", mes(), [
        (soma("qtd_entradas_estoque"), "Passaram a estar ativas (aberturas e reativações)"),
        (soma("qtd_saidas_estoque"), "Deixaram de estar ativas (baixas, inaptidões, suspensões)"),
    ], descricao="Por mês em que a mudança apareceu no cadastro. A distância entre as linhas é o crescimento "
                 "líquido do mês."), pos(6, 10, 6, 7)),
]

# 2. Quem ---------------------------------------------------------------------------------------
P2 = ["fluxos_empresa"]
PAGINA_QUEM = [
    (abertura("p2_texto", "Quem abre e quem fecha?",
              "Porte e setor das empresas que abrem e fecham. Os percentuais cobrem todo o período com movimentação "
              "(jun–ago/2023). As taxas são por 1.000 empresas ativas no início do mês e valem para o Brasil "
              "(não mudam com os filtros)."), pos(0, 0, 12, 3)),
    (filtro("f2_uf", "UF", "uf", P2), pos(0, 3, 6, 1)),
    (filtro("f2_area", AREA, "grande_area_cnae", P2), pos(6, 3, 6, 1)),

    (contador("c2_mei_ab", "% das aberturas que são MEI", "fluxos_empresa", media("pct_mei_abertura")),
     pos(0, 4, 3, 3)),
    (contador("c2_mei_bx", "% das baixas que são MEI", "fluxos_empresa", media("pct_mei_baixa"),
              descricao="Para quem estava ativa, vale o porte de antes da baixa (o MEI baixado vira ME na Receita). "
                        "Quem já aparece baixada no primeiro cadastro conta como MEI se as datas de opção e de "
                        "exclusão do MEI mostram que era MEI no dia da baixa."),
     pos(3, 4, 3, 3)),
    (tabela("t2_setor_extremos", "Setor que mais cresce e setor que menos cresce", "extremos_setor", [
        ("posicao", "Posição"), ("grande_area_cnae", "Setor"), ("taxa_saldo_por_mil", f"{CRESC_MIL} (por mês)"),
    ], descricao=f"Brasil; só setores com CNAE conhecido e média ≥ {PISO_ATIVAS} ativas. {DEF_CRESC}"),
     pos(6, 4, 6, 3)),

    (barras("b2_porte", "O MEI domina tanto as aberturas quanto as baixas?", "fluxos_empresa", coluna("fluxo"),
            soma("qtd"), ("", "Empresas"), cor=coluna("porte_empresa"), rotulo_cor="Porte", ordem_cor=ORDEM_PORTE,
            descricao="Aberturas e baixas do período, divididas por porte (o maior porte na base da barra)."),
     pos(0, 7, 6, 7)),
    (barras("b2_saldo_area", "Quais setores crescem mais, em proporção ao seu tamanho?", "taxas_setor",
            coluna("grande_area_cnae"), soma("taxa_saldo_por_mil"), ("Setor", CRESC_MIL), horizontal=True,
            descricao=f"Média mensal, Brasil. {DEF_CRESC}"), pos(6, 7, 6, 7)),
    (barras("b2_taxa_area", "Em quais setores as empresas fecham mais?", "taxas_setor",
            coluna("grande_area_cnae"), soma("taxa_fechamento_por_mil"), ("Setor", "Baixas por 1.000 ativas"),
            horizontal=True, descricao="Baixas de empresas que estavam ativas, para cada 1.000 ativas no início do "
                                       "mês (média mensal). Brasil."), pos(0, 14, 6, 8)),
    (tabela("t2_cnaes", "Quais atividades concentram as aberturas?", "top_cnaes", [
        ("nome_cnae", "Atividade (CNAE principal)"), ("grande_area_cnae", "Setor"),
        ("aberturas", "Aberturas"), ("baixas", "Baixas"), ("saldo", "Crescimento líquido"),
    ], descricao=f"As 20 atividades com mais aberturas no período. Brasil. {DEF_CRESC}"), pos(6, 14, 6, 8)),
]

# 3. Onde ---------------------------------------------------------------------------------------
PAGINA_ONDE = [
    (abertura("p3_texto", "Onde o número de empresas ativas cresce mais?",
              "Comparação por **taxa** — crescimento líquido para cada 1.000 empresas ativas no início do mês, média "
              "mensal — e não por número absoluto (em número absoluto, SP sempre lideraria). Ficam fora UFs com média "
              f"menor que {PISO_ATIVAS} ativas, UF não identificada e exterior. Brasil inteiro: esta página não tem "
              "filtros."), pos(0, 0, 12, 3)),
    (tabela("t3_uf_extremos", "UF que mais cresce e UF que menos cresce", "extremos_uf", [
        ("posicao", "Posição"), ("uf", "UF"), ("taxa_crescimento_por_mil", f"{CRESC_MIL} (por mês)"),
    ], descricao=DEF_CRESC), pos(0, 3, 5, 3)),
    (tabela("t3_uf", "Aberturas, baixas e crescimento líquido por UF, para cada 1.000 ativas", "taxas_uf", [
        ("uf", "UF"), ("taxa_abertura_por_mil", "Aberturas por 1.000 ativas"),
        ("taxa_fechamento_por_mil", "Baixas por 1.000 ativas"), ("taxa_crescimento_por_mil", CRESC_MIL),
        ("ativas_inicio_media_mensal", "Empresas ativas no início do mês (média)"),
    ], descricao="Taxas mensais (média do período)."), pos(0, 6, 5, 10)),
    (barras("b3_saldo_uf", "Em quais UFs o número de empresas ativas cresce mais, em proporção ao tamanho?",
            "taxas_uf", coluna("uf"), soma("taxa_crescimento_por_mil"), ("UF", CRESC_MIL), horizontal=True,
            descricao=f"Média mensal. {DEF_CRESC}"), pos(5, 3, 7, 13)),
]

# 4. Como fecha ---------------------------------------------------------------------------------
P4 = ["fluxos_empresa", "baixas_idade", "baixas_motivo", "painel", "eventos"]
PAGINA_COMO_FECHA = [
    (abertura("p4_texto", "Por que e com que idade as empresas fecham?",
              "Baixas de matrizes no período, pelo **motivo** registrado pela Receita e pela idade da empresa na "
              "baixa (data da baixa − data de início de atividade). Inaptidão e suspensão também tiram a empresa da "
              "lista de ativas, mas podem ser revertidas: a reativação a traz de volta."), pos(0, 0, 12, 3)),
    (filtro("f4_uf", "UF", "uf", P4), pos(0, 3, 4, 1)),
    (filtro("f4_porte", "Porte", "porte_empresa", P4), pos(4, 3, 4, 1)),
    (filtro("f4_area", AREA, "grande_area_cnae", P4), pos(8, 3, 4, 1)),

    (contador("c4_falencia", "% das baixas por falência ou liquidação judicial", "fluxos_empresa",
              media("pct_falencia_baixa"), descricao="Motivos 05, 06 e 10 da tabela de motivos da Receita."),
     pos(0, 4, 3, 3)),
    (contador("c4_idade", "Idade mediana na baixa (anos)", "fluxos_empresa", mediana("idade_baixa_anos"),
              descricao="Metade das empresas baixadas tinha menos que esta idade."), pos(3, 4, 3, 3)),
    (contador("c4_menos_2", "% das baixas com menos de 2 anos de vida", "fluxos_empresa",
              media("pct_baixa_menos_2_anos")), pos(6, 4, 3, 3)),
    (contador("c4_reativacoes", "Reativações no período", "painel", soma("qtd_reativacoes"),
              descricao="Matrizes que voltaram a ser ativas (estavam inaptas, suspensas ou baixadas)."),
     pos(9, 4, 3, 3)),

    (barras("b4_motivo", "Falência é rara: por que as empresas são baixadas?", "baixas_motivo",
            coluna("categoria_macro_evento"), soma("qtd_empresas"), ("Motivo da baixa", "Empresas"),
            horizontal=True, descricao="Motivo agrupado pelo código que a Receita registra na baixa "
                                       "(ver 'Sobre os dados')."), pos(0, 7, 6, 7)),
    (barras("b4_idade", "Com que idade as empresas fecham?", "baixas_idade", coluna("faixa_idade_baixa"),
            soma("qtd_baixas"), ("Idade na baixa", "Baixas"), ordem_categoria=ORDEM_FAIXA_IDADE,
            descricao="Faixas em ordem de idade. 'sem data': falta a data de início ou a da baixa, ou a baixa é "
                      "anterior ao início."), pos(6, 7, 6, 7)),
    (linha("l4_saidas", "Além das baixas: quantas empresas ficam inaptas, são suspensas ou voltam a ser ativas?",
           "painel", mes(), [
               (soma("qtd_baixas"), "Baixas"), (soma("qtd_inaptidoes"), "Inaptidões"),
               (soma("qtd_suspensoes"), "Suspensões"), (soma("qtd_reativacoes"), "Reativações"),
           ], descricao="Matrizes, por mês em que a mudança apareceu no cadastro. Inaptidão: a Receita declara a "
                        "empresa inapta (ex.: deixou de entregar declarações). Suspensão: o CNPJ é suspenso por "
                        "irregularidade cadastral. Reativação: volta a ser ativa."), pos(0, 14, 6, 7)),
    (barras("b4_tipo", "O que mais muda no cadastro, além de abrir e fechar?", "eventos", coluna("tipo_evento"),
            soma("qtd_eventos"), ("Tipo de mudança", "Mudanças registradas"), horizontal=True,
            descricao="Todas as mudanças detectadas entre um mês e o seguinte, inclusive de filiais e de sócios. "
                      "Aberturas não são contadas aqui."), pos(6, 14, 6, 7)),
]

# 5. Mercado ativo ------------------------------------------------------------------------------
# Os quatro filtros valem para todos os widgets: os três datasets têm uf, municipio, bairro (mesma normalização,
# constante BAIRRO) e nome_cnae.
P5 = ["mercado", "mercado_bairros", "mercado_empresas"]
PAGINA_MERCADO = [
    (abertura("p5_texto", "Quem são os concorrentes de uma empresa num lugar?",
              "Matrizes ativas no último cadastro. Escolha UF, município, bairro e atividade (CNAE) para ver o tamanho "
              "do mercado local. Sem identificação de MEI nem de empresário individual (LGPD)."), pos(0, 0, 12, 3)),
    (filtro("f5_uf", "UF", "uf", P5), pos(0, 3, 3, 1)),
    (filtro("f5_municipio", "Município", "municipio", P5), pos(3, 3, 3, 1)),
    (filtro("f5_bairro", "Bairro", "bairro", P5), pos(6, 3, 3, 1)),
    (filtro("f5_cnae", "Atividade (CNAE principal)", "nome_cnae", P5), pos(9, 3, 3, 1)),

    (contador("c5_ativas", "Empresas ativas", "mercado", soma("empresas_ativas")), pos(0, 4, 4, 4)),
    (barras("b5_porte", "Empresas ativas por porte", "mercado", coluna("porte_empresa"), soma("empresas_ativas"),
            ("Porte", "Empresas")), pos(4, 4, 8, 4)),
    (barras("b5_bairro", "Em quais bairros essas empresas estão? (20 maiores bairros do município)",
            "mercado_bairros", coluna("bairro"), soma("empresas_ativas"), ("Bairro", "Empresas"), horizontal=True,
            descricao="Os 20 bairros com mais empresas ativas de cada município (contando todas as atividades). "
                      "Com filtro de atividade, as barras mostram só as empresas daquela atividade nesses bairros."),
     pos(0, 8, 6, 8)),
    (barras("b5_area", "Empresas ativas por setor", "mercado", coluna("grande_area_cnae"), soma("empresas_ativas"),
            ("Setor", "Empresas"), horizontal=True), pos(6, 8, 6, 8)),
    (tabela("t5_empresas", "Maiores empresas ativas (1.000 maiores por capital social)", "mercado_empresas", [
        ("razao_social", "Razão social"), ("nome_fantasia", "Nome fantasia"), ("porte_empresa", "Porte"),
        ("capital_social", "Capital social (R$)"), ("data_inicio_atividade", "Início"),
        ("municipio", "Município"), ("bairro", "Bairro"), ("nome_cnae", "Atividade"),
    ], descricao="Fora MEI, empresário individual e razão social com 11 dígitos seguidos (LGPD). "
                 "Os filtros atuam sobre as 1.000 maiores do Brasil."), pos(0, 16, 12, 9)),
]

# 6. Sobre os dados -----------------------------------------------------------------------------
PAGINA_SOBRE = [
    (texto("p6_texto", [
        "## Como ler estes números?",
        "- **Mês da mudança × data real.** Toda contagem mensal usa o mês em que a mudança apareceu pela primeira "
        "vez no cadastro mensal publicado pela Receita. Esse cadastro é extraído por volta do dia 10, então o mês M "
        "cobre, aproximadamente, de meados de M−1 a meados de M. A **data real** declarada pela Receita só é usada "
        "para calcular idade (ex.: idade na baixa), nunca para contar quantas mudanças houve no mês.",
        "- **Empresa = matriz** do CNPJ (os 8 primeiros dígitos). Mudanças de filiais são registradas, mas não "
        "entram nos números de empresas.",
        "- **Empresas ativas** = matrizes com situação cadastral ATIVA. **Passaram a estar ativas** = aberturas e "
        "reativações. **Deixaram de estar ativas** = baixas, inaptidões, suspensões e anulações de empresas que "
        "estavam ativas. **Crescimento líquido** = a diferença entre os dois; somado às ativas do mês anterior, dá "
        "exatamente as ativas do mês.",
        "- **Abertura e baixa no mesmo mês.** Uma empresa que abre e é baixada entre duas publicações mensais da "
        "Receita já aparece baixada no primeiro cadastro em que surge: ela conta como abertura **e** como baixa "
        "naquele mês. Como nunca esteve ativa num cadastro publicado, não entra no crescimento líquido nem nas "
        "baixas por 1.000 ativas (que contam só empresas ativas no início do mês). O mesmo vale para quem já "
        "aparece inapta ou suspensa.",
        "- **Motivos de baixa** vêm do código registrado pela Receita: falência/liquidação judicial = motivos 05, "
        "06 e 10; óbito do MEI = 75; baixa voluntária = 01, 37, 54; de ofício = 13, 14, 15, 63, 64, 66, 71, 73.",
        "- **Taxas** são por 1.000 empresas ativas no início do mês (média mensal do período). Comparações entre "
        f"UFs e setores só incluem grupos com média de pelo menos {PISO_ATIVAS} ativas.",
        "- **Limites.** Só há três meses com movimentação (jun–ago/2023): maio/2023 é a primeira carga e serve só "
        "de ponto de partida. Setor (grande área) é um agrupamento provisório de CNAE. Nenhum CPF, nome de sócio ou "
        "razão social de MEI/empresário individual aparece no painel (LGPD).",
        "- **Filtros** valem só na própria página. Taxas e comparações por setor/UF são do Brasil e não mudam com "
        "filtros.",
    ]), pos(0, 0, 12, 12)),
    (linha("l6_macro", "Contexto econômico do período: Selic e inflação em 12 meses (% ao ano)", "macro", mes(), [
        (soma("selic_anualizada_media_pct"), "Selic média"), (soma("ipca_acumulado_12m_pct"), "IPCA 12 meses"),
    ], descricao="Só contexto: três meses não permitem tirar conclusão de causa. Fonte: Banco Central."),
     pos(0, 12, 12, 6)),
]

PAGINAS = [
    ("resumo", "1. Resumo", PAGINA_RESUMO),
    ("quem", "2. Quem abre e fecha", PAGINA_QUEM),
    ("onde", "3. Onde", PAGINA_ONDE),
    ("como_fecha", "4. Como fecha", PAGINA_COMO_FECHA),
    ("mercado_ativo", "5. Mercado ativo", PAGINA_MERCADO),
    ("sobre", "6. Sobre os dados", PAGINA_SOBRE),
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
    return {"datasets": datasets, "pages": [_pagina(n, t, w) for n, t, w in PAGINAS]}


# --- validação local ----------------------------------------------------------------------------

# Formas de campo de widget aceitas: coluna pura, mês de uma data, ou UM agregado simples sobre UMA coluna.
# Expressão composta (SUM(a) / SUM(b), try_divide(...), SUM(a) - SUM(b)...) é proibida: na prévia o Lakeview
# mostrou SUM(a) / SUM(b) como se fosse só SUM(a), sem erro. Contas entre agregados vão para o SQL do dataset.
CAMPO_SIMPLES = re.compile(r"`\w+`|(SUM|AVG)\(`\w+`\)|PERCENTILE\(`\w+`, 0\.5\)|DATE_TRUNC\(\"MONTH\", `\w+`\)")
AGREGACOES = ("SUM(", "AVG(", "PERCENTILE(")
# Jargão do projeto que a banca não entende (pedido do autor): não pode aparecer no texto exibido.
JARGAO = r"\b(estoque|saldo|fluxos?|snapshot|baseline|detec[çc][ãa]o)\b|Δ"


def validar(dash: dict) -> None:
    """Falha (SystemExit) com mensagem clara se o JSON tiver referência quebrada, widget sobreposto, página sem
    texto de abertura, contador sem agregação, dataset sem uso ou nome de widget repetido."""
    erros = []
    fontes, colunas = DATASETS, COLUNAS

    # 1. o dicionário COLUNAS tem de bater com o texto do SELECT (cada coluna aparece como palavra inteira)
    for nome, (_, sql) in fontes.items():
        if nome not in colunas:
            erros.append(f"dataset '{nome}' sem entrada em COLUNAS")
            continue
        for c in colunas[nome]:
            if not re.search(rf"\b{c}\b", sql):
                erros.append(f"dataset '{nome}': coluna '{c}' não aparece no SELECT")

    usados, nomes_widgets = set(), []
    for pagina in dash["pages"]:
        retangulos = []
        layout = pagina["layout"]
        # 2. toda página abre com um texto no topo (a pergunta e como ler)
        if not any("multilineTextboxSpec" in i["widget"] and i["position"]["y"] == 0 for i in layout):
            erros.append(f"{pagina['name']}: sem texto de abertura em y = 0")
        for item in layout:
            w, p = item["widget"], item["position"]
            nome = w["name"]
            nomes_widgets.append(nome)
            retangulos.append((nome, p))
            if p["x"] < 0 or p["x"] + p["width"] > LARGURA_GRADE:
                erros.append(f"{pagina['name']}/{nome}: fora da grade de {LARGURA_GRADE} colunas")
            campos_por_query = {}
            for q in w.get("queries", []):
                d = q["query"]["datasetName"]
                usados.add(d)
                if d not in colunas:
                    erros.append(f"{nome}: dataset '{d}' não existe")
                    continue
                campos_por_query[q["name"]] = {f["name"] for f in q["query"]["fields"]}
                for f in q["query"]["fields"]:
                    for col in re.findall(r"`(\w+)`", f["expression"]):
                        if col not in colunas[d]:
                            erros.append(f"{nome}: dataset '{d}' não tem a coluna '{col}'")
            if "queries" in w:
                erros += _validar_encodings(nome, w["spec"]["encodings"], campos_por_query)
                # 3. contador agrega (senão mostraria um valor arbitrário de uma linha)
                consulta = w["queries"][0]["query"]
                if w["spec"]["widgetType"] == "counter" and not consulta["disaggregated"]:
                    for f in consulta["fields"]:
                        if not f["expression"].startswith(AGREGACOES):
                            erros.append(f"{nome}: contador sem agregação ({f['expression']})")

        for i, (n1, a) in enumerate(retangulos):
            for n2, b in retangulos[i + 1:]:
                if (a["x"] < b["x"] + b["width"] and b["x"] < a["x"] + a["width"]
                        and a["y"] < b["y"] + b["height"] and b["y"] < a["y"] + a["height"]):
                    erros.append(f"{pagina['name']}: '{n1}' e '{n2}' se sobrepõem")

    # 3b. só campo simples (ver CAMPO_SIMPLES); nome de campo sem vírgula
    for pagina in dash["pages"]:
        for item in pagina["layout"]:
            for q in item["widget"].get("queries", []):
                for f in q["query"]["fields"]:
                    if not CAMPO_SIMPLES.fullmatch(f["expression"]) or "," in f["name"]:
                        erros.append(f"{item['widget']['name']}: campo composto ou fora do padrão "
                                     f"({f['expression']}); faça a conta no SQL do dataset")

    # 4. sem jargão no texto exibido (títulos, rótulos, descrições, caixas de texto); nomes de coluna não contam
    for pagina in dash["pages"]:
        for texto_exibido in _textos_exibidos(pagina):
            achado = re.search(JARGAO, texto_exibido, flags=re.IGNORECASE)
            if achado:
                erros.append(f"{pagina['name']}: jargão '{achado.group(0)}' em: {texto_exibido[:80]}")

    # 5. nomes de widget únicos no dashboard e nenhum dataset órfão
    repetidos = {n for n in nomes_widgets if nomes_widgets.count(n) > 1}
    if repetidos:
        erros.append(f"nomes de widget repetidos: {sorted(repetidos)}")
    for d in sorted(set(fontes) - usados):
        erros.append(f"dataset '{d}' não é usado por nenhum widget")

    if erros:
        raise SystemExit("❌ Validação do dashboard falhou:\n  - " + "\n  - ".join(erros))
    print("✅ Validação ok: datasets, colunas, filtros, contadores, textos de abertura e grade consistentes.")


def _textos_exibidos(pagina):
    """Todo texto que o leitor vê na página: displayName da página, títulos, descrições, rótulos e textos."""
    yield pagina["displayName"]

    def percorre(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k in ("title", "description", "displayName") and isinstance(v, str):
                    yield v
                elif k == "lines":
                    yield from v
                else:
                    yield from percorre(v)
        elif isinstance(o, list):
            for v in o:
                yield from percorre(v)

    yield from percorre(pagina["layout"])


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
    parser = argparse.ArgumentParser(description="Gera o JSON do dashboard a partir da gold.")
    parser.add_argument("--saida", default=str(SAIDA), help="arquivo de saída (padrão: o JSON versionado)")
    saida = Path(parser.parse_args().saida).resolve()
    dash = montar_dashboard()
    validar(dash)
    saida.write_text(json.dumps(dash, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"✅ {saida} gerado.")


if __name__ == "__main__":
    main()
