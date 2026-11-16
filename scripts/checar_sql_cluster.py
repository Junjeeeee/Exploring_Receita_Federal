"""Checa se os SQL do repositório rodam no cluster do Job (DBR 15.4 LTS / Spark 3.5, cluster clássico).

O SQL warehouse (usado na amostra) roda uma versão mais nova do Databricks SQL: uma construção pode passar
na amostra e quebrar no Job. Este script procura, fora de comentários e de strings, construções que já
quebraram na base real ou que a documentação dá como indisponíveis no DBR 15.4. Cada regra cita a fonte.

    python scripts/checar_sql_cluster.py                 # varre src/**/*.sql
    python scripts/checar_sql_cluster.py arquivo.sql ... # só os arquivos indicados

Sai com código 1 e imprime `arquivo:linha: [regra] mensagem` para cada ocorrência.
Exceção por linha (uso consciente, com motivo): `-- compat: ok <motivo>` na mesma linha.
"""

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

MARCADOR_EXCECAO = re.compile(r"--\s*compat:\s*ok\s+\S", re.I)


@dataclass(frozen=True)
class Regra:
    nome: str
    padrao: re.Pattern
    mensagem: str
    so_no_job: bool = False  # True: vale só para arquivos que o Job executa (lista de src/jobs/gerar_job.py)


REGRAS = [
    # Fonte: falhou na base real (commit f032773, "TRY_TO_DATE não existe no Spark 3.5 do cluster do Job");
    # try_to_date só entrou no Spark 4.0 / DBR 16+. Ver src/jobs/README.md (linha spark_version).
    Regra(
        "try_to_date",
        re.compile(r"\bTRY_TO_DATE\s*\(", re.I),
        "TRY_TO_DATE não existe no Spark 3.5; use CAST(TRY_TO_TIMESTAMP(x, 'fmt') AS DATE).",
    ),
    # Fonte: falhou na base real em 2026-11-14 (commit 59efdc1, gold/dashboard_mercado_ativo.sql). Documentação
    # Databricks, "Use liquid clustering for tables": liquid clustering não é compatível com ZORDER; tabela
    # gerenciada pode receber clustering automático (predictive optimization) sem o código pedir.
    Regra(
        "zorder",
        re.compile(r"\bZORDER\s+BY\b", re.I),
        "OPTIMIZE ... ZORDER BY falha em tabela com liquid clustering (inclusive o automático); use OPTIMIZE simples.",
    ),
    # Fonte: sintaxe de pipe SQL (`|>`) entrou no Apache Spark 4.0 (SPARK-49555) e no Databricks Runtime 16.2;
    # o Spark 3.5 do DBR 15.4 não a reconhece.
    Regra(
        "pipe_sql",
        re.compile(r"\|>"),
        "Sintaxe de pipe SQL (|>) só existe a partir do Spark 4.0 / DBR 16.2.",
    ),
    # Fonte: SQL scripting (instrução composta BEGIN ... END) é do Databricks Runtime 16.3+ / Spark 4.0
    # (documentação Databricks, "SQL scripting" e "compound statement").
    Regra(
        "sql_scripting",
        re.compile(r"(?:\A|;)\s*(BEGIN)\b", re.I),
        "Bloco BEGIN ... END (SQL scripting) só existe a partir do DBR 16.3 / Spark 4.0.",
    ),
    # Fonte: src/jobs/run_sql.py só substitui {{mes_referencia_*}} e chama spark.sql(statement) sem `args`;
    # um marcador nomeado (:param_x, usado pelos datasets do dashboard) fica sem valor e o Spark recusa
    # (classe de erro UNBOUND_SQL_PARAMETER). Só vale para os arquivos que o Job executa.
    Regra(
        "parametro_dashboard",
        re.compile(r"(?<![:\w]):[A-Za-z_]\w*"),
        "Marcador de parâmetro (:nome) não recebe valor no Job (run_sql.py só substitui {{...}}).",
        so_no_job=True,
    ),
]


def mascarar(sql: str) -> str:
    """Troca por espaço o conteúdo de comentários (-- e /* */) e de strings/identificadores entre aspas
    ('...', "...", `...`), mantendo as quebras de linha: as posições e os números de linha não mudam."""
    saida = list(sql)
    i, n = 0, len(sql)

    def apagar(ini: int, fim: int) -> None:
        for k in range(ini, fim):
            if saida[k] != "\n":
                saida[k] = " "

    while i < n:
        c = sql[i]
        prox = sql[i + 1] if i + 1 < n else ""
        if c == "-" and prox == "-":
            fim = sql.find("\n", i)
            fim = n if fim == -1 else fim
            apagar(i, fim)
            i = fim
        elif c == "/" and prox == "*":
            fim = sql.find("*/", i + 2)
            fim = n if fim == -1 else fim + 2
            apagar(i, fim)
            i = fim
        elif c in ("'", '"', "`"):
            j = i + 1
            while j < n and sql[j] != c:
                j += 2 if (sql[j] == "\\" and c != "`") else 1  # escape estilo Spark: \'
            fim = min(j + 1, n)
            apagar(i + 1, fim - 1)  # mantém as aspas: '' continua sendo um literal
            i = fim
        else:
            i += 1
    return "".join(saida)


def arquivos_do_job() -> set[Path]:
    from src.jobs.gerar_job import SQL_DQ, SQL_INGEST

    return {(RAIZ / a).resolve() for _, a in SQL_INGEST} | {(RAIZ / a).resolve() for _, a, _ in SQL_DQ}


def checar_texto(sql: str, no_job: bool = True) -> list[tuple[int, str, str]]:
    """Devolve (linha, regra, mensagem) para cada ocorrência não liberada por `-- compat: ok <motivo>`."""
    mascarado = mascarar(sql)
    linhas_originais = sql.splitlines()
    achados = []
    for regra in REGRAS:
        if regra.so_no_job and not no_job:
            continue
        for m in regra.padrao.finditer(mascarado):
            # Com grupo, a posição útil é a do grupo (ex.: BEGIN, cujo match começa no ';' anterior).
            inicio = m.start(1) if m.re.groups else m.start()
            linha = mascarado.count("\n", 0, inicio) + 1
            texto_linha = linhas_originais[linha - 1] if linha <= len(linhas_originais) else ""
            if MARCADOR_EXCECAO.search(texto_linha):
                continue
            achados.append((linha, regra.nome, regra.mensagem))
    return sorted(achados)


def checar_arquivos(caminhos: list[Path]) -> list[str]:
    job = arquivos_do_job()
    erros = []
    for caminho in sorted(caminhos):
        sql = caminho.read_text(encoding="utf-8")
        for linha, nome, mensagem in checar_texto(sql, no_job=caminho.resolve() in job):
            try:
                rotulo = caminho.resolve().relative_to(RAIZ)
            except ValueError:
                rotulo = caminho
            erros.append(f"{rotulo}:{linha}: [{nome}] {mensagem}")
    return erros


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("arquivos", nargs="*", help="Arquivos .sql (padrão: src/**/*.sql).")
    args = parser.parse_args(argv)
    caminhos = [Path(a) for a in args.arquivos] or sorted((RAIZ / "src").rglob("*.sql"))
    erros = checar_arquivos(caminhos)
    for erro in erros:
        print(erro)
    if erros:
        print(f"\n❌ {len(erros)} construção(ões) incompatível(is) com o cluster do Job (DBR 15.4 / Spark 3.5).")
        return 1
    print(f"✅ {len(caminhos)} arquivo(s) SQL compatíveis com o cluster do Job.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
