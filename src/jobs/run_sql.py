"""Executa arquivos .sql do repositório no cluster do Job (sem SQL warehouse).

Uso (task `spark_python_task` do Databricks, com o repo como `git_source`):
    src/jobs/run_sql.py --arquivos src/silver_intermediate/ingest/empresas.sql \
        --mes_referencia_inicial 2023-05 --mes_referencia_final 2023-08

- Substitui `{{mes_referencia_inicial}}` e `{{mes_referencia_final}}` pelos valores informados.
- Divide o arquivo em statements por `;`, ignorando `;` dentro de strings e comentários.
- Executa cada statement com `spark.sql`. SELECTs (ex.: DQ com ASSERT_TRUE) são materializados
  com `.collect()` para que as asserções realmente rodem.
- Vários arquivos em uma task = uma única sessão Spark (economiza subida de cluster).
"""

import argparse
import re
import sys
import time
from pathlib import Path

RAIZ_REPO = Path(__file__).resolve().parents[2]

PARAMETROS = ("mes_referencia_inicial", "mes_referencia_final")


def dividir_statements(sql: str) -> list[str]:
    """Divide SQL por ';' fora de strings ('...', "...", `...`) e comentários (-- e /* */)."""
    statements, atual = [], []
    i, n = 0, len(sql)
    aspas = None  # caractere de abertura da string atual
    while i < n:
        c = sql[i]
        prox = sql[i + 1] if i + 1 < n else ""
        if aspas:
            atual.append(c)
            if c == "\\" and aspas != "`" and prox:
                atual.append(prox)  # escape estilo Spark: \'
                i += 2
                continue
            if c == aspas:
                aspas = None
        elif c == "-" and prox == "-":
            fim = sql.find("\n", i)
            fim = n if fim == -1 else fim
            atual.append(sql[i:fim])
            i = fim
            continue
        elif c == "/" and prox == "*":
            fim = sql.find("*/", i + 2)
            fim = n if fim == -1 else fim + 2
            atual.append(sql[i:fim])
            i = fim
            continue
        elif c in ("'", '"', "`"):
            aspas = c
            atual.append(c)
        elif c == ";":
            statements.append("".join(atual))
            atual = []
        else:
            atual.append(c)
        i += 1
    statements.append("".join(atual))
    return [s.strip() for s in statements if _tem_codigo(s)]


def _tem_codigo(statement: str) -> bool:
    sem_comentarios = re.sub(r"--[^\n]*|/\*.*?\*/", "", statement, flags=re.S)
    return bool(sem_comentarios.strip())


def substituir_parametros(sql: str, valores: dict[str, str]) -> str:
    for nome, valor in valores.items():
        if valor and not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", valor):
            raise ValueError(f"{nome} deve estar no formato YYYY-MM (recebido: {valor!r}).")
        sql = sql.replace("{{" + nome + "}}", valor)
    restantes = sorted(set(re.findall(r"\{\{\s*(\w+)\s*\}\}", sql)))
    if restantes:
        raise ValueError(f"Parâmetros sem valor no SQL: {restantes}")
    return sql


def _eh_consulta(statement: str) -> bool:
    sem_comentarios = re.sub(r"--[^\n]*|/\*.*?\*/", "", statement, flags=re.S).lstrip().upper()
    # "WITH ... INSERT INTO" é DML (já executa no spark.sql); só consultas puras precisam de collect.
    return sem_comentarios.startswith(("SELECT", "WITH")) and not re.search(r"\b(INSERT|MERGE)\b", sem_comentarios)


def executar_arquivo(spark, caminho: Path, valores: dict[str, str]) -> None:
    sql = substituir_parametros(caminho.read_text(encoding="utf-8"), valores)
    statements = dividir_statements(sql)
    print(f"\n📄 {caminho.relative_to(RAIZ_REPO)} — {len(statements)} statement(s)")
    for i, statement in enumerate(statements, 1):
        primeira_linha = next(
            (l.strip() for l in statement.splitlines() if l.strip() and not l.strip().startswith("--")), ""
        )
        print(f"  [{i}/{len(statements)}] {primeira_linha[:100]}")
        inicio = time.time()
        df = spark.sql(statement)
        if _eh_consulta(statement):
            df.collect()  # força a execução de ASSERT_TRUE em DQs
        print(f"      ✓ {time.time() - inicio:.1f}s")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Executa arquivos SQL do repo via spark.sql.")
    parser.add_argument("--arquivos", required=True,
                        help="Caminhos relativos à raiz do repo, separados por vírgula (executados em ordem).")
    for nome in PARAMETROS:
        parser.add_argument(f"--{nome}", default="", help="YYYY-MM")
    args, _ = parser.parse_known_args(argv)

    valores = {nome: getattr(args, nome).strip() for nome in PARAMETROS}
    arquivos = [RAIZ_REPO / a.strip() for a in args.arquivos.split(",") if a.strip()]
    faltando = [str(a) for a in arquivos if not a.is_file()]
    if faltando:
        raise FileNotFoundError(f"Arquivos SQL não encontrados: {faltando}")

    from pyspark.sql import SparkSession  # import tardio: permite testar o parser sem Spark

    spark = SparkSession.builder.getOrCreate()
    for caminho in arquivos:
        executar_arquivo(spark, caminho, valores)
    print("\n✅ Todos os arquivos executados.")


if __name__ == "__main__":
    sys.exit(main())
