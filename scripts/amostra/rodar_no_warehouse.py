"""Roda os SQL do pipeline num SQL warehouse, sobre a AMOSTRA (schemas `amostra_*`).

Serve para validar silver/gold de ponta a ponta em minutos e por centavos, sem subir o cluster do Job
nem tocar nas tabelas reais. Os mesmos arquivos .sql do Job são usados; a única diferença é que
`tcc_receita.<camada>.` vira `tcc_receita.amostra_<camada>.` antes de executar.

Pré-requisitos: CLI `databricks` autenticado (perfil padrão) e um SQL warehouse.

    export DATABRICKS_WAREHOUSE_ID=<id>            # ou --warehouse <id>
    python scripts/amostra/rodar_no_warehouse.py --criar-bronze          # 1ª vez: copia 1% da bronze
    python scripts/amostra/rodar_no_warehouse.py --etapas si,sf,gold     # ingestão + DQ, na ordem do Job
    python scripts/amostra/rodar_no_warehouse.py --arquivos src/gold/fato_empresa_mes.sql
    python scripts/amostra/rodar_no_warehouse.py --etapas gold --sem-dq
    # destino/origem em schemas paralelos (validação lado a lado, sem tocar nas tabelas amostra_*):
    python scripts/amostra/rodar_no_warehouse.py --arquivos src/silver_intermediate/ingest/empresas.sql \
        --schemas silver_intermediate=amostra_f2,bronze=amostra_f2_bt

Para o primeiro erro e mostra a mensagem do Databricks. Não imprime linhas de dados, só o status.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ))

from src.jobs.gerar_job import SQL_DQ, SQL_INGEST  # noqa: E402  (mesma lista e ordem do Job)
from src.jobs.run_sql import dividir_statements, substituir_parametros  # noqa: E402

CAMADAS = r"\btcc_receita\.(bronze|silver_intermediate|silver_final|gold)\b"


def para_amostra(sql: str, schemas: dict[str, str] | None = None) -> str:
    """tcc_receita.gold.x -> tcc_receita.amostra_gold.x (vale para as quatro camadas).

    `schemas` troca o destino de uma camada (ex.: {"silver_intermediate": "amostra_f2"}); as camadas que não
    aparecem nele continuam indo para `amostra_<camada>`."""
    schemas = schemas or {}
    return re.sub(CAMADAS, lambda m: f"tcc_receita.{schemas.get(m.group(1), 'amostra_' + m.group(1))}", sql)


def ler_schemas(texto: str) -> dict[str, str]:
    """'silver_intermediate=amostra_f2,bronze=amostra_f2_bt' -> dict. Só aceita as quatro camadas e nomes amostra_*,
    para que nenhuma troca aponte para um schema real."""
    schemas = {}
    for par in [p.strip() for p in texto.split(",") if p.strip()]:
        camada, _, destino = par.partition("=")
        camada, destino = camada.strip(), destino.strip()
        if camada not in ("bronze", "silver_intermediate", "silver_final", "gold"):
            raise ValueError(f"camada desconhecida em --schemas: {camada!r}")
        if not re.fullmatch(r"amostra_\w+", destino):
            raise ValueError(f"--schemas só aceita destinos amostra_* (recebido: {destino!r})")
        schemas[camada] = destino
    return schemas


def _api(metodo: str, caminho: str, corpo: dict | None = None) -> dict:
    comando = ["databricks", "api", metodo, caminho]
    if corpo is not None:
        comando += ["--json", json.dumps(corpo)]
    saida = subprocess.run(comando, capture_output=True, text=True)
    if saida.returncode != 0:
        raise RuntimeError(saida.stderr.strip() or saida.stdout.strip())
    return json.loads(saida.stdout) if saida.stdout.strip() else {}


def executar(warehouse: str, statement: str) -> tuple[bool, str]:
    """Executa um statement e espera terminar. Devolve (ok, mensagem de erro)."""
    resposta = _api("post", "/api/2.0/sql/statements", {
        "warehouse_id": warehouse, "statement": statement,
        "wait_timeout": "50s", "on_wait_timeout": "CONTINUE", "row_limit": 1,
    })
    while resposta["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(5)
        resposta = _api("get", f"/api/2.0/sql/statements/{resposta['statement_id']}")
    status = resposta["status"]
    if status["state"] == "SUCCEEDED":
        return True, ""
    return False, status.get("error", {}).get("message", status["state"])


def rodar_arquivo(warehouse: str, caminho: Path, valores: dict[str, str], trocar_schema: bool = True,
                  schemas: dict[str, str] | None = None) -> bool:
    sql = substituir_parametros(caminho.read_text(encoding="utf-8"), valores)
    if trocar_schema:
        sql = para_amostra(sql, schemas)
    statements = dividir_statements(sql)
    rotulo = caminho.relative_to(RAIZ) if caminho.is_relative_to(RAIZ) else caminho  # aceita cópias fora do repo
    print(f"\n▶ {rotulo} ({len(statements)} statement(s))", flush=True)
    for i, statement in enumerate(statements, start=1):
        inicio = time.time()
        ok, erro = executar(warehouse, statement)
        print(f"   [{i}/{len(statements)}] {'ok' if ok else 'ERRO'} {time.time() - inicio:.0f}s", flush=True)
        if not ok:
            print(f"   {erro[:3000]}", flush=True)
            return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Roda os SQL do pipeline sobre a amostra, num SQL warehouse.")
    parser.add_argument("--warehouse", default=os.getenv("DATABRICKS_WAREHOUSE_ID", ""))
    parser.add_argument("--criar-bronze", action="store_true", help="(Re)cria tcc_receita.amostra_bronze.")
    parser.add_argument("--etapas", default="", help="si,sf,gold (qualquer subconjunto, na ordem do Job).")
    parser.add_argument("--arquivos", default="", help="Arquivos .sql específicos, separados por vírgula.")
    parser.add_argument("--sem-dq", action="store_true", help="Não roda os DQ das etapas escolhidas.")
    parser.add_argument("--continuar", action="store_true", help="Não para no primeiro arquivo com erro.")
    parser.add_argument("--schemas", default="",
                        help="Troca o schema de uma camada: 'silver_intermediate=amostra_f2,bronze=amostra_f2_bt'. "
                             "Padrão: amostra_<camada>.")
    parser.add_argument("--mes_referencia_inicial", default="2023-05")
    parser.add_argument("--mes_referencia_final", default="2023-08")
    args = parser.parse_args()
    if not args.warehouse:
        parser.error("informe --warehouse ou a variável DATABRICKS_WAREHOUSE_ID")

    schemas = ler_schemas(args.schemas)
    valores = {"mes_referencia_inicial": args.mes_referencia_inicial,
               "mes_referencia_final": args.mes_referencia_final}
    falhas = []

    if args.criar_bronze:
        if not rodar_arquivo(args.warehouse, RAIZ / "scripts/amostra/criar_amostra_bronze.sql", valores,
                             trocar_schema=False):
            sys.exit(1)

    arquivos = [(RAIZ / a.strip()).resolve() for a in args.arquivos.split(",") if a.strip()]
    for etapa in [e.strip() for e in args.etapas.split(",") if e.strip()]:
        arquivos += [RAIZ / caminho for prefixo, caminho in SQL_INGEST if prefixo == etapa]
        if not args.sem_dq:
            arquivos += [RAIZ / caminho for prefixo, caminho, _ in SQL_DQ if prefixo == etapa]

    for arquivo in arquivos:
        if not rodar_arquivo(args.warehouse, arquivo, valores, schemas=schemas):
            falhas.append(str(arquivo))
            if not args.continuar:
                break

    if falhas:
        print(f"\n❌ Falhou: {falhas}")
        sys.exit(1)
    print("\n✅ Tudo executado sem erro.")


if __name__ == "__main__":
    main()
