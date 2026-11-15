"""Publica no workspace uma PRÉVIA do dashboard apontando para a amostra (schemas `amostra_*`).

Cria (ou atualiza, com --id) um dashboard separado; o dashboard oficial não é tocado.

    export DATABRICKS_WAREHOUSE_ID=<id>
    python scripts/amostra/publicar_dashboard_amostra.py            # cria
    python scripts/amostra/publicar_dashboard_amostra.py --id <id>  # atualiza o mesmo
    python -m src.gold.dashboard.gerar_dashboard --saida /tmp/outro.lvdash.json
    python scripts/amostra/publicar_dashboard_amostra.py --id <id> --arquivo /tmp/outro.lvdash.json
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ))

from scripts.amostra.rodar_no_warehouse import para_amostra  # noqa: E402

NOME = "TCC Receita — Dinâmica empresarial (PRÉVIA, amostra 1%)"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", default=os.getenv("DATABRICKS_WAREHOUSE_ID", ""))
    parser.add_argument("--id", default="", help="dashboard_id de uma prévia já criada (atualiza em vez de criar)")
    parser.add_argument("--arquivo", default=str(RAIZ / "src/gold/dashboard/tcc_receita.lvdash.json"),
                        help="JSON a publicar (padrão: o JSON versionado)")
    args = parser.parse_args()
    if not args.warehouse:
        parser.error("informe --warehouse ou a variável DATABRICKS_WAREHOUSE_ID")

    dashboard = json.loads(Path(args.arquivo).read_text(encoding="utf-8"))
    for dataset in dashboard["datasets"]:
        dataset["queryLines"] = [para_amostra(linha) for linha in dataset["queryLines"]]

    corpo = {"display_name": NOME, "warehouse_id": args.warehouse,
             "serialized_dashboard": json.dumps(dashboard, ensure_ascii=False)}
    if args.id:
        comando = ["databricks", "api", "patch", f"/api/2.0/lakeview/dashboards/{args.id}"]
    else:
        comando = ["databricks", "api", "post", "/api/2.0/lakeview/dashboards"]
    saida = subprocess.run(comando + ["--json", json.dumps(corpo)], capture_output=True, text=True)
    if saida.returncode != 0:
        sys.exit(saida.stderr.strip() or saida.stdout.strip())
    resposta = json.loads(saida.stdout)
    print("dashboard_id:", resposta.get("dashboard_id"))
    print("caminho:", resposta.get("path"))


if __name__ == "__main__":
    main()
