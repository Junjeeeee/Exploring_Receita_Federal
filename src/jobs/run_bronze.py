"""Ingestão e/ou DQ da Bronze (task `spark_python_task`, repo como `git_source`).

Uso no Job (uma task de ingestão e uma de DQ por tabela):
    src/jobs/run_bronze.py --modo ingestao --tabelas empresas \
        --mes_referencia_inicial 2023-05 --mes_referencia_final 2023-08
    src/jobs/run_bronze.py --modo dq --tabelas empresas --mes_referencia_inicial ... 

Uso local/manual: --modo ambos com várias tabelas (lista, MENSAIS, ESTATICAS ou ALL).
Tabelas estáticas ignoram a janela de meses. Requer AWS_BUCKET_NAME no ambiente do cluster.
"""

import argparse
import importlib
import sys
from pathlib import Path

RAIZ_REPO = Path(__file__).resolve().parents[2]
if str(RAIZ_REPO) not in sys.path:
    # Scripts de git_source rodam com o diretório do script no path, não a raiz do repo.
    sys.path.insert(0, str(RAIZ_REPO))

MENSAIS = ["empresas", "estabelecimentos", "socios", "dados_simples"]
ESTATICAS = [
    "cnaes_ibge", "paises", "municipios", "naturezas_juridicas",
    "qualificacoes_socios", "motivos_situacao", "ceps",
]


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Ingestão + DQ Bronze em lote.")
    parser.add_argument("--tabelas", required=True,
                        help="Lista separada por vírgula, MENSAIS, ESTATICAS ou ALL.")
    parser.add_argument("--mes_referencia_inicial", default="")
    parser.add_argument("--mes_referencia_final", default="")
    parser.add_argument("--modo", choices=["ingestao", "dq", "ambos"], default="ambos")
    args, _ = parser.parse_known_args(argv)

    atalhos = {"MENSAIS": MENSAIS, "ESTATICAS": ESTATICAS, "ALL": ESTATICAS + MENSAIS}
    pedido = args.tabelas.strip()
    tabelas = atalhos.get(pedido.upper()) or [t.strip() for t in pedido.split(",") if t.strip()]

    from pyspark.sql import SparkSession
    from src.bronze.bronze_lib import ingerir_tabela_bronze, testar_tabela_bronze

    spark = SparkSession.builder.getOrCreate()
    mes_ini = args.mes_referencia_inicial.strip() or None
    mes_fim = args.mes_referencia_final.strip() or None

    for tabela in tabelas:
        config = importlib.import_module(f"src.bronze.configs.{tabela}").CONFIG
        if args.modo in ("ingestao", "ambos"):
            ingerir_tabela_bronze(spark, config, mes_ini, mes_fim)
        if args.modo in ("dq", "ambos"):
            testar_tabela_bronze(spark, config, mes_ini, mes_fim)
    print(f"\n✅ Bronze ({args.modo}) concluída: {tabelas}")


if __name__ == "__main__":
    sys.exit(main())
