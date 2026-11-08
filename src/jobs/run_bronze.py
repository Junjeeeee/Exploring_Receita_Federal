"""Ingestão + DQ da Bronze para várias tabelas numa única task do Job.

Uso (task `spark_python_task`, repo como `git_source`):
    src/jobs/run_bronze.py --tabelas empresas,estabelecimentos \
        --mes_referencia_inicial 2023-05 --mes_referencia_final 2023-08 [--sem_dq]

Para cada tabela: ingere (landing S3 → tcc_receita.bronze.<t>) e roda o DQ em seguida.
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
    parser.add_argument("--sem_dq", action="store_true", help="Pula o DQ (não recomendado).")
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
        ingerir_tabela_bronze(spark, config, mes_ini, mes_fim)
        if not args.sem_dq:
            testar_tabela_bronze(spark, config, mes_ini, mes_fim)
    print(f"\n✅ Bronze concluída: {tabelas}")


if __name__ == "__main__":
    sys.exit(main())
