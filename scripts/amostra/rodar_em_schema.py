"""Roda SQL do pipeline sobre a amostra, gravando as SAÍDAS num schema de teste separado.

Por que existe: `rodar_no_warehouse.py` grava por cima das tabelas `amostra_*`. Para comparar uma versão nova de
um SQL com a referência sem destruir a amostra (e sem disputar tabela com quem mais estiver testando), este script
redireciona:

- `tcc_receita.bronze.X`                         -> `tcc_receita.amostra_bronze.X` (só leitura);
- as quatro SCD2 de `silver_intermediate`        -> `tcc_receita.<--schema-scd2>.X` (só leitura; padrão
  `amostra_silver_intermediate`);
- todo o resto (log, `silver_final.*`, `gold.*`) -> `tcc_receita.<--schema-saida>.X`.

    python scripts/amostra/rodar_em_schema.py --schema-saida amostra_f3 \\
        --arquivos src/silver_final/ingest/dim_cnae.sql,src/gold/fato_empresa_mes.sql \\
        --mes_referencia_inicial 2023-05 --mes_referencia_final 2023-08

`--etapas sf,gold` usa a mesma lista e ordem do Job (gerar_job.py); `si` aqui roda SÓ o log (as SCD2 não são
recalculadas). Reaproveita a divisão em statements, a troca de parâmetros e a chamada ao warehouse do runner da
amostra (importa, não copia).
"""

import argparse
import os
import re
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ))

from scripts.amostra.rodar_no_warehouse import executar  # noqa: E402
from src.jobs.gerar_job import SQL_DQ, SQL_INGEST  # noqa: E402
from src.jobs.run_sql import dividir_statements, substituir_parametros  # noqa: E402

SCD2 = ("empresas", "estabelecimentos", "dados_simples", "socios")


def redirecionar(sql: str, schema_saida: str, schema_scd2: str) -> str:
    """Troca os nomes de tabela conforme a regra do cabeçalho."""
    sql = re.sub(r"\btcc_receita\.bronze\.", "tcc_receita.amostra_bronze.", sql)
    sql = re.sub(rf"\btcc_receita\.silver_intermediate\.({'|'.join(SCD2)})\b",
                 rf"tcc_receita.{schema_scd2}.\1", sql)
    # Tabelas (camada.tabela) e também o nome do schema sozinho (CREATE SCHEMA IF NOT EXISTS ...).
    return re.sub(r"\btcc_receita\.(silver_intermediate|silver_final|gold)\b", f"tcc_receita.{schema_saida}", sql)


def rodar_arquivo(warehouse: str, caminho: Path, valores: dict, schema_saida: str, schema_scd2: str) -> bool:
    sql = redirecionar(substituir_parametros(caminho.read_text(encoding="utf-8"), valores), schema_saida, schema_scd2)
    statements = dividir_statements(sql)
    print(f"\n▶ {caminho.relative_to(RAIZ)} ({len(statements)} statement(s))", flush=True)
    for i, statement in enumerate(statements, start=1):
        inicio = time.time()
        ok, erro = executar(warehouse, statement)
        print(f"   [{i}/{len(statements)}] {'ok' if ok else 'ERRO'} {time.time() - inicio:.0f}s", flush=True)
        if not ok:
            print(f"   {erro[:3000]}", flush=True)
            return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Roda SQL sobre a amostra gravando num schema de teste.")
    parser.add_argument("--warehouse", default=os.getenv("DATABRICKS_WAREHOUSE_ID", ""))
    parser.add_argument("--schema-saida", required=True, help="schema (em tcc_receita) que recebe as saídas")
    parser.add_argument("--schema-scd2", default="amostra_silver_intermediate", help="schema das quatro SCD2")
    parser.add_argument("--etapas", default="", help="si (só o log), sf, gold, na ordem do Job")
    parser.add_argument("--arquivos", default="", help="arquivos .sql, separados por vírgula (rodam antes)")
    parser.add_argument("--sem-dq", action="store_true")
    parser.add_argument("--mes_referencia_inicial", default="2023-05")
    parser.add_argument("--mes_referencia_final", default="2023-08")
    args = parser.parse_args()
    if not args.warehouse:
        parser.error("informe --warehouse ou a variável DATABRICKS_WAREHOUSE_ID")
    if args.schema_saida.startswith(("bronze", "silver", "gold")) or args.schema_saida in (
            "amostra_bronze", "amostra_silver_intermediate", "amostra_silver_final", "amostra_gold"):
        parser.error("o schema de saída tem de ser um schema de teste, nunca um schema do pipeline")

    valores = {"mes_referencia_inicial": args.mes_referencia_inicial,
               "mes_referencia_final": args.mes_referencia_final}
    arquivos = [RAIZ / a.strip() for a in args.arquivos.split(",") if a.strip()]
    for etapa in [e.strip() for e in args.etapas.split(",") if e.strip()]:
        ingest = [c for p, c in SQL_INGEST if p == etapa]
        dq = [c for p, c, _ in SQL_DQ if p == etapa] if not args.sem_dq else []
        if etapa == "si":  # nunca recalcula as SCD2: só o log e o DQ dele
            ingest = [c for c in ingest if "log_eventos_negocio" in c]
            dq = [c for c in dq if "log_eventos_negocio" in c]
        arquivos += [RAIZ / c for c in ingest + dq]

    for arquivo in arquivos:
        if not rodar_arquivo(args.warehouse, arquivo, valores, args.schema_saida, args.schema_scd2):
            print(f"\n❌ Falhou: {arquivo.relative_to(RAIZ)}")
            sys.exit(1)
    print("\n✅ Tudo executado sem erro.")


if __name__ == "__main__":
    main()
