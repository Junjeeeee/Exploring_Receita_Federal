"""Gera `tcc_receita_pipeline.job.yml` com uma task por tarefa (ingestão e DQ separados).

    python -m src.jobs.gerar_job            # escreve o YAML
    python -m src.jobs.gerar_job --checar   # falha se o YAML commitado estiver desatualizado (CI)

As dependências entre tasks são extraídas dos próprios SQL: cada arquivo declara o que escreve
(CREATE/MERGE/INSERT) e o que lê (`tcc_receita.<schema>.<tabela>`). Uma task depende do DQ da
tabela que lê (ou da própria task produtora, se a tabela não tiver DQ). Spec:
docs/specs/features/job-databricks.md.
"""

import argparse
import re
import sys
from pathlib import Path

import yaml

RAIZ = Path(__file__).resolve().parents[2]
SAIDA = RAIZ / "src/jobs/tcc_receita_pipeline.job.yml"

BRONZE_ESTATICAS = [
    "cnaes_ibge", "paises", "municipios", "naturezas_juridicas",
    "qualificacoes_socios", "motivos_situacao", "ceps",
]
BRONZE_MENSAIS = ["empresas", "estabelecimentos", "socios", "dados_simples"]

# Arquivos SQL de transformação, na ordem em que aparecem no YAML (a execução segue o DAG).
SQL_INGEST = [
    ("si", "src/silver_intermediate/ingest/empresas.sql"),
    ("si", "src/silver_intermediate/ingest/estabelecimentos.sql"),
    ("si", "src/silver_intermediate/ingest/dados_simples.sql"),
    ("si", "src/silver_intermediate/ingest/socios.sql"),
    ("si", "src/silver_intermediate/ingest/log_eventos_negocio.sql"),
    ("sf", "src/silver_final/ingest/dim_cnae.sql"),
    ("sf", "src/silver_final/ingest/dim_grupo_cnae.sql"),
    ("sf", "src/silver_final/ingest/ponte_cnae.sql"),
    ("sf", "src/silver_final/ingest/dim_tempo.sql"),
    ("sf", "src/silver_final/ingest/dim_mudanca_estado.sql"),
    ("sf", "src/silver_final/ingest/dim_empresa.sql"),
    ("sf", "src/silver_final/ingest/fato_mudanca_estado.sql"),
    ("gold", "src/gold/fato_indicadores_macro_mes.sql"),
    ("gold", "src/gold/obt_dashboard_eventos.sql"),
    ("gold", "src/gold/dashboard_mercado_ativo.sql"),
    ("gold", "src/gold/d_coorte_sobrevivencia.sql"),
    ("gold", "src/gold/fato_empresa_mes.sql"),
    ("gold", "src/gold/ds_visao_macro.sql"),
    ("gold", "src/gold/fato_indicadores_setoriais_mes.sql"),
]
# DQ SQL: (prefixo, arquivo, tabela validada; None = DQ de várias tabelas, roda após todas que lê).
SQL_DQ = [
    ("si", "src/silver_intermediate/tests/dq_empresas.sql", "silver_intermediate.empresas"),
    ("si", "src/silver_intermediate/tests/dq_estabelecimentos.sql", "silver_intermediate.estabelecimentos"),
    ("si", "src/silver_intermediate/tests/dq_dados_simples.sql", "silver_intermediate.dados_simples"),
    ("si", "src/silver_intermediate/tests/dq_socios.sql", "silver_intermediate.socios"),
    ("si", "src/silver_intermediate/tests/dq_log_eventos_negocio.sql", "silver_intermediate.log_eventos_negocio"),
    ("sf", "src/silver_final/tests/dq_dim_cnae.sql", "silver_final.dim_cnae"),
    ("sf", "src/silver_final/tests/dq_dim_grupo_cnae.sql", "silver_final.dim_grupo_cnae"),
    ("sf", "src/silver_final/tests/dq_ponte_cnae.sql", "silver_final.ponte_cnae"),
    ("sf", "src/silver_final/tests/dq_dim_mucanca_estado.sql", "silver_final.dim_mudanca_estado"),
    ("sf", "src/silver_final/tests/dq_dim_empresa.sql", "silver_final.dim_empresa"),
    ("sf", "src/silver_final/tests/dq_fato_mudanca_estado.sql", "silver_final.fato_mudanca_estado"),
    ("gold", "src/gold/dq/dq_gold.sql", None),
]

PARAMS = [
    "--mes_referencia_inicial", "{{job.parameters.mes_referencia_inicial}}",
    "--mes_referencia_final", "{{job.parameters.mes_referencia_final}}",
]

CLUSTER = {
    "job_cluster_key": "cluster_pipeline",
    "new_cluster": {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "r5d.xlarge",
        "driver_node_type_id": "m5d.xlarge",
        "autoscale": {"min_workers": 1, "max_workers": 4},
        "aws_attributes": {
            "availability": "SPOT_WITH_FALLBACK",
            "first_on_demand": 1,
            "spot_bid_price_percent": 100,
            "zone_id": "auto",
        },
        "data_security_mode": "SINGLE_USER",
        "spark_conf": {
            "spark.sql.shuffle.partitions": "auto",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            # Muitas tasks em paralelo no mesmo cluster: evita que uma tabela grande monopolize.
            "spark.scheduler.mode": "FAIR",
        },
        "spark_env_vars": {"AWS_BUCKET_NAME": "lakehouse-receita"},
        # Logs do driver + event log do Spark (stages, shuffle, spill) sobrevivem ao fim do cluster.
        # Volume criado uma vez: tcc_receita.ops.cluster_logs (ver README).
        "cluster_log_conf": {"volumes": {"destination": "/Volumes/tcc_receita/ops/cluster_logs"}},
        "custom_tags": {"projeto": "tcc_receita"},
    },
}

RE_ESCRITA = re.compile(
    r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)(?:\s+IF\s+NOT\s+EXISTS)?|MERGE\s+INTO|"
    r"INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?)\s+tcc_receita\.(\w+\.\w+)",
    re.I,
)
RE_REFERENCIA = re.compile(r"tcc_receita\.(\w+\.\w+)", re.I)


def _sem_comentarios(sql: str) -> str:
    return re.sub(r"--[^\n]*|/\*.*?\*/", "", sql, flags=re.S)


def _nome_task(prefixo: str, arquivo: str) -> str:
    return f"{prefixo}_{Path(arquivo).stem}"


def _task(chave, script, args, deps):
    t = {
        "task_key": chave,
        "job_cluster_key": "cluster_pipeline",
        "spark_python_task": {"python_file": script, "source": "GIT", "parameters": args + PARAMS},
    }
    if deps:
        t["depends_on"] = [{"task_key": d} for d in sorted(set(deps))]
    return t


def montar_tasks() -> list[dict]:
    tasks = []
    # tabela (schema.nome) -> task que a deixa "pronta" (DQ se houver, senão a produtora)
    pronta: dict[str, str] = {}
    produtora: dict[str, str] = {}

    for tabela in BRONZE_ESTATICAS + BRONZE_MENSAIS:
        ingest, dq = f"bronze_{tabela}", f"bronze_dq_{tabela}"
        args = ["--modo", "ingestao", "--tabelas", tabela]
        tasks.append(_task(ingest, "src/jobs/run_bronze.py", args, []))
        tasks.append(_task(dq, "src/jobs/run_bronze.py", ["--modo", "dq", "--tabelas", tabela], [ingest]))
        produtora[f"bronze.{tabela}"] = ingest
        pronta[f"bronze.{tabela}"] = dq

    leituras: dict[str, set[str]] = {}
    for prefixo, arquivo in SQL_INGEST:
        sql = _sem_comentarios((RAIZ / arquivo).read_text(encoding="utf-8"))
        escritas = {m.lower() for m in RE_ESCRITA.findall(sql)}
        if not escritas:
            raise ValueError(f"{arquivo}: não escreve nenhuma tabela tcc_receita.*")
        chave = _nome_task(prefixo, arquivo)
        for tabela in escritas:
            if tabela in produtora:
                raise ValueError(f"{tabela} escrita por {produtora[tabela]} e {chave}")
            produtora[tabela] = chave
            pronta[tabela] = chave
        leituras[chave] = {m.lower() for m in RE_REFERENCIA.findall(sql)} - escritas

    dq_tasks = []
    for prefixo, arquivo, tabela in SQL_DQ:
        chave = _nome_task(prefixo, arquivo)
        sql = _sem_comentarios((RAIZ / arquivo).read_text(encoding="utf-8"))
        lidas = {m.lower() for m in RE_REFERENCIA.findall(sql)}
        dq_tasks.append((chave, arquivo, lidas))
        if tabela is not None:
            pronta[tabela] = chave

    def deps_de(lidas: set[str], origem: str) -> list[str]:
        faltando = [t for t in lidas if t not in pronta]
        if faltando:
            raise ValueError(f"{origem} lê tabelas sem task produtora: {sorted(faltando)}")
        return [pronta[t] for t in lidas]

    for prefixo, arquivo in SQL_INGEST:
        chave = _nome_task(prefixo, arquivo)
        args = ["--arquivos", arquivo]
        tasks.append(_task(chave, "src/jobs/run_sql.py", args, deps_de(leituras[chave], arquivo)))

    for chave, arquivo, lidas in dq_tasks:
        # DQ depende das produtoras do que lê (não de si mesmo via `pronta`).
        deps = [produtora[t] for t in lidas if t in produtora]
        if len(deps) != len(lidas):
            raise ValueError(f"{arquivo} lê tabelas sem task produtora: {sorted(lidas - produtora.keys())}")
        tasks.append(_task(chave, "src/jobs/run_sql.py", ["--arquivos", arquivo], deps))

    return tasks


def montar_job() -> dict:
    job = {
        "name": "tcc_receita_pipeline",
        "max_concurrent_runs": 1,
        "timeout_seconds": 14400,
        "parameters": [
            {"name": "mes_referencia_inicial", "default": "2023-05"},
            {"name": "mes_referencia_final", "default": "2023-08"},
        ],
        "git_source": {
            "git_url": "https://github.com/Junjeeeee/Exploring_Receita_Federal",
            "git_provider": "gitHub",
            "git_branch": "ceps_ibge_and_dashboard_v2",
        },
        "job_clusters": [CLUSTER],
        "tasks": montar_tasks(),
    }
    return {"resources": {"jobs": {"tcc_receita_pipeline": job}}}


class _Dumper(yaml.SafeDumper):
    pass


def _str(dumper, valor):
    # Aspas onde o YAML poderia reinterpretar o valor (datas, versões, booleanos, templates).
    precisa = "{{" in valor or valor in ("auto", "true", "false", "FAIR") or valor[:1].isdigit()
    return dumper.represent_scalar("tag:yaml.org,2002:str", valor, style='"' if precisa else None)


_Dumper.add_representer(str, _str)

CABECALHO = """\
# GERADO por src/jobs/gerar_job.py — não editar à mão (rode `python -m src.jobs.gerar_job`).
# Formato da UI (Workflows → Job → ⋮ → Edit as YAML) / Asset Bundles (resources.jobs.<chave>).
# Campos a ajustar no workspace: ver src/jobs/README.md.
"""


def gerar_yaml() -> str:
    return CABECALHO + yaml.dump(montar_job(), Dumper=_Dumper, sort_keys=False, allow_unicode=True, width=200)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--checar", action="store_true", help="Só verifica se o YAML está atualizado.")
    args = parser.parse_args(argv)
    conteudo = gerar_yaml()
    if args.checar:
        atual = SAIDA.read_text(encoding="utf-8") if SAIDA.exists() else ""
        if atual != conteudo:
            print(f"{SAIDA.relative_to(RAIZ)} desatualizado: rode `python -m src.jobs.gerar_job`.")
            return 1
        print("YAML do Job atualizado.")
        return 0
    SAIDA.write_text(conteudo, encoding="utf-8")
    n = len(montar_job()["resources"]["jobs"]["tcc_receita_pipeline"]["tasks"])
    print(f"✅ {SAIDA.relative_to(RAIZ)} gerado com {n} tasks.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
