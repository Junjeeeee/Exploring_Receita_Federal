"""Controle do optimized write na escrita das tabelas mensais da bronze (P2 de
docs/specs/features/2026-11-16-otimizacao-bronze.md).

O cluster do Job liga `spark.databricks.delta.optimizeWrite.enabled` para todas as tasks (gerar_job.py). Na
bronze isso soma um shuffle completo antes de cada escrita mensal (B17), repetindo a redistribuição que o
`dropDuplicates` já fez. Desligar só muda como as linhas se distribuem em arquivos, não quais linhas são gravadas.

Modos (parâmetro `modo_optimize_write` de `ingerir_tabela_bronze` ou variável de ambiente abaixo):
- "nao"    (padrão): não mexe em nada; vale a conf do cluster. Comportamento anterior.
- "writer": opção `optimizeWrite=false` só no DataFrameWriter da escrita mensal. Documentada no Delta Lake
            open source (>= 3.1, maior precedência: writer > sessão > tabela); a documentação do Databricks não a
            menciona, então no DBR 15.4 pode ser ignorada em silêncio (sem erro, sem ganho). A medir.
- "sessao": `spark.conf.set(<conf>, "false")` só durante a escrita, restaurando o valor anterior em seguida.
            Documentada pelo Databricks ("false ... Can be set at the session level to override optimized writes
            for all tables modified in the workload"); não está documentado se a sessão de uma
            `spark_python_task` é isolada das outras tasks do mesmo job cluster.

Sem dependência de Spark, para poder ser testado localmente (tests/test_bronze_otimizacao.py).
"""

from typing import Optional

CONF_SESSAO = "spark.databricks.delta.optimizeWrite.enabled"
VARIAVEL_AMBIENTE = "BRONZE_DESLIGAR_OPTIMIZE_WRITE"
MODO_PADRAO = "nao"
MODOS = ("nao", "writer", "sessao")


def resolver_modo_optimize_write(valor: Optional[str]) -> str:
    """Normaliza o modo pedido; vazio/None = padrão (não mexe). Valor desconhecido derruba a task."""
    modo = (valor or "").strip().lower() or MODO_PADRAO
    if modo not in MODOS:
        raise ValueError(f"{VARIAVEL_AMBIENTE}/modo_optimize_write inválido: {valor!r}. Use um de {MODOS}.")
    return modo
