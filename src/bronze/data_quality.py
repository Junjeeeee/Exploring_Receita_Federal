from typing import Dict, Optional

# Definição de contratos (adicione as colunas essenciais esperadas para cada tabela)
CONTRATOS_SCHEMA = {
    "empresas": ["cnpj_basico", "razao_social", "natureza_juridica", "capital_social"],
    "estabelecimentos": ["cnpj_basico", "cnpj_ordem", "cnpj_dv", "situacao_cadastral", "cnae_fiscal_principal"],
    "socios": ["cnpj_basico", "identificador_socio", "nome_socio_razao_social"],
    "motivos_situacao": ["codigo", "descricao"],
    "cnaes_ibge": ["codigo", "descricao", "secao", "nome_secao", "divisao", "nome_divisao", "grupo", "nome_grupo", "classe", "nome_classe"]
}

LOG_VOLUMETRIA = "tcc_receita.bronze.dq_metrics_log"
# Quantos meses ANTERIORES entram na média da volumetria.
MESES_MEDIA_VOLUMETRIA = 3


def validar_schema(df, nome_tabela_uc):
    """Garante que as colunas críticas não desapareceram do snapshot da Receita."""
    if nome_tabela_uc not in CONTRATOS_SCHEMA:
        return # Pula tabelas sem contrato definido

    colunas_atuais = set(df.columns)
    colunas_esperadas = set(CONTRATOS_SCHEMA[nome_tabela_uc])
    faltantes = colunas_esperadas - colunas_atuais

    if faltantes:
        raise ValueError(f"🚨 FALHA NO CONTRATO DE SCHEMA ({nome_tabela_uc}): Colunas ausentes -> {faltantes}")

    print(f"  ✓ Schema validado para {nome_tabela_uc}.")


# --- Volumetria ---------------------------------------------------------------------------------------------
# Antes (validar_volumetria): por mês, um count(), um CREATE TABLE IF NOT EXISTS, uma consulta da média no log e
# um INSERT. Agora (P3 de docs/specs/features/2026-11-16-otimizacao-bronze.md) a contagem vem da agregação única
# de `testar_tabela_bronze`, o histórico é lido uma vez e os meses aprovados nesta execução entram nele em memória,
# na mesma ordem em que antes eram gravados (o INSERT continua mês a mês, no mesmo ponto).


def ler_historico_volumetria(spark, nome_tabela_uc) -> Dict[Optional[str], Optional[int]]:
    """Última contagem registrada por mês para a tabela ({mes_referencia: contagem}).

    O log é append-only (várias tasks escrevem em paralelo): se um (tabela, mês) foi verificado mais de uma vez,
    vale a linha mais recente por verificado_em (mesma regra da consulta antiga).
    """
    spark.sql(f"CREATE TABLE IF NOT EXISTS {LOG_VOLUMETRIA} (tabela STRING, mes_referencia STRING, contagem BIGINT, verificado_em TIMESTAMP)")
    linhas = spark.sql(f"""
        SELECT mes_referencia, contagem FROM (
            SELECT mes_referencia, contagem,
                   ROW_NUMBER() OVER (PARTITION BY mes_referencia ORDER BY verificado_em DESC) AS rn
            FROM {LOG_VOLUMETRIA}
            WHERE tabela = '{nome_tabela_uc}'
        ) WHERE rn = 1
    """).collect()
    return {linha["mes_referencia"]: linha["contagem"] for linha in linhas}


def media_volumetria_anterior(historico: Dict[Optional[str], Optional[int]], mes_referencia: str,
                              qtd_meses: int = MESES_MEDIA_VOLUMETRIA) -> float:
    """Média das contagens dos até `qtd_meses` meses ANTERIORES a `mes_referencia`; 0 sem histórico.

    Equivale à consulta antiga (`mes_referencia < '<mês>'`, `ORDER BY mes_referencia DESC LIMIT 3`, `AVG`):
    compara só com meses anteriores, porque a base cresce com o tempo e incluir meses futuros (backfill fora de
    ordem) faria meses antigos parecerem quedas. Comparação de texto 'YYYY-MM', como no SQL; o AVG ignora NULL.
    """
    anteriores = sorted((m for m in historico if m is not None and m < mes_referencia), reverse=True)[:qtd_meses]
    valores = [historico[m] for m in anteriores if historico[m] is not None]
    # Log ainda vazio (primeira execução): sem histórico para comparar.
    return sum(valores) / len(valores) if valores else 0


def checar_volumetria(contagem_atual, media_historica, nome_tabela_uc, mes_referencia,
                      threshold_queda=0.15, forcar_aprovacao=False):
    """Mês vazio falha; queda maior que `threshold_queda` contra a média histórica falha (mesmas mensagens de antes)."""
    print(f"  📊 Volumetria atual: {contagem_atual:,} linhas.")

    if contagem_atual == 0:
        raise ValueError(f"🚨 FALHA: O arquivo de {nome_tabela_uc} no mês {mes_referencia} está vazio.")

    if media_historica > 0:
        limite_minimo = media_historica * (1 - threshold_queda)
        if contagem_atual < limite_minimo:
            mensagem = (f"🚨 ANOMALIA VOLUMÉTRICA em {nome_tabela_uc}!\n"
                        f"Contagem ({contagem_atual:,}) caiu mais de {threshold_queda*100}% "
                        f"em relação à média histórica ({int(media_historica):,}).")

            if forcar_aprovacao:
                print(f"  ⚠️ {mensagem}")
                print("  🔓 OVERRIDE ATIVADO: A ignorar a anomalia e a forçar a ingestão.")
            else:
                raise ValueError(mensagem)
        else:
            print(f"  ✓ Volumetria dentro do padrão (Média: {int(media_historica):,}).")


def registrar_volumetria(spark, nome_tabela_uc, mes_referencia, contagem_atual):
    spark.sql(f"INSERT INTO {LOG_VOLUMETRIA} VALUES ('{nome_tabela_uc}', '{mes_referencia}', {contagem_atual}, current_timestamp())")
