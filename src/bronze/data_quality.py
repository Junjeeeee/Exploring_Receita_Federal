from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Definição de contratos (adicione as colunas essenciais esperadas para cada tabela)
CONTRATOS_SCHEMA = {
    "empresas": ["cnpj_basico", "razao_social", "natureza_juridica", "capital_social"],
    "estabelecimentos": ["cnpj_basico", "cnpj_ordem", "cnpj_dv", "situacao_cadastral", "cnae_fiscal_principal"],
    "socios": ["cnpj_basico", "identificador_socio", "nome_socio_razao_social"]
    # ... adicione as restantes conforme a sua documentação
}

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

def validar_volumetria(spark, df, nome_tabela_uc, mes_referencia, threshold_queda=0.15, forcar_aprovacao=False):
    contagem_atual = df.count()
    print(f"  📊 Volumetria atual: {contagem_atual:,} linhas.")
    
    if contagem_atual == 0:
        raise ValueError(f"🚨 FALHA: O arquivo de {nome_tabela_uc} no mês {mes_referencia} está vazio.")

    log_table = "tcc_receita.bronze.dq_metrics_log"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {log_table} (tabela STRING, mes_referencia STRING, contagem BIGINT, verificado_em TIMESTAMP)")
    
    try:
        media_df = spark.sql(f"SELECT AVG(contagem) as media FROM {log_table} WHERE tabela = '{nome_tabela_uc}' AND mes_referencia != '{mes_referencia}'").collect()[0]
        media_historica = media_df['media'] if media_df['media'] is not None else 0
        
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
                
    except Exception as e:
        if "ANOMALIA" in str(e):
            raise e
        print("  ⚠️ Falha ao ler histórico temporal.")

    spark.sql(f"DELETE FROM {log_table} WHERE tabela = '{nome_tabela_uc}' AND mes_referencia = '{mes_referencia}'")
    spark.sql(f"INSERT INTO {log_table} VALUES ('{nome_tabela_uc}', '{mes_referencia}', {contagem_atual}, current_timestamp())")