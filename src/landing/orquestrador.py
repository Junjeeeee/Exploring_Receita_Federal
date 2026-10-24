import argparse
from src.landing.landing_lib import extrair_tabela_landing, testar_tabela_landing

# Importação de todas as configurações (Sem a CNAE da Receita, usando apenas CNAES_IBGE)
from src.landing.configs.empresas import CONFIG_LANDING as CONFIG_LANDING_EMPRESAS
from src.landing.configs.estabelecimentos import CONFIG_LANDING as CONFIG_LANDING_ESTABELECIMENTOS
from src.landing.configs.dados_simples import CONFIG_LANDING as CONFIG_LANDING_DADOS_SIMPLES
from src.landing.configs.socios import CONFIG_LANDING as CONFIG_LANDING_SOCIOS
from src.landing.configs.cnaes_ibge import CONFIG_LANDING as CONFIG_LANDING_CNAES_IBGE
from src.landing.configs.ceps import CONFIG_LANDING as CONFIG_LANDING_CEPS
from src.landing.configs.paises import CONFIG_LANDING as CONFIG_LANDING_PAISES
from src.landing.configs.municipios import CONFIG_LANDING as CONFIG_LANDING_MUNICIPIOS
from src.landing.configs.qualificacoes_socios import CONFIG_LANDING as CONFIG_LANDING_QUALIFICACOES_SOCIOS
from src.landing.configs.naturezas_juridicas import CONFIG_LANDING as CONFIG_LANDING_NATUREZAS_JURIDICAS
from src.landing.configs.motivos_situacao import CONFIG_LANDING as CONFIG_LANDING_MOTIVOS_SITUACAO  

TODAS_CONFIGS = {
    "empresas": CONFIG_LANDING_EMPRESAS,
    "estabelecimentos": CONFIG_LANDING_ESTABELECIMENTOS,
    "dados_simples": CONFIG_LANDING_DADOS_SIMPLES,
    "socios": CONFIG_LANDING_SOCIOS,
    "cnaes_ibge": CONFIG_LANDING_CNAES_IBGE,
    "ceps": CONFIG_LANDING_CEPS,
    "paises": CONFIG_LANDING_PAISES,
    "municipios": CONFIG_LANDING_MUNICIPIOS,
    "qualificacoes_socios": CONFIG_LANDING_QUALIFICACOES_SOCIOS,
    "naturezas_juridicas": CONFIG_LANDING_NATUREZAS_JURIDICAS,
    "motivos_situacao": CONFIG_LANDING_MOTIVOS_SITUACAO
}

def orquestrar(tabelas_alvo: str, mes_ini: str, mes_fim: str, force: bool):
    """
    Executa a extração e o teste de qualidade sequencialmente.
    Se falhar na extração, o teste não roda. Se falhar no teste, a pipeline aborta.
    """
    if tabelas_alvo.strip().upper() == "ALL":
        lista_tabelas = list(TODAS_CONFIGS.keys())
    else:
        lista_tabelas = [t.strip().lower() for t in tabelas_alvo.split(",")]

    print(f"\n🎯 INICIANDO ORQUESTRAÇÃO LANDING")
    print(f"📅 Período: {mes_ini or 'Automático'} a {mes_fim or 'Automático'}")
    print(f"📋 Tabelas na fila: {lista_tabelas}\n")

    for nome_tab in lista_tabelas:
        if nome_tab not in TODAS_CONFIGS:
            print(f"⚠️ AVISO: Tabela '{nome_tab}' desconhecida. Ignorando...")
            continue
        
        config = TODAS_CONFIGS[nome_tab]
        
        print("="*60)
        print(f"🚀 PROCESSANDO: {config.nome_tabela.upper()}")
        print("="*60)
        
        try:
            # 1. Extração (Download + Parquet + Upload S3)
            print(f"\n---> ETAPA 1: Extração (Origem: {config.origem})")
            extrair_tabela_landing(config, mes_ini, mes_fim, force=force)
            
            # 2. Data Quality (Teste Rápido Lendo Footer do S3)
            print(f"\n---> ETAPA 2: Testes de Qualidade (DQ S3)")
            testar_tabela_landing(config, mes_ini, mes_fim)
            
            print(f"\n✅ SUCESSO: Pipeline concluída para {config.nome_tabela.upper()}!\n")
            
        except Exception as e:
            print(f"\n❌ ERRO FATAL: Falha ao processar {config.nome_tabela.upper()}.")
            print(f"Detalhes do erro: {str(e)}")
            print("Abortando orquestração para evitar inconsistência de dados.")
            raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestrador da Camada Landing (Extração + Testes)")
    parser.add_argument(
        "--tabela", 
        type=str, 
        default="ALL", 
        help="Nome da tabela (ex: 'empresas'), lista separada por vírgula (ex: 'ceps,cnaes_ibge') ou 'ALL' para todas."
    )
    parser.add_argument("--mes_referencia_inicial", type=str, default="", help="Mês inicial (YYYY-MM). Vazio = Mês-1")
    parser.add_argument("--mes_referencia_final", type=str, default="", help="Mês final (YYYY-MM). Vazio = Mês-1")
    parser.add_argument("--force", action="store_true", help="Força o reprocessamento ignorando arquivos já existentes no S3")
    
    args = parser.parse_args()
    
    orquestrar(
        tabelas_alvo=args.tabela,
        mes_ini=args.mes_referencia_inicial,
        mes_fim=args.mes_referencia_final,
        force=args.force
    )