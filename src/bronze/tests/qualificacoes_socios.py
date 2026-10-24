from src.bronze.bronze_lib import executar_ingestao_cli
from src.bronze.configs.qualificacoes_socios import CONFIG

if __name__ == "__main__":
    executar_ingestao_dq_cli(CONFIG)