from src.landing.landing_lib import executar_extract_landing_cli
from src.landing.configs.cnaes_ibge import CONFIG_LANDING

if __name__ == "__main__":
    executar_extract_landing_cli(CONFIG_LANDING)