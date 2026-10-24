from src.landing.landing_lib import executar_dq_landing_cli
from src.landing.configs.empresas import CONFIG_LANDING

if __name__ == "__main__":
    executar_dq_landing_cli(CONFIG_LANDING)