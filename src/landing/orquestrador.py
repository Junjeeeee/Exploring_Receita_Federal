import argparse

from src.landing.landing_lib import (
    extrair_tabela_landing,
    testar_tabela_landing,
    gerar_range_meses,
)

# Configurações das tabelas da Landing
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
    "motivos_situacao": CONFIG_LANDING_MOTIVOS_SITUACAO,
}


def _processar_tabela(config, mes_ini, mes_fim, force):
    print("=" * 60)
    print(f"🚀 PROCESSANDO: {config.nome_tabela.upper()}")
    print("=" * 60)
    print(f"\n---> ETAPA 1: Extração (Origem: {config.origem})")
    extrair_tabela_landing(config, mes_ini, mes_fim, force=force)
    print("\n---> ETAPA 2: Testes de Qualidade (DQ S3)")
    testar_tabela_landing(config, mes_ini, mes_fim)
    print(f"\n✅ SUCESSO: Pipeline concluída para {config.nome_tabela.upper()}!\n")


def orquestrar(tabelas_alvo: str, mes_ini: str, mes_fim: str, force: bool):
    if tabelas_alvo.strip().upper() == "ALL":
        nomes_tabelas = list(TODAS_CONFIGS)
    else:
        # Remove espaços, entradas vazias e duplicatas preservando a ordem.
        nomes_tabelas = list(dict.fromkeys(
            nome.strip().lower()
            for nome in tabelas_alvo.split(",")
            if nome.strip()
        ))

    desconhecidas = [nome for nome in nomes_tabelas if nome not in TODAS_CONFIGS]
    if desconhecidas:
        raise ValueError(f"Tabelas desconhecidas: {desconhecidas}. Válidas: {list(TODAS_CONFIGS)}")
    if not nomes_tabelas:
        raise ValueError("Informe ao menos uma tabela ou use --tabela ALL.")

    configs_mensais = [TODAS_CONFIGS[n] for n in nomes_tabelas if TODAS_CONFIGS[n].is_snapshot_mensal]
    configs_estaticas = [TODAS_CONFIGS[n] for n in nomes_tabelas if not TODAS_CONFIGS[n].is_snapshot_mensal]

    print("\n🎯 INICIANDO ORQUESTRAÇÃO LANDING (MÊS A MÊS)")
    print(f"📅 Período: {mes_ini or 'Automático'} a {mes_fim or 'Automático'}")
    print(f"📋 Tabelas mensais: {[c.nome_tabela for c in configs_mensais]}")
    print(f"📋 Tabelas estáticas: {[c.nome_tabela for c in configs_estaticas]}\n")

    # Estratégia month-major: conclui todas as tabelas mensais de um mês
    # (extração + DQ) antes de avançar para o mês seguinte.
    if configs_mensais:
        meses = gerar_range_meses(mes_ini, mes_fim)
        for mes in meses:
            print("\n" + "#" * 72)
            print(f"📆 PROCESSANDO MÊS {mes}")
            print("#" * 72 + "\n")
            for config in configs_mensais:
                try:
                    _processar_tabela(config, mes, mes, force)
                except Exception as exc:
                    print(f"\n❌ ERRO FATAL: {config.nome_tabela.upper()} no mês {mes}.")
                    print(f"Detalhes: {exc}")
                    print("Orquestração interrompida para evitar inconsistência.")
                    raise

    # Tabelas estáticas não possuem partição mensal; são executadas uma única vez.
    for config in configs_estaticas:
        try:
            _processar_tabela(config, mes_ini, mes_fim, force)
        except Exception as exc:
            print(f"\n❌ ERRO FATAL: {config.nome_tabela.upper()} (estática).")
            print(f"Detalhes: {exc}")
            print("Orquestração interrompida para evitar inconsistência.")
            raise

    print("\n✅ ORQUESTRAÇÃO LANDING FINALIZADA COM SUCESSO!\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Orquestrador Landing: extração e DQ, agrupando as tabelas por mês."
    )
    parser.add_argument(
        "--tabela",
        type=str,
        default="ALL",
        help="Tabela, lista separada por vírgula ou ALL.",
    )
    parser.add_argument("--mes_referencia_inicial", type=str, default="", help="Mês inicial (YYYY-MM).")
    parser.add_argument("--mes_referencia_final", type=str, default="", help="Mês final (YYYY-MM).")
    parser.add_argument("--force", action="store_true", help="Reextrai e sobrescreve objetos existentes no S3.")
    args = parser.parse_args()

    orquestrar(
        tabelas_alvo=args.tabela,
        mes_ini=args.mes_referencia_inicial,
        mes_fim=args.mes_referencia_final,
        force=args.force,
    )
