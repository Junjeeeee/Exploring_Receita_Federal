import os
import time
import argparse
from cnpj_pipeline.extract.extractor import (
    executar_pipeline_local_para_nuvem,
    verificar_disponibilidade_mes,
    obter_proximo_mes,
    STATE_FILE
)

def executar_backfill(local_test=False, mes_inicio=None, mes_fim=None):
    print("🚀 A iniciar rotina de Backfill Histórico...")
    
    # 1. Define o mês de arranque (prioridade para o argumento manual)
    if mes_inicio:
        mes_atual = mes_inicio
        print(f"📌 Início forçado a partir do mês: {mes_atual}")
    else:
        ultimo_mes = None
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                ultimo_mes = f.read().strip()
        mes_atual = obter_proximo_mes(ultimo_mes)
    
    while True:
        print(f"A verificar existência do mês {mes_atual} no servidor...")
        if not verificar_disponibilidade_mes(mes_atual):
            print(f"\n🏁 Backfill finalizado! O mês {mes_atual} ainda não foi publicado pela Receita Federal.")
            break
            
        print("\n" + "="*60)
        print(f"🔄 A iniciar a extração da carga: {mes_atual}")
        print("="*60)
        
        # 2. Executa passando o mês explicitamente para o extrator
        executar_pipeline_local_para_nuvem(local_test=local_test, test_month=mes_atual)
        
        # 3. Verifica se atingiu o limite definido pelo utilizador
        if mes_fim and mes_atual == mes_fim:
            print(f"\n🎯 Mês limite ({mes_fim}) processado com sucesso. Paragem solicitada.")
            break
        
        # 4. Prepara a próxima iteração
        mes_atual = obter_proximo_mes(mes_atual)
        
        print(f"\n⏳ Pausa de 10 segundos antes de avançar para o próximo mês...")
        time.sleep(10)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill Histórico - Dados CNPJ")
    parser.add_argument("--local-test", action="store_true", help="Executa sem enviar para o S3.")
    parser.add_argument("--start-month", type=str, help="Mês de início (ex: 2024-01). Ignora o state file.")
    parser.add_argument("--end-month", type=str, help="Mês de fim (ex: 2024-03). O pipeline para após processar este mês.")
    
    args = parser.parse_args()
    executar_backfill(local_test=args.local_test, mes_inicio=args.start_month, mes_fim=args.end_month)