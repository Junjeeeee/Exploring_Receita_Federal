import os
import re
import zipfile
import requests
import shutil
import boto3
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()

TOKEN_SHARE = "YggdBLfdninEJX9"
URL_DOWNLOAD_BASE = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/{TOKEN_SHARE}"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

PASTA_TEMP = "tmp_receita"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
S3_PREFIX = "bronze/receita_federal"
STATE_FILE = "last_processed_month.txt"

# Schemas atualizados com as tabelas de domínio
SCHEMAS_CNPJ = {
    "EMPRESA": [
        "cnpj_basico", "razao_social", "natureza_juridica",
        "qualificacao_responsavel", "capital_social", "porte_empresa",
        "ente_federativo_responsavel"
    ],
    "ESTABELE": [ 
        "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
        "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
        "motivo_situacao_cadastral", "nome_cidade_exterior", "pais",
        "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria",
        "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
        "cep", "uf", "municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2",
        "ddd_fax", "fax", "correio_eletronico", "situacao_especial",
        "data_situacao_especial"
    ],
    "SIMPLES": [
        "cnpj_basico", "opcao_pelo_simples", "data_opcao_simples",
        "data_exclusao_simples", "opcao_pelo_mei", "data_opcao_mei",
        "data_exclusao_mei"
    ],
    "SOCIO": [
        "cnpj_basico", "identificador_socio", "nome_socio_razao_social",
        "cnpj_cpf_socio", "qualificacao_socio", "data_entrada_sociedade",
        "pais", "representante_legal", "nome_representante",
        "qualificacao_representante_legal", "faixa_etaria"
    ],
    "DOMINIO": [
        "codigo", "descricao"
    ]
}

def obter_proximo_mes(mes_atual_str):
    if not mes_atual_str:
        return "2024-01"
    atual = datetime.strptime(mes_atual_str, "%Y-%m")
    return (atual + relativedelta(months=1)).strftime("%Y-%m")

def verificar_disponibilidade_mes(mes_str):
    try:
        response = requests.request("PROPFIND", f"{URL_DOWNLOAD_BASE}/{mes_str}/", headers=HEADERS, timeout=30)
        return response.status_code in (200, 207)
    except:
        return False

def descarregar_e_extrair(url, nome_zip):
    caminho_zip = os.path.join(PASTA_TEMP, nome_zip)
    try:
        with requests.get(url, stream=True, headers=HEADERS, timeout=(30, 600)) as r:
            r.raise_for_status() 
            with open(caminho_zip, 'wb') as f:
                for chunk in r.iter_content(chunk_size=2 * 1024 * 1024):
                    if chunk: f.write(chunk)
                        
        extraidos = []
        with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
            zip_ref.extractall(PASTA_TEMP)
            for nome_interno in zip_ref.namelist():
                extraidos.append(os.path.join(PASTA_TEMP, nome_interno))
                
        os.remove(caminho_zip)
        return extraidos
    except Exception as e:
        print(f"     [!] Erro em {nome_zip}: {e}")
        if os.path.exists(caminho_zip): os.remove(caminho_zip)
        return []

def aplicar_mascara_mei(chunk, nome_tabela):
    regex_cpf = re.compile(r'\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b')
    if nome_tabela == "EMPRESA":
        chunk['razao_social'] = [
            regex_cpf.sub(str(cnpj), str(razao)) 
            for razao, cnpj in zip(chunk['razao_social'], chunk['cnpj_basico'])
        ]
    return chunk

def enviar_para_s3(caminho_local, chave_s3):
    print(f"  -> A enviar {caminho_local} para s3://{AWS_BUCKET_NAME}/{chave_s3} ...")
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(caminho_local, AWS_BUCKET_NAME, chave_s3)
        print("     Upload concluído com sucesso!")
    except Exception as e:
        print(f"     [!] Falha no upload para o S3: {e}")
        raise e

def processar_tabela(diretorio_mes, nome_tabela, prefixo_zip, colunas, quantidade_zips=10, local_test=False):
    print(f"\n--- A processar Tabela {nome_tabela} ({diretorio_mes}) ---")
    
    caminho_parquet_local = os.path.join(PASTA_TEMP, f"{nome_tabela.lower()}_{diretorio_mes}.parquet")
    writer = None
    
    for i in range(quantidade_zips):
        # Correção aplicada: Se for 1 arquivo único (Simples ou Domínios), não usa índice numérico no final.
        nome_arquivo = f"{prefixo_zip}.zip" if quantidade_zips == 1 else f"{prefixo_zip}{i}.zip"
        
        url = f"{URL_DOWNLOAD_BASE}/{diretorio_mes}/{nome_arquivo}"
        ficheiros_csv = descarregar_e_extrair(url, nome_arquivo)
        
        for ficheiro in ficheiros_csv:
            chunks = pd.read_csv(ficheiro, sep=';', header=None, names=colunas, 
                                 encoding='iso-8859-1', chunksize=100_000, dtype=str)
            
            for chunk in chunks:
                chunk = chunk.fillna("")
                chunk = aplicar_mascara_mei(chunk, nome_tabela)
                
                chunk['mes_referencia'] = diretorio_mes
                chunk['ingested_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                tabela_arrow = pa.Table.from_pandas(chunk)
                
                if writer is None:
                    writer = pq.ParquetWriter(caminho_parquet_local, tabela_arrow.schema, compression='snappy')
                
                writer.write_table(tabela_arrow)
                
            os.remove(ficheiro)

    if writer:
        writer.close()
        chave_s3 = f"{S3_PREFIX}/{nome_tabela.lower()}/mes_referencia={diretorio_mes}/dados.parquet"
        
        if not local_test:
            enviar_para_s3(caminho_parquet_local, chave_s3)
            os.remove(caminho_parquet_local)
        else:
            print(f"  [MODO TESTE] Upload para S3 cancelado. Arquivo mantido em: {caminho_parquet_local}")
    else:
        print(f"  [AVISO] Nenhum dado extraído para {nome_tabela}.")

def executar_pipeline_local_para_nuvem(local_test=False, test_month=None):
    os.makedirs(PASTA_TEMP, exist_ok=True)
    
    if test_month:
        proximo_mes = test_month
        print(f"Atenção: Forçando execução para o mês {proximo_mes}")
    else:
        ultimo_mes = None
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                ultimo_mes = f.read().strip()
        proximo_mes = obter_proximo_mes(ultimo_mes)
        print(f"A verificar novos dados para: {proximo_mes}...")

    if verificar_disponibilidade_mes(proximo_mes):
        print(f"✅ Dados encontrados! A iniciar extração...")
        
        # Inclusão das 5 tabelas de domínio do documento
        tabelas_config = [
            ("ESTABELE", "Estabelecimentos", SCHEMAS_CNPJ["ESTABELE"], 10),
            ("EMPRESA", "Empresas", SCHEMAS_CNPJ["EMPRESA"], 10),
            ("SOCIO", "Socios", SCHEMAS_CNPJ["SOCIO"], 10),
            ("SIMPLES", "Simples", SCHEMAS_CNPJ["SIMPLES"], 1),
            ("MUNICIPIOS", "Municipios", SCHEMAS_CNPJ["DOMINIO"], 1),
            ("PAISES", "Paises", SCHEMAS_CNPJ["DOMINIO"], 1),
            ("CNAES", "Cnaes", SCHEMAS_CNPJ["DOMINIO"], 1),
            ("NATUREZAS", "Naturezas", SCHEMAS_CNPJ["DOMINIO"], 1),
            ("QUALIFICACOES", "Qualificacoes", SCHEMAS_CNPJ["DOMINIO"], 1)
        ]
        
        for nome_tabela, prefixo, schema, qtde in tabelas_config:
            processar_tabela(proximo_mes, nome_tabela, prefixo, schema, qtde, local_test)
            
        if not local_test:
            with open(STATE_FILE, "w") as f:
                f.write(proximo_mes)
            print(f"🎉 Mês {proximo_mes} totalmente enviado para o Data Lake S3.")
        else:
            print(f"🎉 [MODO TESTE] Processamento de {proximo_mes} finalizado na máquina local.")
    else:
        print("⏳ Dados ainda não disponíveis.")

    if not local_test and os.path.exists(PASTA_TEMP):
        shutil.rmtree(PASTA_TEMP)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrator de Dados da Receita Federal para S3")
    parser.add_argument("--local-test", action="store_true", help="Executa sem enviar para o S3 e sem apagar os Parquets locais.")
    parser.add_argument("--month", type=str, help="Força a execução de um mês específico (ex: 2024-01).")
    
    args = parser.parse_args()
    
    executar_pipeline_local_para_nuvem(local_test=args.local_test, test_month=args.month)