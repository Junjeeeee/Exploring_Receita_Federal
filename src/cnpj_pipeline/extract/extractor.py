import os
import re
import json
import zipfile
import requests
import shutil
import boto3
import argparse
import time
import resource
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()

TOKEN_SHARE = os.getenv("TOKEN_SHARE")
if not TOKEN_SHARE:
    raise ValueError("ERRO FATAL: Variável TOKEN_SHARE não encontrada no ficheiro .env")

URL_DOWNLOAD_BASE = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/{TOKEN_SHARE}"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

PASTA_TEMP = "tmp_receita"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
S3_PREFIX = "bronze/receita_federal"
STATE_FILE = "last_processed_month.txt"
CHECKPOINT_FILE = "checkpoint_estado.json"

REGEX_CPF_SUJO = re.compile(r'(?i)[-.\s]*(?:CPF)?[-.\s]*(?<!\d)\d{3}\.?\d{3}\.?\d{3}-?\d{2}(?!\d)[-.\s]*')
REGEX_FILIAL_SUJA = re.compile(r'^/?\d{4}-?\d{2}')
REGEX_LIXO_INICIO = re.compile(r'^[-.\s/]+')

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

# --- SISTEMA DE CHECKPOINT ---
def carregar_checkpoint(mes_referencia):
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                dados = json.load(f)
                if dados.get("mes") == mes_referencia:
                    return set(dados.get("tabelas_concluidas", []))
        except Exception as e:
            print(f"⚠️ [Checkpoint] Falha ao ler arquivo de checkpoint: {e}")
    return set()

def salvar_checkpoint(mes_referencia, tabela):
    concluidas = carregar_checkpoint(mes_referencia)
    concluidas.add(tabela)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"mes": mes_referencia, "tabelas_concluidas": list(concluidas)}, f, indent=2)

def limpar_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

def obter_proximo_mes(mes_atual_str):
    if not mes_atual_str:
        return "2023-05"
    atual = datetime.strptime(mes_atual_str, "%Y-%m")
    return (atual + relativedelta(months=1)).strftime("%Y-%m")

def verificar_disponibilidade_mes(mes_str):
    try:
        response = requests.request("PROPFIND", f"{URL_DOWNLOAD_BASE}/{mes_str}/", headers=HEADERS, timeout=30)
        return response.status_code in (200, 207)
    except requests.RequestException:
        return False

# --- PROTEÇÃO ZIP: Captura também falhas de arquivos corrompidos na origem ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((requests.RequestException, zipfile.BadZipFile)),
    reraise=True
)
def descarregar_e_extrair(url, nome_zip):
    caminho_zip = os.path.join(PASTA_TEMP, nome_zip)
    
    with requests.get(url, stream=True, headers=HEADERS, timeout=(30, 600)) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        
        with open(caminho_zip, 'wb') as f, tqdm(
            desc=nome_zip, total=total_size, unit='iB',
            unit_scale=True, unit_divisor=1024, leave=False
        ) as bar:
            for chunk in r.iter_content(chunk_size=2 * 1024 * 1024):
                if chunk:
                    size = f.write(chunk)
                    bar.update(size)
                    
    extraidos = []
    with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
        zip_ref.extractall(PASTA_TEMP)
        for nome_interno in zip_ref.namelist():
            extraidos.append(os.path.join(PASTA_TEMP, nome_interno))
            
    os.remove(caminho_zip)
    return extraidos

def aplicar_mascara_mei(chunk, nome_tabela):
    if nome_tabela == "EMPRESA":
        mask_mei = (chunk['porte_empresa'] == '01') & (chunk['natureza_juridica'] == '2135')
        
        if mask_mei.any():
            razoes_limpas = []
            subset_mei = chunk[mask_mei]
            
            for razao, cnpj in zip(subset_mei['razao_social'], subset_mei['cnpj_basico']):
                if pd.isna(razao) or str(razao).strip().lower() in ['nan', 'none', '']:
                    razao_str = ""
                else:
                    razao_str = str(razao).strip()
                    
                cnpj_str = str(cnpj).zfill(8)
                cnpj_formatado = f"{cnpj_str[:2]}.{cnpj_str[2:5]}.{cnpj_str[5:]}"
                
                if not razao_str:
                    razoes_limpas.append(cnpj_formatado)
                    continue
                    
                # Fix: Substitui por um espaço para não grudar palavras, e retira duplo espaçamento
                nome_sem_cpf = REGEX_CPF_SUJO.sub(" ", razao_str).strip()
                nome_sem_cpf = re.sub(r'\s+', ' ', nome_sem_cpf)
                
                if nome_sem_cpf.startswith(cnpj_formatado):
                    nome_sem_cpf = nome_sem_cpf[len(cnpj_formatado):]
                elif nome_sem_cpf.startswith(cnpj_str):
                    nome_sem_cpf = nome_sem_cpf[len(cnpj_str):]
                    
                nome_sem_cpf = REGEX_FILIAL_SUJA.sub('', nome_sem_cpf)
                nome_sem_cpf = REGEX_LIXO_INICIO.sub('', nome_sem_cpf).strip()
                
                if nome_sem_cpf:
                    razoes_limpas.append(f"{cnpj_formatado} {nome_sem_cpf}")
                else:
                    razoes_limpas.append(cnpj_formatado)
                    
            chunk.loc[mask_mei, 'razao_social'] = razoes_limpas

        if 'capital_social' in chunk.columns:
            chunk['capital_social'] = pd.to_numeric(
                chunk['capital_social'].astype(str).str.replace(',', '.'), 
                errors='coerce'
            ).fillna(0.0)
            
    return chunk

# --- SCHEMA ARROW DINÂMICO: Blinda contra erros de inferência em chunks ---
def gerar_schema_arrow(colunas):
    campos = []
    for col in colunas:
        if col == 'capital_social':
            campos.append((col, pa.float64()))
        else:
            campos.append((col, pa.string()))
    campos.append(('mes_referencia', pa.string()))
    campos.append(('ingested_at', pa.string()))
    return pa.schema(campos)

def enviar_para_s3(caminho_local, chave_s3):
    print(f"  -> A enviar para s3://{AWS_BUCKET_NAME}/{chave_s3} ...", end=" ", flush=True)
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(caminho_local, AWS_BUCKET_NAME, chave_s3)
        print("✅ Concluído!")
    except Exception as e:
        print(f"\n     [!] Falha no upload para o S3: {e}")
        raise e

def processar_tabela(diretorio_mes, nome_tabela, prefixo_zip, colunas, quantidade_zips=10, local_test=False):
    print(f"\n--- Processando Tabela {nome_tabela} ---")
    
    caminho_parquet_local = os.path.join(PASTA_TEMP, f"{nome_tabela.lower()}_{diretorio_mes}.parquet")
    writer = None
    schema_arrow_fixo = gerar_schema_arrow(colunas)
    
    # Prevenção: Remove arquivo residual de execuções anteriores abortadas
    if os.path.exists(caminho_parquet_local):
        os.remove(caminho_parquet_local)
        
    try:
        for i in range(quantidade_zips):
            nome_arquivo = f"{prefixo_zip}.zip" if quantidade_zips == 1 else f"{prefixo_zip}{i}.zip"
            url = f"{URL_DOWNLOAD_BASE}/{diretorio_mes}/{nome_arquivo}"
            
            ficheiros_csv = descarregar_e_extrair(url, nome_arquivo)
            
            for ficheiro in ficheiros_csv:
                chunks = pd.read_csv(ficheiro, sep=';', header=None, names=colunas, 
                                     encoding='iso-8859-1', chunksize=250_000, dtype=str)
                
                nome_base = os.path.basename(ficheiro)
                for chunk in tqdm(chunks, desc=f"Convertendo {nome_base}", leave=False):
                    chunk = aplicar_mascara_mei(chunk, nome_tabela)
                    chunk['mes_referencia'] = diretorio_mes
                    chunk['ingested_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    # Aplica o schema forçado na conversão
                    tabela_arrow = pa.Table.from_pandas(chunk, schema=schema_arrow_fixo)
                    
                    if writer is None:
                        writer = pq.ParquetWriter(caminho_parquet_local, schema_arrow_fixo, compression='snappy')
                    
                    writer.write_table(tabela_arrow)
                    
                os.remove(ficheiro)
                
    except Exception as e:
        # Se explodir no meio de um arquivo, fecha o writer e apaga o parquet incompleto
        if writer:
            writer.close()
            writer = None
        if os.path.exists(caminho_parquet_local):
            os.remove(caminho_parquet_local)
        raise e

    finally:
        if writer:
            writer.close()
            
    if os.path.exists(caminho_parquet_local):
        chave_s3 = f"{S3_PREFIX}/{nome_tabela.lower()}/mes_referencia={diretorio_mes}/dados.parquet"
        if not local_test:
            enviar_para_s3(caminho_parquet_local, chave_s3)
            os.remove(caminho_parquet_local)
            salvar_checkpoint(diretorio_mes, nome_tabela)
        else:
            tamanho_mb = os.path.getsize(caminho_parquet_local) / (1024 * 1024)
            print(f"  [MODO TESTE] Arquivo mantido: {caminho_parquet_local} ({tamanho_mb:.1f} MB)")
            salvar_checkpoint(diretorio_mes, nome_tabela)
    else:
        print(f"  [AVISO] Nenhum dado extraído para {nome_tabela}.")

def executar_pipeline_local_para_nuvem(local_test=False, test_month=None):
    start_time = time.time()
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
        print(f"✅ Dados encontrados! A iniciar extração...\n")
        
        tabelas_concluidas = carregar_checkpoint(proximo_mes)
        if tabelas_concluidas:
            print(f"📌 Checkpoint detectado: {len(tabelas_concluidas)} tabelas já processadas ({', '.join(tabelas_concluidas)}). Pulando-as...\n")
        
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
            if nome_tabela in tabelas_concluidas:
                print(f"⏭️  Pulando {nome_tabela} (já concluída pelo checkpoint).")
                continue
            processar_tabela(proximo_mes, nome_tabela, prefixo, schema, qtde, local_test)
            
        if not local_test:
            with open(STATE_FILE, "w") as f:
                f.write(proximo_mes)
            limpar_checkpoint()
            print(f"\n🎉 Mês {proximo_mes} totalmente enviado para o Data Lake S3.")
        else:
            print(f"\n🎉 [MODO TESTE] Processamento de {proximo_mes} finalizado.")
    else:
        print("⏳ Dados ainda não disponíveis.")

    if not local_test and os.path.exists(PASTA_TEMP):
        shutil.rmtree(PASTA_TEMP)

    tempo_total = time.time() - start_time
    minutos, segundos = divmod(tempo_total, 60)
    ram_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    
    print("\n" + "="*50)
    print(f"⏱️  Tempo total de execução : {int(minutos)}m {int(segundos)}s")
    print(f"💾 Pico de RAM utilizada    : {ram_mb:.2f} MB")
    print("="*50)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrator de Dados da Receita Federal para S3")
    parser.add_argument("--local-test", action="store_true", help="Executa sem enviar para S3 e mantém parquets locais.")
    parser.add_argument("--month", type=str, help="Força a execução de um mês específico (ex: 2024-01).")
    
    args = parser.parse_args()
    executar_pipeline_local_para_nuvem(local_test=args.local_test, test_month=args.month)