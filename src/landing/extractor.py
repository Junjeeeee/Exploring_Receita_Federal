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
import urllib.parse
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
URL_IBGE_CNAE = "https://servicodados.ibge.gov.br/api/v2/cnae/subclasses"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

PASTA_TEMP = "tmp_receita"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
S3_PREFIX = "landing/receita_federal"
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
    ],
    "CNAES_IBGE": [
        "codigo", "descricao", "secao", "nome_secao",
        "divisao", "nome_divisao", "grupo", "nome_grupo",
        "classe", "nome_classe"
    ]
}

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

def listar_arquivos_do_mes(diretorio_mes):
    url_dir = f"{URL_DOWNLOAD_BASE}/{diretorio_mes}/"
    headers_propfind = HEADERS.copy()
    headers_propfind["Depth"] = "1"
    try:
        res = requests.request("PROPFIND", url_dir, headers=headers_propfind, timeout=30)
        if res.status_code in (200, 207):
            padrao = r'/([^/]+\.zip)'
            nomes = re.findall(padrao, urllib.parse.unquote(res.text))
            return list(set(nomes))
    except Exception:
        pass
    return []

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((requests.RequestException, zipfile.BadZipFile)),
    reraise=True
)
def descarregar_e_extrair(url, nome_zip):
    caminho_zip = os.path.join(PASTA_TEMP, nome_zip)
    with requests.get(url, stream=True, headers=HEADERS, timeout=(30, 600)) as r:
        if r.status_code == 404:
            raise requests.exceptions.HTTPError(f"404 Not Found: {url}")
        r.raise_for_status()
        content_type = r.headers.get('Content-Type', '')
        if 'text/html' in content_type:
            raise zipfile.BadZipFile(f"Falso ZIP detectado (HTML retornado): {url}")
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
    try:
        with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
            zip_ref.extractall(PASTA_TEMP)
            for nome_interno in zip_ref.namelist():
                extraidos.append(os.path.join(PASTA_TEMP, nome_interno))
    except zipfile.BadZipFile as e:
        if os.path.exists(caminho_zip):
            os.remove(caminho_zip)
        raise e
            
    if os.path.exists(caminho_zip):
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

def processar_cnaes_ibge(diretorio_mes, colunas, local_test=False):
    """Extrai a hierarquia completa de CNAEs da API do IBGE localmente e envia ao S3."""
    print(f"\n--- Processando Tabela CNAES_IBGE (API IBGE) ---")
    caminho_parquet_local = os.path.join(PASTA_TEMP, f"cnaes_ibge_{diretorio_mes}.parquet")
    schema_arrow_fixo = gerar_schema_arrow(colunas)

    res = requests.get(URL_IBGE_CNAE, headers=HEADERS, timeout=30)
    res.raise_for_status()
    dados = res.json()

    linhas = {}
    for item in dados:
        # Mantém apenas os 7 dígitos numéricos para casar 100% com a Receita Federal
        codigo = re.sub(r'\D', '', str(item.get("id", "")))
        if not codigo or codigo in linhas:
            continue
        classe = item.get("classe", {}) or {}
        grupo = classe.get("grupo", {}) or {}
        divisao = grupo.get("divisao", {}) or {}
        secao = divisao.get("secao", {}) or {}

        linhas[codigo] = {
            "codigo": codigo,
            "descricao": str(item.get("descricao", "")).upper().strip(),
            "secao": str(secao.get("id", "")).strip(),
            "nome_secao": str(secao.get("descricao", "")).upper().strip(),
            "divisao": str(divisao.get("id", "")).strip(),
            "nome_divisao": str(divisao.get("descricao", "")).upper().strip(),
            "grupo": str(grupo.get("id", "")).strip(),
            "nome_grupo": str(grupo.get("descricao", "")).upper().strip(),
            "classe": str(classe.get("id", "")).strip(),
            "nome_classe": str(classe.get("descricao", "")).upper().strip(),
            "mes_referencia": diretorio_mes,
            "ingested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    df_ibge = pd.DataFrame(list(linhas.values()))
    tabela_arrow = pa.Table.from_pandas(df_ibge, schema=schema_arrow_fixo)
    pq.write_table(tabela_arrow, caminho_parquet_local, compression='snappy')

    chave_s3 = f"{S3_PREFIX}/cnaes_ibge/mes_referencia={diretorio_mes}/dados.parquet"
    if not local_test:
        enviar_para_s3(caminho_parquet_local, chave_s3)
        os.remove(caminho_parquet_local)
    else:
        print(f"  [MODO TESTE] Arquivo mantido: {caminho_parquet_local}")

def processar_tabela(diretorio_mes, nome_tabela, prefixo_zip, colunas, local_test=False):
    if nome_tabela == "CNAES_IBGE":
        processar_cnaes_ibge(diretorio_mes, colunas, local_test)
        return

    print(f"\n--- Processando Tabela {nome_tabela} ---")
    caminho_parquet_local = os.path.join(PASTA_TEMP, f"{nome_tabela.lower()}_{diretorio_mes}.parquet")
    writer = None
    schema_arrow_fixo = gerar_schema_arrow(colunas)
    
    if os.path.exists(caminho_parquet_local):
        os.remove(caminho_parquet_local)
        
    arquivos_disponiveis = listar_arquivos_do_mes(diretorio_mes)
    arquivos_tabela = [arq for arq in arquivos_disponiveis if arq.startswith(prefixo_zip) and arq.endswith('.zip')]
    arquivos_tabela.sort()
    
    if not arquivos_tabela:
        print(f"  [AVISO] Nenhum arquivo encontrado para a tabela {nome_tabela} (prefixo '{prefixo_zip}').")
        return
        
    try:
        for nome_arquivo in arquivos_tabela:
            url = f"{URL_DOWNLOAD_BASE}/{diretorio_mes}/{nome_arquivo}"
            try:
                ficheiros_csv = descarregar_e_extrair(url, nome_arquivo)
            except zipfile.BadZipFile:
                print(f"\n  [!] ALERTA: Arquivo corrompido na origem ({nome_arquivo}). Ignorando...")
                continue
            except requests.exceptions.HTTPError as e:
                if "404" in str(e):
                    continue
                raise e
                
            for ficheiro in ficheiros_csv:
                chunks = pd.read_csv(ficheiro, sep=';', header=None, names=colunas, 
                                     encoding='iso-8859-1', chunksize=250_000, dtype=str)
                nome_base = os.path.basename(ficheiro)
                for chunk in tqdm(chunks, desc=f"Convertendo {nome_base}", leave=False):
                    chunk = aplicar_mascara_mei(chunk, nome_tabela)
                    chunk['mes_referencia'] = diretorio_mes
                    chunk['ingested_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    tabela_arrow = pa.Table.from_pandas(chunk, schema=schema_arrow_fixo)
                    if writer is None:
                        writer = pq.ParquetWriter(caminho_parquet_local, schema_arrow_fixo, compression='snappy')
                    writer.write_table(tabela_arrow)
                os.remove(ficheiro)
    except Exception as e:
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
            salvar_checkpoint(diretorio_mes, nome_tabela)

def executar_pipeline_local_para_nuvem(local_test=False, test_month=None, only_table=None):
    start_time = time.time()
    os.makedirs(PASTA_TEMP, exist_ok=True)
    
    ultimo_mes = None
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            ultimo_mes = f.read().strip()

    if test_month:
        meses_alvo = [m.strip() for m in test_month.split(",")]
        print(f"🎯 Atenção: Forçando execução para o(s) mês(es): {meses_alvo}")
    else:
        meses_alvo = [obter_proximo_mes(ultimo_mes)]
        print(f"🔍 A verificar novos dados para: {meses_alvo[0]}...")

    tabelas_config = [
        ("ESTABELE", "Estabelecimentos", SCHEMAS_CNPJ["ESTABELE"]),
        ("EMPRESA", "Empresas", SCHEMAS_CNPJ["EMPRESA"]),
        ("SOCIO", "Socios", SCHEMAS_CNPJ["SOCIO"]),
        ("SIMPLES", "Simples", SCHEMAS_CNPJ["SIMPLES"]),
        ("MUNICIPIOS", "Municipios", SCHEMAS_CNPJ["DOMINIO"]),
        ("PAISES", "Paises", SCHEMAS_CNPJ["DOMINIO"]),
        ("CNAES", "Cnaes", SCHEMAS_CNPJ["DOMINIO"]),
        ("NATUREZAS", "Naturezas", SCHEMAS_CNPJ["DOMINIO"]),
        ("QUALIFICACOES", "Qualificacoes", SCHEMAS_CNPJ["DOMINIO"]),
        ("MOTIVOS", "Motivos", SCHEMAS_CNPJ["DOMINIO"]),
        ("CNAES_IBGE", " API_IBGE", SCHEMAS_CNPJ["CNAES_IBGE"])
    ]

    if only_table:
        tabelas_Alvo_Set = {t.strip().upper() for t in only_table.split(",")}
        tabelas_config = [t for t in tabelas_config if t[0].upper() in tabelas_Alvo_Set]
        if not tabelas_config:
            raise ValueError(f"Tabela(s) '{only_table}' não encontrada(s) na configuração.")
        print(f"⚡ Modo Cirúrgico Ativado: Processando APENAS {[t[0] for t in tabelas_config]}")

    for proximo_mes in meses_alvo:
        print(f"\n==================== MÊS: {proximo_mes} ====================")
        if verificar_disponibilidade_mes(proximo_mes):
            tabelas_concluidas = carregar_checkpoint(proximo_mes) if not only_table else set()
            for nome_tabela, prefixo, schema in tabelas_config:
                if nome_tabela in tabelas_concluidas:
                    continue
                processar_tabela(proximo_mes, nome_tabela, prefixo, schema, local_test)
                
            if not local_test and not only_table:
                if not ultimo_mes or proximo_mes > ultimo_mes:
                    with open(STATE_FILE, "w") as f:
                        f.write(proximo_mes)
                    ultimo_mes = proximo_mes
                limpar_checkpoint()
        else:
            print(f"⏳ Dados de {proximo_mes} ainda não disponíveis.")

    if not local_test and os.path.exists(PASTA_TEMP):
        shutil.rmtree(PASTA_TEMP)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrator de Dados da Receita Federal para S3")
    parser.add_argument("--local-test", action="store_true")
    parser.add_argument("--month", type=str, help="Mês ou lista de meses separados por vírgula (ex: 2023-05,2023-06).")
    parser.add_argument("--only-table", type=str, help="Tabela ou lista separada por vírgula (ex: MOTIVOS,CNAES_IBGE).")
    args = parser.parse_args()
    executar_pipeline_local_para_nuvem(local_test=args.local_test, test_month=args.month, only_table=args.only_table)