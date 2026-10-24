import argparse
import os
import re
import shutil
import urllib.parse
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Callable, List, Optional

import basedosdados as bd
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from tqdm import tqdm

load_dotenv()

TOKEN_SHARE = os.getenv("TOKEN_SHARE")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")

URL_DOWNLOAD_BASE = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/{TOKEN_SHARE}"
URL_IBGE_CNAE = "https://servicodados.ibge.gov.br/api/v2/cnae/subclasses"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
PASTA_TEMP = "tmp_receita"
S3_PREFIX = "landing/receita_federal"

REGEX_CPF_SUJO = re.compile(r'(?i)[-.\s]*(?:CPF)?[-.\s]*(?<!\d)\d{3}\.?\d{3}\.?\d{3}-?\d{2}(?!\d)[-.\s]*')
REGEX_FILIAL_SUJA = re.compile(r'^/?\d{4}-?\d{2}')
REGEX_LIXO_INICIO = re.compile(r'^[-.\s/]+')


@dataclass
class TabelaLandingConfig:
    nome_tabela: str
    pasta_s3: str
    colunas: List[str]
    origem: str = "RFB_ZIP"  # Opções: "RFB_ZIP", "API_IBGE", "BQ_CEPS"
    prefixo_zip: str = ""
    is_snapshot_mensal: bool = True
    colunas_float: List[str] = field(default_factory=list)
    min_linhas_esperadas: int = 1
    transform_chunk_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None


def gerar_range_meses(mes_ini: Optional[str] = None, mes_fim: Optional[str] = None) -> List[str]:
    mes_padrao = (datetime.now() - relativedelta(months=1)).strftime("%Y-%m")
    ini_str = mes_ini.strip() if mes_ini and mes_ini.strip() else mes_padrao
    fim_str = mes_fim.strip() if mes_fim and mes_fim.strip() else ini_str

    dt_atual = datetime.strptime(ini_str, "%Y-%m")
    dt_fim = datetime.strptime(fim_str, "%Y-%m")
    if dt_atual > dt_fim:
        raise ValueError(f"mes_referencia_inicial ({ini_str}) maior que mes_referencia_final ({fim_str}).")

    meses = []
    while dt_atual <= dt_fim:
        meses.append(dt_atual.strftime("%Y-%m"))
        dt_atual += relativedelta(months=1)
    return meses


def gerar_schema_arrow(config: TabelaLandingConfig) -> pa.Schema:
    campos = []
    floats = set(config.colunas_float)
    for col in config.colunas:
        campos.append((col, pa.float64() if col in floats else pa.string()))
    if config.is_snapshot_mensal:
        campos.append(("mes_referencia", pa.string()))
    campos.append(("ingested_at", pa.string()))
    return pa.schema(campos)


def enviar_para_s3(caminho_local: str, chave_s3: str):
    print(f"  ☁️ Enviando para s3://{AWS_BUCKET_NAME}/{chave_s3} ...", end=" ", flush=True)
    s3_client = boto3.client("s3")
    s3_client.upload_file(caminho_local, AWS_BUCKET_NAME, chave_s3)
    print("✅ Concluído!")


def arquivo_existe_s3(chave_s3: str) -> bool:
    s3_client = boto3.client("s3")
    try:
        s3_client.head_object(Bucket=AWS_BUCKET_NAME, Key=chave_s3)
        return True
    except ClientError:
        return False


def listar_arquivos_do_mes(diretorio_mes: str) -> List[str]:
    url_dir = f"{URL_DOWNLOAD_BASE}/{diretorio_mes}/"
    headers_propfind = {**HEADERS, "Depth": "1"}
    res = requests.request("PROPFIND", url_dir, headers=headers_propfind, timeout=30)
    if res.status_code in (200, 207):
        padrao = r'/([^/]+\.zip)'
        return sorted(list(set(re.findall(padrao, urllib.parse.unquote(res.text)))))
    return []


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((requests.RequestException, zipfile.BadZipFile)),
    reraise=True
)
def descarregar_e_extrair(url: str, nome_zip: str, pasta_temp_tabela: str) -> List[str]:
    caminho_zip = os.path.join(pasta_temp_tabela, nome_zip)
    with requests.get(url, stream=True, headers=HEADERS, timeout=(30, 600)) as r:
        r.raise_for_status()
        if "text/html" in r.headers.get("Content-Type", ""):
            raise zipfile.BadZipFile(f"Falso ZIP detectado (HTML): {url}")
        total_size = int(r.headers.get("content-length", 0))
        with open(caminho_zip, "wb") as f, tqdm(
            desc=nome_zip, total=total_size, unit="iB", unit_scale=True, unit_divisor=1024, leave=False
        ) as bar:
            for chunk in r.iter_content(chunk_size=2 * 1024 * 1024):
                if chunk:
                    bar.update(f.write(chunk))

    extraidos = []
    try:
        with zipfile.ZipFile(caminho_zip, "r") as zip_ref:
            zip_ref.extractall(pasta_temp_tabela)
            for nome_interno in zip_ref.namelist():
                extraidos.append(os.path.join(pasta_temp_tabela, nome_interno))
    finally:
        if os.path.exists(caminho_zip):
            os.remove(caminho_zip)
    return extraidos


def limpar_chunk_empresas_mei(chunk: pd.DataFrame) -> pd.DataFrame:
    """Função específica para limpeza de CPF na Razão Social do MEI e conversão de capital_social."""
    mask_mei = (chunk["porte_empresa"] == "01") & (chunk["natureza_juridica"] == "2135")
    if mask_mei.any():
        razoes_limpas = []
        subset_mei = chunk[mask_mei]
        for razao, cnpj in zip(subset_mei["razao_social"], subset_mei["cnpj_basico"]):
            razao_str = "" if pd.isna(razao) or str(razao).strip().lower() in ["nan", "none", ""] else str(razao).strip()
            cnpj_str = str(cnpj).zfill(8)
            cnpj_formatado = f"{cnpj_str[:2]}.{cnpj_str[2:5]}.{cnpj_str[5:]}"
            if not razao_str:
                razoes_limpas.append(cnpj_formatado)
                continue
            nome_sem_cpf = re.sub(r"\s+", " ", REGEX_CPF_SUJO.sub(" ", razao_str).strip())
            if nome_sem_cpf.startswith(cnpj_formatado):
                nome_sem_cpf = nome_sem_cpf[len(cnpj_formatado):]
            elif nome_sem_cpf.startswith(cnpj_str):
                nome_sem_cpf = nome_sem_cpf[len(cnpj_str):]
            nome_sem_cpf = REGEX_LIXO_INICIO.sub("", REGEX_FILIAL_SUJA.sub("", nome_sem_cpf)).strip()
            razoes_limpas.append(f"{cnpj_formatado} {nome_sem_cpf}" if nome_sem_cpf else cnpj_formatado)
        chunk.loc[mask_mei, "razao_social"] = razoes_limpas

    if "capital_social" in chunk.columns:
        chunk["capital_social"] = pd.to_numeric(
            chunk["capital_social"].astype(str).str.replace(",", "."), errors="coerce"
        ).fillna(0.0)
    return chunk


def _extrair_zip_rfb_para_s3(config: TabelaLandingConfig, mes_alvo: str, chave_s3: str, pasta_temp_tabela: str):
    caminho_parquet_local = os.path.join(pasta_temp_tabela, f"{config.pasta_s3}_{mes_alvo}.parquet")
    schema_arrow = gerar_schema_arrow(config)
    writer = None

    arquivos_disponiveis = listar_arquivos_do_mes(mes_alvo)
    arquivos_tabela = [a for a in arquivos_disponiveis if a.startswith(config.prefixo_zip) and a.endswith(".zip")]
    if not arquivos_tabela:
        raise FileNotFoundError(f"Nenhum ZIP encontrado para {config.nome_tabela} ({config.prefixo_zip}) no mês {mes_alvo}.")

    try:
        for nome_arquivo in arquivos_tabela:
            url = f"{URL_DOWNLOAD_BASE}/{mes_alvo}/{nome_arquivo}"
            ficheiros_csv = descarregar_e_extrair(url, nome_arquivo, pasta_temp_tabela)
            for ficheiro in ficheiros_csv:
                chunks = pd.read_csv(
                    ficheiro, sep=";", header=None, names=config.colunas,
                    encoding="iso-8859-1", chunksize=250_000, dtype=str
                )
                for chunk in tqdm(chunks, desc=f"Convertendo {os.path.basename(ficheiro)}", leave=False):
                    if config.transform_chunk_fn is not None:
                        chunk = config.transform_chunk_fn(chunk)
                    if config.is_snapshot_mensal:
                        chunk["mes_referencia"] = mes_alvo
                    chunk["ingested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    tabela_arrow = pa.Table.from_pandas(chunk, schema=schema_arrow)
                    if writer is None:
                        writer = pq.ParquetWriter(caminho_parquet_local, schema_arrow, compression="snappy")
                    writer.write_table(tabela_arrow)
                os.remove(ficheiro)
    finally:
        if writer:
            writer.close()

    enviar_para_s3(caminho_parquet_local, chave_s3)
    os.remove(caminho_parquet_local)


def extrair_tabela_landing(config: TabelaLandingConfig, mes_ini: Optional[str] = None, mes_fim: Optional[str] = None, force: bool = False):
    pasta_temp_tabela = os.path.join(PASTA_TEMP, config.pasta_s3)
    os.makedirs(pasta_temp_tabela, exist_ok=True)
    schema_arrow = gerar_schema_arrow(config)

    try:
        # 1. Fontes Estáticas (Sem partição mensal: pega apenas 1 vez ou usa o último mês do range)
        if not config.is_snapshot_mensal:
            chave_s3 = f"{S3_PREFIX}/{config.pasta_s3}/dados.parquet"
            if not force and arquivo_existe_s3(chave_s3):
                print(f"⏭️ [SKIP] {config.nome_tabela} já existe em s3://{AWS_BUCKET_NAME}/{chave_s3}. Use --force para sobrescrever.")
                return

            caminho_local = os.path.join(pasta_temp_tabela, f"{config.pasta_s3}.parquet")

            if config.origem == "API_IBGE":
                print(f"🚀 Extraindo {config.nome_tabela} (API IBGE)...")
                res = requests.get(URL_IBGE_CNAE, headers=HEADERS, timeout=30)
                res.raise_for_status()
                linhas = {}
                for item in res.json():
                    codigo = re.sub(r"\D", "", str(item.get("id", "")))
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
                        "ingested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                df_ibge = pd.DataFrame(list(linhas.values()))
                pq.write_table(pa.Table.from_pandas(df_ibge, schema=schema_arrow), caminho_local, compression="snappy")
                enviar_para_s3(caminho_local, chave_s3)

            elif config.origem == "BQ_CEPS":
                if not GCP_PROJECT_ID:
                    raise ValueError("Variável GCP_PROJECT_ID ausente no .env!")
                print(f"🚀 Extraindo {config.nome_tabela} via BigQuery Sandbox ({GCP_PROJECT_ID})...")
                query_ceps = """
                SELECT 
                    LPAD(REGEXP_REPLACE(TRIM(cep), '[^0-9]', ''), 8, '0') AS cep,
                    UPPER(TRIM(sigla_uf)) AS uf,
                    UPPER(TRIM(nome_municipio)) AS cidade,
                    UPPER(TRIM(localidade)) AS localidade,
                    CAST(id_municipio AS STRING) AS codigo_ibge,
                    ST_Y(centroide) AS latitude,
                    ST_X(centroide) AS longitude
                FROM `basedosdados.br_bd_diretorios_brasil.cep`
                WHERE cep IS NOT NULL 
                  AND LENGTH(REGEXP_REPLACE(TRIM(cep), '[^0-9]', '')) = 8
                """
                df_ceps = bd.read_sql(query=query_ceps, billing_project_id=GCP_PROJECT_ID)
                df_ceps = df_ceps.drop_duplicates(subset=["cep"]).reset_index(drop=True)
                df_ceps["ingested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                pq.write_table(pa.Table.from_pandas(df_ceps, schema=schema_arrow), caminho_local, compression="snappy")
                enviar_para_s3(caminho_local, chave_s3)

            elif config.origem == "RFB_ZIP":
                # Se uma tabela da RFB for configurada como estática, baixa apenas o ÚLTIMO mês do range!
                mes_mais_recente = gerar_range_meses(mes_ini, mes_fim)[-1]
                print(f"🚀 Extraindo domínio estático {config.nome_tabela} usando o snapshot mais recente ({mes_mais_recente})...")
                _extrair_zip_rfb_para_s3(config, mes_mais_recente, chave_s3, pasta_temp_tabela)

            return

        # 2. Fontes Mensais Particionadas (RFB_ZIP com is_snapshot_mensal = True)
        meses = gerar_range_meses(mes_ini, mes_fim)
        print(f"🚀 Iniciando Extração Landing de {config.nome_tabela} para os meses: {meses}")
        for mes in meses:
            chave_s3 = f"{S3_PREFIX}/{config.pasta_s3}/mes_referencia={mes}/dados.parquet"
            if not force and arquivo_existe_s3(chave_s3):
                print(f"⏭️ [SKIP] {config.nome_tabela} ({mes}) já existe no S3. Use --force para sobrescrever.")
                continue
            print(f"🔄 Baixando e convertendo {config.nome_tabela} (Mês: {mes})...")
            _extrair_zip_rfb_para_s3(config, mes, chave_s3, pasta_temp_tabela)

    finally:
        if os.path.exists(pasta_temp_tabela):
            shutil.rmtree(pasta_temp_tabela)


def _validar_parquet_s3(chave_s3: str, config: TabelaLandingConfig):
    """Baixa apenas os metadados/footer do Parquet no S3 ou valida via pyarrow sem precisar de cluster Spark."""
    s3_client = boto3.client("s3")
    try:
        head = s3_client.head_object(Bucket=AWS_BUCKET_NAME, Key=chave_s3)
    except ClientError:
        raise FileNotFoundError(f"❌ ERRO DQ Landing: Arquivo não encontrado em s3://{AWS_BUCKET_NAME}/{chave_s3}")

    tamanho_bytes = head.get("ContentLength", 0)
    if tamanho_bytes == 0:
        raise ValueError(f"❌ ERRO DQ Landing: Arquivo vazio (0 bytes) em s3://{AWS_BUCKET_NAME}/{chave_s3}")

    # Lê apenas o footer do Parquet diretamente do S3 usando PyArrow S3FileSystem
    fs = pa.fs.S3FileSystem(region=s3_client.meta.region_name)
    caminho_pa = f"{AWS_BUCKET_NAME}/{chave_s3}"
    metadata = pq.read_metadata(caminho_pa, filesystem=fs)
    schema_real = pq.read_schema(caminho_pa, filesystem=fs)

    num_linhas = metadata.num_rows
    if num_linhas < config.min_linhas_esperadas:
        raise ValueError(
            f"❌ ERRO DQ Landing [{config.nome_tabela}]: Volumetria abaixo do mínimo em {chave_s3} "
            f"({num_linhas:,} linhas < {config.min_linhas_esperadas:,} esperadas)."
        )

    colunas_reais = set(schema_real.names)
    faltantes = set(config.colunas) - colunas_reais
    if faltantes:
        raise ValueError(f"❌ ERRO DQ Landing [{config.nome_tabela}]: Colunas ausentes em {chave_s3}: {faltantes}")

    print(f"  ✅ OK: {chave_s3} ({num_linhas:,} linhas | {tamanho_bytes / (1024*1024):.2f} MB)")


def testar_tabela_landing(config: TabelaLandingConfig, mes_ini: Optional[str] = None, mes_fim: Optional[str] = None):
    print(f"🧪 Executando Data Quality S3 Landing: {config.nome_tabela}")
    if not config.is_snapshot_mensal:
        chave_s3 = f"{S3_PREFIX}/{config.pasta_s3}/dados.parquet"
        _validar_parquet_s3(chave_s3, config)
        return

    meses = gerar_range_meses(mes_ini, mes_fim)
    for mes in meses:
        chave_s3 = f"{S3_PREFIX}/{config.pasta_s3}/mes_referencia={mes}/dados.parquet"
        _validar_parquet_s3(chave_s3, config)


def executar_extract_landing_cli(config: TabelaLandingConfig):
    parser = argparse.ArgumentParser(description=f"Extração Landing S3: {config.nome_tabela}")
    parser.add_argument("--mes_referencia_inicial", default="", help="YYYY-MM inicial (Vazio = Mês-1)")
    parser.add_argument("--mes_referencia_final", default="", help="YYYY-MM final (Vazio = Mês-1)")
    parser.add_argument("--force", action="store_true", help="Força novo download mesmo se o Parquet já existir no S3")
    args, _ = parser.parse_known_args()
    extrair_tabela_landing(config, args.mes_referencia_inicial, args.mes_referencia_final, force=args.force)


def executar_dq_landing_cli(config: TabelaLandingConfig):
    parser = argparse.ArgumentParser(description=f"DQ Landing S3: {config.nome_tabela}")
    parser.add_argument("--mes_referencia_inicial", default="", help="YYYY-MM inicial (Vazio = Mês-1)")
    parser.add_argument("--mes_referencia_final", default="", help="YYYY-MM final (Vazio = Mês-1)")
    args, _ = parser.parse_known_args()
    testar_tabela_landing(config, args.mes_referencia_inicial, args.mes_referencia_final)