"""Utilitários de extração da camada Landing.

A extração é executada localmente; os arquivos Parquet resultantes são enviados
para o S3 para consumo posterior pela camada Bronze.
"""

import argparse
import os
import re
import tempfile
import urllib.parse
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from pathlib import PurePosixPath
from typing import Callable, List, Optional

import basedosdados as bd
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
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
TAMANHO_CHUNK_CSV = 250_000

# Mantidas conforme a regra de higienização de MEI validada pelo projeto.
# A finalidade é não persistir CPF em claro na razão social dos MEIs.
REGEX_CPF_SUJO = re.compile(r'(?i)[-.\s]*(?:CPF)?[-.\s]*(?<!\d)\d{3}\.?\d{3}\.?\d{3}-?\d{2}(?!\d)[-.\s]*')
REGEX_FILIAL_SUJA = re.compile(r'^/?\d{4}-?\d{2}')
REGEX_LIXO_INICIO = re.compile(r'^[-.\s/]+')


@dataclass
class TabelaLandingConfig:
    nome_tabela: str
    pasta_s3: str
    colunas: List[str]
    origem: str = "RFB_ZIP"  # "RFB_ZIP", "API_IBGE" ou "BQ_CEPS"
    prefixo_zip: str = ""
    is_snapshot_mensal: bool = True
    colunas_float: List[str] = field(default_factory=list)
    min_linhas_esperadas: int = 1
    transform_chunk_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None


def gerar_range_meses(mes_ini: Optional[str] = None, mes_fim: Optional[str] = None) -> List[str]:
    """Gera ['YYYY-MM', ...]. Se não informado, usa o mês anterior."""
    mes_padrao = (datetime.now() - relativedelta(months=1)).strftime("%Y-%m")
    ini_str = mes_ini.strip() if mes_ini and mes_ini.strip() else mes_padrao
    fim_str = mes_fim.strip() if mes_fim and mes_fim.strip() else ini_str

    # Exige o formato exato anunciado pela CLI.
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", ini_str):
        raise ValueError(f"Mês inicial inválido: {ini_str!r}. Use YYYY-MM.")
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", fim_str):
        raise ValueError(f"Mês final inválido: {fim_str!r}. Use YYYY-MM.")

    dt_atual = datetime.strptime(ini_str, "%Y-%m")
    dt_fim = datetime.strptime(fim_str, "%Y-%m")
    if dt_atual > dt_fim:
        raise ValueError(f"mes_referencia_inicial ({ini_str}) maior que mes_referencia_final ({fim_str}).")

    meses = []
    while dt_atual <= dt_fim:
        meses.append(dt_atual.strftime("%Y-%m"))
        dt_atual += relativedelta(months=1)
    return meses


def validar_config_landing(
    config: TabelaLandingConfig, *, validar_dependencias_origem: bool = True
) -> None:
    """Valida o contrato da configuração antes de iniciar rede ou escrita."""
    origens_validas = {"RFB_ZIP", "API_IBGE", "BQ_CEPS"}
    if config.origem not in origens_validas:
        raise ValueError(f"Origem inválida: {config.origem!r}. Valores aceitos: {sorted(origens_validas)}")
    if not config.nome_tabela.strip() or not config.pasta_s3.strip():
        raise ValueError("nome_tabela e pasta_s3 devem ser preenchidos.")
    if not config.colunas or any(not isinstance(c, str) or not c.strip() for c in config.colunas):
        raise ValueError(f"A configuração de {config.nome_tabela} precisa de nomes de colunas válidos.")
    if len(config.colunas) != len(set(config.colunas)):
        raise ValueError(f"Há nomes de colunas duplicados em {config.nome_tabela}.")
    reservadas = {"mes_referencia", "ingested_at"}
    conflito = reservadas.intersection(config.colunas)
    if conflito:
        raise ValueError(f"Colunas de origem usam nomes reservados: {sorted(conflito)}")
    floats_faltantes = set(config.colunas_float) - set(config.colunas)
    if floats_faltantes:
        raise ValueError(f"colunas_float não presentes em colunas: {sorted(floats_faltantes)}")
    if config.min_linhas_esperadas < 1:
        raise ValueError("min_linhas_esperadas deve ser maior ou igual a 1.")
    if config.is_snapshot_mensal and config.origem != "RFB_ZIP":
        raise ValueError(
            f"A origem {config.origem} ainda não implementa snapshots mensais; "
            "use is_snapshot_mensal=False ou implemente o fluxo mensal para essa origem."
        )
    if config.origem == "RFB_ZIP" and not config.prefixo_zip.strip():
        raise ValueError(f"prefixo_zip não informado para a tabela {config.nome_tabela}.")
    _require_bucket()
    if validar_dependencias_origem and config.origem == "RFB_ZIP":
        token = os.getenv("TOKEN_SHARE") or TOKEN_SHARE
        if not token or token.strip().lower() in {"none", "null"}:
            raise EnvironmentError("Variável TOKEN_SHARE ausente no .env ou ambiente.")
    if validar_dependencias_origem and config.origem == "BQ_CEPS" and not (os.getenv("GCP_PROJECT_ID") or GCP_PROJECT_ID):
        raise EnvironmentError("Variável GCP_PROJECT_ID ausente no .env ou ambiente.")


def gerar_schema_arrow(config: TabelaLandingConfig) -> pa.Schema:
    campos = []
    floats = set(config.colunas_float)
    for col in config.colunas:
        campos.append((col, pa.float64() if col in floats else pa.string()))
    if config.is_snapshot_mensal:
        campos.append(("mes_referencia", pa.string()))
    campos.append(("ingested_at", pa.string()))
    return pa.schema(campos)


def _bucket_s3() -> str:
    bucket = os.getenv("AWS_BUCKET_NAME") or AWS_BUCKET_NAME
    if not bucket or not bucket.strip():
        raise EnvironmentError("Variável AWS_BUCKET_NAME ausente no .env ou ambiente.")
    return bucket.strip()


def _require_bucket() -> str:
    return _bucket_s3()


def _s3_client():
    # boto3 usa a cadeia padrão de credenciais: perfil local, variáveis de ambiente,
    # role da máquina/runner etc. Nenhuma chave deve ser colocada no código.
    return boto3.client("s3")


def _codigo_erro_s3(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _s3_objeto_inexistente(exc: ClientError) -> bool:
    codigo = _codigo_erro_s3(exc)
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return codigo in {"404", "NoSuchKey", "NotFound", "NoSuchObject"} or status == 404


def enviar_para_s3(caminho_local: str, chave_s3: str) -> None:
    bucket = _require_bucket()
    if not os.path.isfile(caminho_local):
        raise FileNotFoundError(f"Arquivo local não encontrado para upload: {caminho_local}")
    print(f"  ☁️ Enviando para s3://{bucket}/{chave_s3} ...", end=" ", flush=True)
    _s3_client().upload_file(caminho_local, bucket, chave_s3)
    print("✅ Concluído!")


def arquivo_existe_s3(chave_s3: str) -> bool:
    bucket = _require_bucket()
    try:
        _s3_client().head_object(Bucket=bucket, Key=chave_s3)
        return True
    except ClientError as exc:
        if _s3_objeto_inexistente(exc):
            return False
        # 403, credenciais inválidas e outros erros não devem parecer "arquivo ausente".
        raise


def _url_download_base() -> str:
    token = os.getenv("TOKEN_SHARE") or TOKEN_SHARE
    if not token:
        raise EnvironmentError("Variável TOKEN_SHARE ausente no .env ou ambiente.")
    return f"https://arquivos.receitafederal.gov.br/public.php/dav/files/{token}"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=8),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def listar_arquivos_do_mes(diretorio_mes: str) -> List[str]:
    url_dir = f"{_url_download_base()}/{diretorio_mes}/"
    headers_propfind = {**HEADERS, "Depth": "1"}
    res = requests.request("PROPFIND", url_dir, headers=headers_propfind, timeout=30)
    if res.status_code not in (200, 207):
        res.raise_for_status()
        raise requests.HTTPError(f"Status HTTP inesperado na listagem: {res.status_code} ({url_dir})")

    texto = urllib.parse.unquote(res.text)
    encontrados = re.findall(r"/([^/\"<>]+\.zip)(?:[\"<>]|\s)", texto, flags=re.IGNORECASE)
    if not encontrados:
        # Fallback para respostas WebDAV nas quais o nome termina antes de uma quebra de linha.
        encontrados = re.findall(r"/([^/\"<>]+\.zip)", texto, flags=re.IGNORECASE)
    return sorted(set(encontrados), key=str.casefold)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=8),
    retry=retry_if_exception_type((requests.RequestException, zipfile.BadZipFile)),
    reraise=True,
)
def descarregar_zip(url: str, nome_zip: str, pasta_temp_tabela: str) -> str:
    """Baixa e verifica o ZIP, sem extrair seus membros no disco."""
    nome_seguro = os.path.basename(nome_zip)
    if not nome_seguro.lower().endswith(".zip"):
        raise ValueError(f"Nome de arquivo ZIP inválido: {nome_zip!r}")
    caminho_zip = os.path.join(pasta_temp_tabela, nome_seguro)
    if os.path.exists(caminho_zip):
        os.remove(caminho_zip)

    try:
        with requests.get(url, stream=True, headers=HEADERS, timeout=(30, 600)) as response:
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "").lower()
            if "text/html" in content_type:
                raise zipfile.BadZipFile(f"A Receita retornou HTML em vez de ZIP: {url}")
            total_size = int(response.headers.get("content-length", 0))
            with open(caminho_zip, "wb") as arquivo, tqdm(
                desc=nome_seguro,
                total=total_size or None,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
                leave=False,
            ) as barra:
                for bloco in response.iter_content(chunk_size=2 * 1024 * 1024):
                    if bloco:
                        barra.update(arquivo.write(bloco))

        with zipfile.ZipFile(caminho_zip, "r") as zip_ref:
            # Força a leitura da tabela central e garante que existe ao menos um CSV.
            membros_csv = [
                membro for membro in zip_ref.infolist()
                if not membro.is_dir() and PurePosixPath(membro.filename).suffix.lower() == ".csv"
            ]
            if not membros_csv:
                raise ValueError(f"ZIP sem membros CSV: {nome_seguro}")
        return caminho_zip
    except Exception:
        if os.path.exists(caminho_zip):
            os.remove(caminho_zip)
        raise


def descarregar_e_extrair(url: str, nome_zip: str, pasta_temp_tabela: str) -> List[str]:
    """Compatibilidade com chamadas antigas: extrai apenas CSVs, com validação de caminhos.

    O pipeline principal usa descarregar_zip e lê os CSVs diretamente do ZIP para
    evitar duplicar o consumo de disco da máquina local.
    """
    caminho_zip = descarregar_zip(url, nome_zip, pasta_temp_tabela)
    caminhos = []
    try:
        raiz = os.path.realpath(pasta_temp_tabela)
        with zipfile.ZipFile(caminho_zip, "r") as zip_ref:
            for membro in zip_ref.infolist():
                if membro.is_dir() or PurePosixPath(membro.filename).suffix.lower() != ".csv":
                    continue
                destino = os.path.realpath(os.path.join(pasta_temp_tabela, *PurePosixPath(membro.filename).parts))
                if os.path.commonpath([raiz, destino]) != raiz:
                    raise ValueError(f"Caminho inseguro dentro do ZIP: {membro.filename!r}")
                os.makedirs(os.path.dirname(destino), exist_ok=True)
                with zip_ref.open(membro, "r") as origem, open(destino, "wb") as saida:
                    while True:
                        bloco = origem.read(1024 * 1024)
                        if not bloco:
                            break
                        saida.write(bloco)
                caminhos.append(destino)
    finally:
        if os.path.exists(caminho_zip):
            os.remove(caminho_zip)
    return caminhos


def limpar_chunk_empresas_mei(chunk: pd.DataFrame) -> pd.DataFrame:
    """Remove CPF em claro da razão social de MEIs e converte capital_social.

    A regra de regex e a lógica de limpeza de MEI foram mantidas conforme a
    validação do projeto. Valores ausentes/inválidos de capital_social ficam como
    nulos em vez de serem artificialmente convertidos em zero.
    """
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
        bruto = chunk["capital_social"].astype("string").str.strip()
        ausente = bruto.isna() | bruto.str.lower().isin(["", "nan", "none", "null"])
        tem_virgula_e_ponto = bruto.str.contains(",", regex=False, na=False) & bruto.str.contains(".", regex=False, na=False)
        # Se o dado vier em formato brasileiro com separadores de milhar, remove
        # pontos somente nas células que também têm vírgula decimal.
        bruto.loc[tem_virgula_e_ponto] = bruto.loc[tem_virgula_e_ponto].str.replace(".", "", regex=False)
        normalizado = bruto.str.replace(",", ".", regex=False).where(~ausente, pd.NA)
        numerico = pd.to_numeric(normalizado, errors="coerce")
        invalidos = (~ausente) & numerico.isna()
        if invalidos.any():
            print(
                f"⚠️ capital_social: {int(invalidos.sum())} valor(es) não numérico(s) no chunk; "
                "serão preservados como nulos, não como zero."
            )
        chunk["capital_social"] = numerico
    return chunk


def _validar_schema_local_parquet(caminho: str, config: TabelaLandingConfig) -> int:
    schema_esperado = gerar_schema_arrow(config)
    metadata = pq.read_metadata(caminho)
    schema_real = pq.read_schema(caminho)
    _validar_schema(schema_real, schema_esperado, config.nome_tabela)
    linhas = metadata.num_rows
    if linhas < config.min_linhas_esperadas:
        raise ValueError(
            f"Data Quality Landing [{config.nome_tabela}]: volumetria abaixo do mínimo "
            f"({linhas:,} linhas < {config.min_linhas_esperadas:,})."
        )
    return linhas


def _validar_schema(schema_real: pa.Schema, schema_esperado: pa.Schema, nome_tabela: str) -> None:
    nomes_reais = schema_real.names
    nomes_esperados = schema_esperado.names
    faltantes = set(nomes_esperados) - set(nomes_reais)
    extras = set(nomes_reais) - set(nomes_esperados)
    if faltantes or extras:
        raise ValueError(
            f"Data Quality Landing [{nome_tabela}]: incompatibilidade de colunas; "
            f"ausentes={sorted(faltantes)}, inesperadas={sorted(extras)}."
        )
    tipos_incorretos = {
        nome: (str(schema_real.field(nome).type), str(schema_esperado.field(nome).type))
        for nome in nomes_esperados
        if schema_real.field(nome).type != schema_esperado.field(nome).type
    }
    if tipos_incorretos:
        raise ValueError(
            f"Data Quality Landing [{nome_tabela}]: tipos incompatíveis "
            f"(real, esperado)={tipos_incorretos}."
        )


def _timestamp_ingestao() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _extrair_zip_rfb_para_s3(
    config: TabelaLandingConfig,
    mes_alvo: str,
    chave_s3: str,
    pasta_temp_tabela: str,
) -> None:
    caminho_parquet_local = os.path.join(pasta_temp_tabela, f"{config.pasta_s3}_{mes_alvo}.parquet")
    schema_arrow = gerar_schema_arrow(config)
    writer = None
    linhas_gravadas = 0
    timestamp_ingestao = _timestamp_ingestao()

    arquivos_disponiveis = listar_arquivos_do_mes(mes_alvo)
    arquivos_tabela = [
        arquivo for arquivo in arquivos_disponiveis
        if arquivo.startswith(config.prefixo_zip) and arquivo.lower().endswith(".zip")
    ]
    if not arquivos_tabela:
        raise FileNotFoundError(
            f"Nenhum ZIP encontrado para {config.nome_tabela} "
            f"(prefixo={config.prefixo_zip!r}) no mês {mes_alvo}."
        )

    try:
        for nome_arquivo in arquivos_tabela:
            url = f"{_url_download_base()}/{mes_alvo}/{urllib.parse.quote(nome_arquivo, safe='')}"
            caminho_zip = descarregar_zip(url, nome_arquivo, pasta_temp_tabela)
            with zipfile.ZipFile(caminho_zip, "r") as zip_ref:
                membros_csv = [
                    membro for membro in zip_ref.infolist()
                    if not membro.is_dir() and PurePosixPath(membro.filename).suffix.lower() == ".csv"
                ]
                if not membros_csv:
                    raise ValueError(f"ZIP sem CSVs para {config.nome_tabela}: {nome_arquivo}")

                for membro in membros_csv:
                    print(f"  📄 Processando {membro.filename}... ")
                    with zip_ref.open(membro, "r") as arquivo_csv:
                        chunks = pd.read_csv(
                            arquivo_csv,
                            sep=";",
                            header=None,
                            encoding="iso-8859-1",
                            chunksize=TAMANHO_CHUNK_CSV,
                            dtype=str,
                            keep_default_na=False,
                            na_filter=False,
                            on_bad_lines="error",
                        )
                        for chunk in tqdm(chunks, desc=f"Convertendo {PurePosixPath(membro.filename).name}", leave=False):
                            if chunk.shape[1] != len(config.colunas):
                                raise ValueError(
                                    f"Layout CSV incompatível em {membro.filename}: "
                                    f"{chunk.shape[1]} campos recebidos; {len(config.colunas)} esperados."
                                )
                            chunk.columns = config.colunas

                            if config.transform_chunk_fn is not None:
                                chunk = config.transform_chunk_fn(chunk)
                            if not isinstance(chunk, pd.DataFrame):
                                raise TypeError("transform_chunk_fn deve retornar um pandas.DataFrame.")

                            faltantes = set(config.colunas) - set(chunk.columns)
                            extras = set(chunk.columns) - set(config.colunas)
                            if faltantes or extras:
                                raise ValueError(
                                    f"transform_chunk_fn alterou o contrato de colunas: "
                                    f"ausentes={sorted(faltantes)}, extras={sorted(extras)}."
                                )

                            if config.is_snapshot_mensal:
                                chunk["mes_referencia"] = mes_alvo
                            chunk["ingested_at"] = timestamp_ingestao

                            tabela_arrow = pa.Table.from_pandas(
                                chunk,
                                schema=schema_arrow,
                                preserve_index=False,
                                safe=True,
                            )
                            if writer is None:
                                writer = pq.ParquetWriter(
                                    caminho_parquet_local,
                                    schema_arrow,
                                    compression="snappy",
                                )
                            writer.write_table(tabela_arrow)
                            linhas_gravadas += tabela_arrow.num_rows
            os.remove(caminho_zip)
    finally:
        if writer is not None:
            writer.close()

    if not os.path.isfile(caminho_parquet_local):
        raise ValueError(f"Nenhuma linha foi processada para {config.nome_tabela} ({mes_alvo}); upload cancelado.")
    linhas = _validar_schema_local_parquet(caminho_parquet_local, config)
    if linhas != linhas_gravadas:
        raise ValueError(
            f"Contagem inconsistente em {config.nome_tabela} ({mes_alvo}): "
            f"escritas={linhas_gravadas}, Parquet={linhas}."
        )
    enviar_para_s3(caminho_parquet_local, chave_s3)


def _gravar_dataframe_estatico(
    df: pd.DataFrame,
    caminho_local: str,
    config: TabelaLandingConfig,
) -> None:
    schema = gerar_schema_arrow(config)
    faltantes = set(schema.names) - set(df.columns)
    extras = set(df.columns) - set(schema.names)
    if faltantes or extras:
        raise ValueError(
            f"Schema incompatível para {config.nome_tabela}: "
            f"ausentes={sorted(faltantes)}, extras={sorted(extras)}."
        )
    tabela = pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=True)
    pq.write_table(tabela, caminho_local, compression="snappy")
    _validar_schema_local_parquet(caminho_local, config)


def _extrair_fonte_estatica(
    config: TabelaLandingConfig,
    chave_s3: str,
    pasta_temp_tabela: str,
    mes_ini: Optional[str],
    mes_fim: Optional[str],
) -> None:
    caminho_local = os.path.join(pasta_temp_tabela, f"{config.pasta_s3}.parquet")
    timestamp = _timestamp_ingestao()

    if config.origem == "API_IBGE":
        print(f"🚀 Extraindo {config.nome_tabela} (API IBGE)...")
        response = requests.get(URL_IBGE_CNAE, headers=HEADERS, timeout=(10, 60))
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("Resposta inesperada da API de CNAE do IBGE: era esperada uma lista.")

        linhas = {}
        for item in payload:
            codigo = re.sub(r"\D", "", str(item.get("id", "")))
            if not codigo or codigo in linhas:
                continue
            classe = item.get("classe") or {}
            grupo = classe.get("grupo") or {}
            divisao = grupo.get("divisao") or {}
            secao = divisao.get("secao") or {}
            linhas[codigo] = {
                "codigo": codigo,
                "descricao": str(item.get("descricao") or "").upper().strip(),
                "secao": str(secao.get("id") or "").strip(),
                "nome_secao": str(secao.get("descricao") or "").upper().strip(),
                "divisao": str(divisao.get("id") or "").strip(),
                "nome_divisao": str(divisao.get("descricao") or "").upper().strip(),
                "grupo": str(grupo.get("id") or "").strip(),
                "nome_grupo": str(grupo.get("descricao") or "").upper().strip(),
                "classe": str(classe.get("id") or "").strip(),
                "nome_classe": str(classe.get("descricao") or "").upper().strip(),
                "ingested_at": timestamp,
            }
        df_ibge = pd.DataFrame(list(linhas.values()))
        _gravar_dataframe_estatico(df_ibge, caminho_local, config)

    elif config.origem == "BQ_CEPS":
        project_id = os.getenv("GCP_PROJECT_ID") or GCP_PROJECT_ID
        if not project_id:
            raise EnvironmentError("Variável GCP_PROJECT_ID ausente no .env ou ambiente.")
        print(f"🚀 Extraindo {config.nome_tabela} via BigQuery Sandbox ({project_id})...")
        query_ceps = """
        SELECT
            LPAD(REGEXP_REPLACE(TRIM(cep), r'[^0-9]', ''), 8, '0') AS cep,
            UPPER(TRIM(sigla_uf)) AS uf,
            UPPER(TRIM(nome_municipio)) AS cidade,
            UPPER(TRIM(localidade)) AS localidade,
            CAST(id_municipio AS STRING) AS codigo_ibge,
            ST_Y(centroide) AS latitude,
            ST_X(centroide) AS longitude
        FROM `basedosdados.br_bd_diretorios_brasil.cep`
        WHERE cep IS NOT NULL
          AND LENGTH(REGEXP_REPLACE(TRIM(cep), r'[^0-9]', '')) = 8
        """
        df_ceps = bd.read_sql(query=query_ceps, billing_project_id=project_id)
        df_ceps = df_ceps.drop_duplicates(subset=["cep"]).reset_index(drop=True)
        df_ceps["ingested_at"] = timestamp
        _gravar_dataframe_estatico(df_ceps, caminho_local, config)

    elif config.origem == "RFB_ZIP":
        mes_mais_recente = gerar_range_meses(mes_ini, mes_fim)[-1]
        print(
            f"🚀 Extraindo domínio estático {config.nome_tabela} usando "
            f"o snapshot mais recente ({mes_mais_recente})..."
        )
        _extrair_zip_rfb_para_s3(config, mes_mais_recente, chave_s3, pasta_temp_tabela)
        return

    else:  # Defesa adicional, mesmo após validar_config_landing.
        raise ValueError(f"Origem não implementada: {config.origem}")

    enviar_para_s3(caminho_local, chave_s3)


def extrair_tabela_landing(
    config: TabelaLandingConfig,
    mes_ini: Optional[str] = None,
    mes_fim: Optional[str] = None,
    force: bool = False,
) -> None:
    validar_config_landing(config)
    os.makedirs(PASTA_TEMP, exist_ok=True)
    prefixo_temp = re.sub(r"[^A-Za-z0-9_-]+", "_", config.pasta_s3)[:40] or "landing"

    # Diretório isolado por execução: evita que duas execuções da mesma tabela
    # removam ou sobrescrevam arquivos temporários uma da outra.
    with tempfile.TemporaryDirectory(prefix=f"{prefixo_temp}_", dir=PASTA_TEMP) as pasta_temp_tabela:
        if not config.is_snapshot_mensal:
            chave_s3 = f"{S3_PREFIX}/{config.pasta_s3}/dados.parquet"
            if not force and arquivo_existe_s3(chave_s3):
                print(
                    f"⏭️ [SKIP] {config.nome_tabela} já existe em "
                    f"s3://{_bucket_s3()}/{chave_s3}. Use --force para sobrescrever."
                )
                return
            _extrair_fonte_estatica(config, chave_s3, pasta_temp_tabela, mes_ini, mes_fim)
            return

        meses = gerar_range_meses(mes_ini, mes_fim)
        print(f"🚀 Iniciando Extração Landing de {config.nome_tabela} para os meses: {meses}")
        for mes in meses:
            chave_s3 = f"{S3_PREFIX}/{config.pasta_s3}/mes_referencia={mes}/dados.parquet"
            if not force and arquivo_existe_s3(chave_s3):
                print(
                    f"⏭️ [SKIP] {config.nome_tabela} ({mes}) já existe em "
                    f"s3://{_bucket_s3()}/{chave_s3}. Use --force para sobrescrever."
                )
                continue
            print(f"🔄 Baixando e convertendo {config.nome_tabela} (Mês: {mes})...")
            _extrair_zip_rfb_para_s3(config, mes, chave_s3, pasta_temp_tabela)


def _filesystem_s3_para_validacao(regiao: Optional[str]):
    """Cria filesystem Arrow usando a mesma cadeia de credenciais do boto3.

    PyArrow não aceita `profile_name` no construtor S3FileSystem em várias
    versões; quando há um perfil AWS configurado, resolvemos as credenciais pelo
    boto3 e as repassamos somente em memória para a leitura do footer.
    """
    kwargs = {}
    if regiao:
        kwargs["region"] = regiao

    perfil = os.getenv("AWS_PROFILE")
    sessao = boto3.Session(profile_name=perfil, region_name=regiao) if perfil else boto3.Session(region_name=regiao)
    credenciais = sessao.get_credentials()
    if credenciais is not None:
        congeladas = credenciais.get_frozen_credentials()
        kwargs["access_key"] = congeladas.access_key
        kwargs["secret_key"] = congeladas.secret_key
        if congeladas.token:
            kwargs["session_token"] = congeladas.token
    return pafs.S3FileSystem(**kwargs)


def _validar_parquet_s3(chave_s3: str, config: TabelaLandingConfig) -> None:
    bucket = _require_bucket()
    cliente = _s3_client()
    caminho_completo = f"s3://{bucket}/{chave_s3}"
    try:
        head = cliente.head_object(Bucket=bucket, Key=chave_s3)
    except ClientError as exc:
        if _s3_objeto_inexistente(exc):
            raise FileNotFoundError(f"ERRO DQ Landing: arquivo não encontrado em {caminho_completo}") from exc
        raise

    tamanho_bytes = head.get("ContentLength", 0)
    if tamanho_bytes == 0:
        raise ValueError(f"ERRO DQ Landing: arquivo vazio (0 bytes) em {caminho_completo}")

    regiao = cliente.meta.region_name or os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION")
    filesystem = _filesystem_s3_para_validacao(regiao)
    caminho_pa = f"{bucket}/{chave_s3}"
    try:
        metadata = pq.read_metadata(caminho_pa, filesystem=filesystem)
        schema_real = pq.read_schema(caminho_pa, filesystem=filesystem)
    except Exception as exc:
        raise ValueError(f"ERRO DQ Landing: Parquet ilegível ou corrompido em {caminho_completo}: {exc}") from exc

    _validar_schema(schema_real, gerar_schema_arrow(config), config.nome_tabela)
    num_linhas = metadata.num_rows
    if num_linhas < config.min_linhas_esperadas:
        raise ValueError(
            f"ERRO DQ Landing [{config.nome_tabela}]: volumetria abaixo do mínimo em {chave_s3} "
            f"({num_linhas:,} linhas < {config.min_linhas_esperadas:,} esperadas)."
        )
    print(f"  ✅ OK: {chave_s3} ({num_linhas:,} linhas | {tamanho_bytes / (1024 * 1024):.2f} MB)")


def testar_tabela_landing(
    config: TabelaLandingConfig,
    mes_ini: Optional[str] = None,
    mes_fim: Optional[str] = None,
) -> None:
    validar_config_landing(config, validar_dependencias_origem=False)
    print(f"🧪 Executando Data Quality S3 Landing: {config.nome_tabela}")
    if not config.is_snapshot_mensal:
        chave_s3 = f"{S3_PREFIX}/{config.pasta_s3}/dados.parquet"
        _validar_parquet_s3(chave_s3, config)
        return

    meses = gerar_range_meses(mes_ini, mes_fim)
    for mes in meses:
        chave_s3 = f"{S3_PREFIX}/{config.pasta_s3}/mes_referencia={mes}/dados.parquet"
        _validar_parquet_s3(chave_s3, config)


def executar_extract_landing_cli(config: TabelaLandingConfig) -> None:
    parser = argparse.ArgumentParser(description=f"Extração Landing S3: {config.nome_tabela}")
    parser.add_argument("--mes_referencia_inicial", default="", help="YYYY-MM inicial (vazio = mês anterior)")
    parser.add_argument("--mes_referencia_final", default="", help="YYYY-MM final (vazio = mês inicial)")
    parser.add_argument("--force", action="store_true", help="Força novo download mesmo se o Parquet já existir no S3")
    args, _ = parser.parse_known_args()
    extrair_tabela_landing(config, args.mes_referencia_inicial, args.mes_referencia_final, force=args.force)


def executar_dq_landing_cli(config: TabelaLandingConfig) -> None:
    parser = argparse.ArgumentParser(description=f"DQ Landing S3: {config.nome_tabela}")
    parser.add_argument("--mes_referencia_inicial", default="", help="YYYY-MM inicial (vazio = mês anterior)")
    parser.add_argument("--mes_referencia_final", default="", help="YYYY-MM final (vazio = mês inicial)")
    args, _ = parser.parse_known_args()
    testar_tabela_landing(config, args.mes_referencia_inicial, args.mes_referencia_final)
