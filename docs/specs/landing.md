# Spec — Landing

**Status:** implementado e executado. WIP não commitado em `landing_lib.py` / `orquestrador.py` (ver fim).

## Responsabilidade
Baixar fontes brutas e gravar **Parquet tipado como string** no S3, uma partição por mês para tabelas mensais.
Sem regra de negócio, exceto o **mascaramento de CPF de MEI** (LGPD).

## Entradas / saídas
- Entrada: Receita (ZIP via WebDAV), IBGE API, BigQuery (CEPs).
- Saída: `s3://$AWS_BUCKET_NAME/landing/receita_federal/<pasta_s3>/mes_referencia=YYYY-MM/dados.parquet` (mensal)
  ou `.../<pasta_s3>/dados.parquet` (estática).
- Colunas adicionadas: `mes_referencia` (só mensais), `source_reference`, `ingested_at` (UTC).
- Tipos: tudo `string`, exceto `colunas_float` (`float64`).

## Tabelas

| Tabela | pasta_s3 | Origem | Mensal | min_linhas | Observação |
|---|---|---|---|---|---|
| empresas | empresa | RFB_ZIP `Empresas` | ✔ | 40M | `capital_social` float; mascaramento MEI |
| estabelecimentos | estabele | RFB_ZIP `Estabelecimentos` | ✔ | 45M | 30 colunas |
| dados_simples | simples | RFB_ZIP `Simples` | ✔ | 30M | |
| socios | socio | RFB_ZIP `Socios` | ✔ | 20M | |
| municipios | municipios | RFB_ZIP | ✘ | 500 | ZIP do último mês da janela |
| paises | paises | RFB_ZIP | ✘ | 200 | |
| naturezas_juridicas | naturezas_juridicas | RFB_ZIP `Naturezas` | ✘ | 70 | |
| qualificacoes_socios | qualificacoes_socios | RFB_ZIP `Qualificacoes` | ✘ | 40 | |
| motivos_situacao | motivos_situacao | RFB_ZIP `Motivos` | ✘ | 50 | |
| cnaes_ibge | cnaes_ibge | API_IBGE | ✘ | 1.300 | upper-case, dedup por `codigo` |
| ceps | ceps | BQ_CEPS | ✘ | 900k | CEP normalizado 8 dígitos, lat/long float |

## Contrato para nova fonte
1. Criar `src/landing/configs/<t>.py` com `CONFIG_LANDING = TabelaLandingConfig(nome_tabela, pasta_s3, colunas, origem, prefixo_zip, is_snapshot_mensal, colunas_float, min_linhas_esperadas, transform_chunk_fn)`.
2. Registrar em `TODAS_CONFIGS` (`src/landing/orquestrador.py`).
3. (Opcional) stubs `extract/<t>.py` e `dq/<t>.py`.
4. Nova origem ⇒ novo ramo em `_extrair_fonte_estatica` + `origens_validas`.
`validar_config_landing` proíbe colunas reservadas (`mes_referencia`, `source_reference`, `ingested_at`) e snapshot mensal fora de RFB_ZIP.

## Fluxo RFB_ZIP (`_extrair_zip_rfb_para_s3`)
PROPFIND lista ZIPs do mês → filtra por `prefixo_zip` → `descarregar_zip` (3 retries, valida ZIP) →
lê CSVs internos (`;`, iso-8859-1, sem header, chunks de 250k; **não filtrar por `.csv`**, extensões são `.EMPRECSV` etc.)
→ `transform_chunk_fn` → `ParquetWriter` → valida schema / contagem → upload.

## Idempotência
O próprio S3 é o checkpoint: objeto existente é pulado (exceto com `--force`). Estáticas nunca se atualizam sem `--force`.
`checkpoint_estado.json` e `last_processed_month.txt` são **legado** (não usados).

## DQ (`testar_tabela_landing`)
Objeto existe e não vazio; footer Parquet legível; schema exato; `num_rows >= min_linhas_esperadas`. Sem checagem de valores.

## WIP (não commitado)
- `descarregar_zip`: rejeita status ≠ 200 e HTML, confere Content-Length, diagnóstico hex de `BadZipFile` com cópia para `tmp_receita/diagnostico_zip/`, log de retry sem token.
- Arquivos de **correção da RFB** (ex. `Estabelecimentos6-RF0819000032530.zip`) substituem o ZIP base; >1 correção por pacote ⇒ `ValueError`.
- Orquestrador: loop mês-major, estáticas depois, validação estrita de nomes de tabela.
