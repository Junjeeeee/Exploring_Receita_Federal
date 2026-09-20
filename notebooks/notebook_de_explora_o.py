# %% [markdown]
# # Exploração Segura de Dados da Receita Federal com DuckDB
# Executa consultas analíticas diretamente nos arquivos Parquet com baixo consumo de RAM.

# %%
import os
from pathlib import Path
!poetry add duckdb
import duckdb

# Inicializar conexão in-memory
con = duckdb.connect(database=":memory:")

# Limitar o uso de RAM para proteger o sistema operacional
# Ajuste conforme sua folga (ex: '4GB' ou '6GB' garante que o OOM não atue)
con.execute("SET max_memory = '4GB';")
con.execute("SET preserve_insertion_order = false;")

print("DuckDB configurado com limite seguro de memória.")

# %% [markdown]
# ### 1. Verificação dos Arquivos Gerados

# %%
# Ancoragem robusta: pega o diretório pai da pasta 'notebooks' (ou seja, a raiz do repo)
ROOT_DIR = Path(__file__).resolve().parent.parent if "__file__" in locals() else Path.cwd().parent
PASTA_PARQUET = ROOT_DIR / "tmp_receita"
MES_TESTE = "2024-01"

path_empresa = str(PASTA_PARQUET / f"empresa_{MES_TESTE}.parquet")
path_estabele = str(PASTA_PARQUET / f"estabele_{MES_TESTE}.parquet")
path_socio = str(PASTA_PARQUET / f"socio_{MES_TESTE}.parquet")
path_simples = str(PASTA_PARQUET / f"simples_{MES_TESTE}.parquet")

print(f"Diretório raiz detectado: {ROOT_DIR}")
print(f"Buscando arquivos em: {PASTA_PARQUET}")
print("-" * 50)

for p in [path_empresa, path_estabele, path_socio, path_simples]:
    status = "✅ Encontrado" if os.path.exists(p) else "❌ Não encontrado"
    print(f"{status}: {os.path.basename(p)}")

# %% [markdown]
# ### 2. Metadados e Contagem Total de Linhas
# O DuckDB lê apenas os cabeçalhos Parquet para contar linhas quase instantaneamente.

# %%
if os.path.exists(path_empresa):
    query_count = f"""
    SELECT 
        count(*) as total_linhas,
        count(DISTINCT cnpj_basico) as cnpjs_unicos
    FROM read_parquet('{path_empresa}')
    """
    resumo_empresa = con.execute(query_count).df()
    print("Métricas da Tabela EMPRESA:")
    print(resumo_empresa)

# %% [markdown]
# ### 3. Validação do Mascaramento do MEI
# Consulta registros para confirmar se o CPF foi substituído pelo CNPJ básico na Razão Social.

# %%
if os.path.exists(path_empresa):
    query_mei = f"""
    SELECT 
        cnpj_basico,
        razao_social,
        natureza_juridica,
        porte_empresa
    FROM read_parquet('{path_empresa}')
    WHERE porte_empresa = '05' 
      AND razao_social ~ '[0-9]{{8}}' -- Removido o '^', agora busca em qualquer lugar
    LIMIT 10
    """
    amostra_mei = con.execute(query_mei).df()
    print("Amostra de Razões Sociais mascaradas (MEI):")
    print(amostra_mei)

# %% [markdown]
# ### 4. Distribuição por Porte de Empresa
# Agregação pesada executada via DuckDB sem estourar memória.

# %%
if os.path.exists(path_empresa):
    query_porte = f"""
    SELECT 
        porte_empresa,
        count(*) as total_empresas,
        round(count(*) * 100.0 / sum(count(*)) over (), 2) as percentual
    FROM read_parquet('{path_empresa}')
    GROUP BY porte_empresa
    ORDER BY total_empresas DESC
    """
    df_porte = con.execute(query_porte).df()
    print("Distribuição por Porte:")
    print(df_porte)

# %% [markdown]
# ### 5. Amostra de Estabelecimentos Ativos
# Leitura com projeção de colunas específicas (evita ler as 30 colunas do disco).

# %%
if os.path.exists(path_estabele):
    query_estab = f"""
    SELECT 
        cnpj_basico,
        cnpj_ordem,
        cnpj_dv,
        nome_fantasia,
        situacao_cadastral,
        uf,
        municipio,
        data_inicio_atividade
    FROM read_parquet('{path_estabele}')
    WHERE situacao_cadastral = '02' -- Ativa
      AND uf IS NOT NULL
    LIMIT 10
    """
    amostra_estab = con.execute(query_estab).df()
    print("Amostra Estabelecimentos Ativos:")
    print(amostra_estab)

# %% [markdown]
# ### 6. Cruzamento (JOIN) Seguro em Disco
# Junta Empresa e Estabelecimento trazendo apenas o topo por estado.

# %%
if os.path.exists(path_empresa) and os.path.exists(path_estabele):
    query_join = f"""
    SELECT 
        e.uf,
        count(*) as total_matrizes_ativas
    FROM read_parquet('{path_estabele}') e
    JOIN read_parquet('{path_empresa}') emp ON e.cnpj_basico = emp.cnpj_basico
    WHERE e.identificador_matriz_filial = '1' -- Matriz
      AND e.situacao_cadastral = '02'        -- Ativa
      AND e.uf != ''
    GROUP BY e.uf
    ORDER BY total_matrizes_ativas DESC
    LIMIT 10
    """
    top_ufs = con.execute(query_join).df()
    print("Top 10 UFs com Matrizes Ativas:")
    print(top_ufs)


    # %% [markdown]
# ### 7. Explorando as Tabelas de Domínio
# Verificando a carga dos Municípios e CNAEs (Atividades Econômicas).

# %%
path_municipios = str(PASTA_PARQUET / f"municipios_{MES_TESTE}.parquet")
path_cnaes = str(PASTA_PARQUET / f"cnaes_{MES_TESTE}.parquet")

if os.path.exists(path_municipios):
    query_mun = f"""
    SELECT codigo, descricao
    FROM read_parquet('{path_municipios}')
    LIMIT 5
    """
    df_mun = con.execute(query_mun).df()
    print("Amostra Tabela MUNICÍPIOS:")
    print(df_mun)

if os.path.exists(path_cnaes):
    query_cnae = f"""
    SELECT codigo, descricao
    FROM read_parquet('{path_cnaes}')
    LIMIT 5
    """
    df_cnae = con.execute(query_cnae).df()
    print("\nAmostra Tabela CNAES:")
    print(df_cnae)

# %% [markdown]
# ### 8. JOIN Completo: Estabelecimentos + Municípios + CNAEs
# Trazendo o nome real do município e a descrição da atividade principal do estabelecimento em tempo recorde.

# %%
if os.path.exists(path_estabele) and os.path.exists(path_municipios) and os.path.exists(path_cnaes):
    query_completa = f"""
    SELECT 
        e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj_completo,
        e.nome_fantasia,
        m.descricao AS nome_municipio,
        e.uf,
        c.descricao AS atividade_principal
    FROM read_parquet('{path_estabele}') e
    LEFT JOIN read_parquet('{path_municipios}') m ON e.municipio = m.codigo
    LEFT JOIN read_parquet('{path_cnaes}') c ON e.cnae_fiscal_principal = c.codigo
    WHERE e.situacao_cadastral = '02' -- Ativa
      AND e.uf = 'RJ'                 -- Apenas Rio de Janeiro
    LIMIT 10
    """
    df_completo = con.execute(query_completa).df()
    print("Estabelecimentos Enriquecidos (JOIN com Domínios):")
    print(df_completo)