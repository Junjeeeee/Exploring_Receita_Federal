# Spec — Bronze

**Status:** implementado (PySpark no Databricks). Otimização da fase 1b (P1–P3 de
[`features/2026-11-16-otimizacao-bronze.md`](features/2026-11-16-otimizacao-bronze.md)): **preparada, não executada no
Databricks**.

## Responsabilidade
Ler a landing do S3 e gravar Delta em `tcc_receita.bronze.<tabela>` com metadados de CDC. **Sem regra de negócio.**

## Comportamento (`src/bronze/bronze_lib.py::ingerir_tabela_bronze`)
- Leitura: `s3://$AWS_BUCKET_NAME (default lakehouse-receita)/landing/receita_federal/<pasta>/mes_referencia=<m>/`
  (mensais, um mês por volta do laço) ou `.../<pasta>/dados.parquet` (estáticas) (`obter_bucket_landing`, `ingerir_tabela_bronze`).
- Escrita: `saveAsTable`, `mode("overwrite")`, `mergeSchema=true`; mensais com `replaceWhere mes_referencia='<m>'` +
  `partitionBy("mes_referencia")` (overwrite idempotente por partição, um commit por mês); estáticas em overwrite total.
- Metadados adicionados: `pk` (JSON das colunas PK; socios usa `PK_SOCIOS_EXPR`), `change_hash` = `sha2(to_json(struct(atributos)), 256)`, `mes_referencia`, `bronze_processed_at`. Mantém `source_reference`; descarta `ingested_at`.
- `estabelecimentos.cnae_fiscal_secundaria` é normalizada (split/sort/join) antes do hash.
- Linhas com componente de PK nulo/vazio são **descartadas silenciosamente** (`_validar_componentes_pk`).
- Cópias exatas removidas (`_remover_copias_exatas` = `dropDuplicates()` em todas as colunas). Mesma `pk` com
  `change_hash` diferente é **mantida** (a Receita publica algumas; a silver desempata, B16). A contagem dessas pks
  repetidas é um **aviso da DQ**, não da ingestão (P1).
- Optimized write (P2): parâmetro `modo_optimize_write` de `ingerir_tabela_bronze` ou variável de ambiente
  `BRONZE_DESLIGAR_OPTIMIZE_WRITE` (`src/bronze/optimize_write.py`), só nas escritas mensais:
  `nao` (padrão — vale a conf do cluster, `optimizeWrite` ligado), `writer` (opção `optimizeWrite=false` no
  DataFrameWriter) ou `sessao` (`spark.databricks.delta.optimizeWrite.enabled=false` só durante a escrita, restaurado em
  seguida). Qual dos dois funciona no DBR 15.4 ainda não foi medido (Q3 da spec da fase 1b).
- Comentários UC (`aplicar_comentarios_uc`), uma vez por tabela após o laço: compara com o que está gravado
  (descrição da tabela e `metadata["comment"]` de cada coluna) e só aplica o que difere; a partir da 2ª execução, 0
  commits. Colunas do dicionário que não existem na tabela são ignoradas.

## Tabelas (configs em `src/bronze/configs/`)
empresas (pk `cnpj_basico`), estabelecimentos (`cnpj_basico,cnpj_ordem,cnpj_dv`), dados_simples (`cnpj_basico`),
socios (custom), e estáticas: cnaes, cnaes_ibge, paises, municipios, naturezas_juridicas, qualificacoes_socios, motivos_situacao, ceps.
Alias de pasta em `_obter_pasta_landing`.

## DQ (`testar_tabela_bronze`, `data_quality.py`)
Uma passada por tabela (P3): uma agregação sobre os meses da execução (lê `mes_referencia`, `change_hash`, `pk`,
agrupando por mês e pk) + uma leitura do histórico de volumetria. Avaliação mês a mês, em ordem:
1. `colunas_esperadas` presentes (metadados; uma vez).
2. `validar_schema` contra `CONTRATOS_SCHEMA` (só 5 tabelas; uma vez).
3. Volumetria: count > 0 e queda ≤ `limite_queda_volumetria` (15%) vs média dos até 3 meses **anteriores** em
   `bronze.dq_metrics_log` (última linha por mês); o mês aprovado é gravado no log (`INSERT` por mês) e entra na média
   dos seguintes.
4. Sem `change_hash` nulo, sem `pk` nula/vazia.
5. Aviso (não falha): nº de pks repetidas com conteúdo divergente no mês.

## Entradas CLI
- Job: `src/jobs/run_bronze.py --modo ingestao|dq|ambos --tabelas <lista|MENSAIS|ESTATICAS|ALL>
  --mes_referencia_inicial YYYY-MM --mes_referencia_final YYYY-MM` (vazio = mês anterior).
- Por tabela: `src/bronze/ingest/<t>.py` e `src/bronze/tests/<t>.py` (`executar_ingestao_cli` / `executar_dq_cli`).
