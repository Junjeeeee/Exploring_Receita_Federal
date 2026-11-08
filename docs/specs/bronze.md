# Spec — Bronze

**Status:** implementado (PySpark no Databricks). CLIs de DQ quebradas (ver backlog).

## Responsabilidade
Ler a landing do S3 e gravar Delta em `tcc_receita.bronze.<tabela>` com metadados de CDC. **Sem regra de negócio.**

## Comportamento
- Leitura: `s3://$AWS_BUCKET_NAME (default lakehouse-receita)/landing/receita_federal/<pasta>/...` (`bronze_lib.py:61,201`).
- Escrita: `saveAsTable`, `mode("overwrite")`, `mergeSchema=true`; mensais com `replaceWhere mes_referencia='<m>'` (overwrite idempotente por partição); estáticas em overwrite total.
- Metadados adicionados: `pk` (JSON das colunas PK; socios usa `PK_SOCIOS_EXPR`), `change_hash` = `sha2(to_json(struct(atributos)), 256)`, `mes_referencia`, `bronze_processed_at`. Mantém `source_reference`; descarta `ingested_at`.
- `estabelecimentos.cnae_fiscal_secundaria` é normalizada (split/sort/join) antes do hash.
- Linhas com componente de PK nulo/vazio são **descartadas silenciosamente** (`_validar_componentes_pk`).
- Duplicatas exatas removidas; mesma `pk` com `change_hash` diferente ⇒ `ValueError`.
- Comentários UC aplicados por `aplicar_comentarios_uc`.

## Tabelas (configs em `src/bronze/configs/`)
empresas (pk `cnpj_basico`), estabelecimentos (`cnpj_basico,cnpj_ordem,cnpj_dv`), dados_simples (`cnpj_basico`),
socios (custom), e estáticas: cnaes, cnaes_ibge, paises, municipios, naturezas_juridicas, qualificacoes_socios, motivos_situacao.
Alias de pasta em `_obter_pasta_landing` (`bronze_lib.py:90`).

## DQ (`testar_tabela_bronze`, `data_quality.py`)
1. `colunas_esperadas` presentes.
2. `validar_schema` contra `CONTRATOS_SCHEMA` (só 5 tabelas).
3. `validar_volumetria`: count > 0 e queda ≤ `limite_queda_volumetria` (15%) vs média em `bronze.dq_metrics_log`.
4. Sem `change_hash` nulo, sem `pk` nula/vazia.

## Entradas CLI
`python src/bronze/ingest/<t>.py --mes_referencia_inicial YYYY-MM --mes_referencia_final YYYY-MM` (vazio = mês anterior).
DQ: `src/bronze/tests/<t>.py` (**quebrado**: chama `executar_ingestao_dq_cli`, que não existe; o correto é `executar_dq_cli`).
