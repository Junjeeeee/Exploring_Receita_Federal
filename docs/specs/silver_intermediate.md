# Spec — Silver Intermediate

**Status:** SQL escrito e revisado estaticamente ("Revisão 2", ver `src/silver_intermediate/readme.md`). **Nunca executado em cluster.**

## Responsabilidade
Converter snapshots mensais do bronze em **histórico SCD2** e derivar o **log de eventos de negócio**.

## Padrão SCD2 comum (empresas, estabelecimentos, dados_simples, socios)
- Janela `{{mes_referencia_inicial}}..{{mes_referencia_final}}` (vazio = mês anterior; fim default = início).
- 1 linha bronze por (pk, mês) via `ROW_NUMBER() ORDER BY bronze_processed_at DESC`.
- Limpeza: `NULLIF(TRIM())`, nomes upper-case sem pontuação, datas `yyyyMMdd` inválidas → NULL, booleanos S/N.
- Versões fora da janela preservadas (`historico_intacto`); nova versão só quando `change_hash IS DISTINCT FROM` anterior.
- `valid_since` = mês, `valid_until` = `LEAD` (exclusivo, NULL se atual), `is_current`.
- `MERGE INTO` com staging FULL OUTER JOIN: DELETE versões obsoletas, UPDATE **todos** os atributos, INSERT novas.
- Colunas comuns: `pk, mes_referencia, inserted_at, updated_at, change_hash, valid_since, valid_until, is_current`.

| SQL | Destino | Particularidades |
|---|---|---|
| `empresas.sql` | silver_intermediate.empresas | `capital_social` DOUBLE (vírgula→ponto) |
| `estabelecimentos.sql` | silver_intermediate.estabelecimentos | decodifica `situacao_cadastral_desc`; telefones formatados; 3 datas |
| `dados_simples.sql` | silver_intermediate.dados_simples | `opcao_pelo_simples_bool`, `opcao_pelo_mei_bool` (NULL se desconhecido) |
| `socios.sql` | silver_intermediate.socios (PARTITIONED BY is_current) | `vinculo_ativo`; lê janela **+ mês anterior**; `ASSERT_TRUE` de meses completos |
| `log_eventos_negocio.sql` | silver_intermediate.log_eventos_negocio (PARTITIONED BY mes_referencia) | `INSERT ... REPLACE WHERE`; **parâmetros precisam ser literais** |
| `migracao_socios.sql` | ALTER em socios existente | one-off; rodar `DESCRIBE` antes |

## Socios — saída do QSA
- Presente no mês M-1 e ausente em M ⇒ versão `vinculo_ativo = FALSE`, `valid_since = M`.
- `estados_inativos_legado` fecha vínculos ativos antigos com data = `mes_ini` (aproximação).
- Snapshot parcial **não** é detectado (só contagem de meses distintos) ⇒ risco de saídas falsas.

## Eventos
Ver `docs/glossario.md#eventos-de-negócio`.

## DQ (`tests/dq_*.sql`)
`SELECT ASSERT_TRUE(...)`; falha = erro `ERRO DQ [tabela]: ...`.
SCD2: não vazio, pk não nula, hash não nulo, `is_current ⇒ valid_until NULL`, intervalos válidos, ≤1 atual por pk.
Log: ≥1 evento na janela, pk/evento/valor_novo não nulos.

## Limitações aceitas (readme)
`change_hash` não é hash dos atributos normalizados; `capital_social` DOUBLE; `socios.pk` mascarada não é identidade global;
"EMPRESA NASCEU" = início de atividade; novos eventos exigem atualizar dimensões fechadas downstream.
