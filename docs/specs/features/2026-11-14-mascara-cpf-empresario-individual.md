# Feature: máscara de CPF para todo empresário individual (D-a)

**Status:** código concluído; **reextração de `empresas` na landing não executada** (por decisão do autor)
**Camadas afetadas:** landing
**Branch:** `worktree-dashboard-segunda`

## 1. Requisito
- Problema: a landing (`limpar_chunk_empresas_mei` em `src/landing/landing_lib.py`) só mascarava o CPF da
  razão social quando `porte_empresa = '01' AND natureza_juridica = '2135'`. Medido na base real (4 meses):
  **2.816** razões sociais de natureza 2135 com porte 03/05 têm 11 dígitos seguidos em claro, e **~370** em
  outras naturezas (podem não ser CPF). Viola a regra LGPD "nunca CPF em claro".
- Quem consome: toda a cadeia (bronze → gold); o dashboard já esconde a razão social de empresário individual
  (`exibir_razao_social = FALSE`), mas o dado bruto fica no lake.
- Fora de escopo: reextrair `empresas` (autor decidiu não reprocessar agora); mudar a regex ou o formato de
  saída; mascarar razão social de pessoa jurídica.

## 2. Design
- Condição **antes**: `porte == "01" AND natureza == "2135"` (comparação de string exata).
- Condição **depois**: `natureza == 2135`, qualquer porte (inclusive nulo). É superconjunto da anterior:
  nada que era mascarado deixa de ser.
- Robustez ao tipo: a landing lê o CSV com `dtype=str` (natureza chega como `"2135"`), mas a comparação passa
  a ser numérica (`pd.to_numeric` após `strip`), aceitando `"2135"`, `" 2135 "`, `"02135"`, `2135`, `2135.0`.
  Nulo/não numérico → linha não tocada.
- Regex (`REGEX_CPF_SUJO`) e formato de saída (`"NN.NNN.NNN NOME"`) **inalterados**: validados em dados reais.
  Efeito colateral aceito: toda razão social 2135 de porte 03/05 passa a ter o CNPJ básico formatado como
  prefixo, como já acontecia com porte 01 — formato uniforme para todo empresário individual.
- Outras naturezas (~370): **não mascarar**. São pessoas jurídicas; 11 dígitos podem ser número legítimo
  (registro, telefone, código). Mascarar às cegas alteraria nomes de PJ sem evidência de que é CPF. Fica como
  ponto a medir no backlog (classificar por natureza e checar dígito verificador de CPF antes de decidir).
- Nome da função mantido (`limpar_chunk_empresas_mei`): "mei" é histórico; docstring atualizada.
- Reprocessamento: só vale para extrações futuras. Os Parquets já na landing (e bronze/silver derivadas)
  continuam com as 2.816 razões em claro até reextrair `empresas` (`--force`) e reprocessar a cadeia.
- Impacto LGPD: reduz exposição de CPF na origem. Gold continua escondendo via `exibir_razao_social`.
- Decisão: D-16 em `docs/decisions.md`.

## 3. Tarefas
- [x] Ampliar a condição em `landing_lib.py` (`_eh_empresario_individual`)
- [x] Testes em `tests/test_landing_mei.py`: 2135 em todo porte; natureza str/int/float; natureza nula,
      inválida ou vizinha não tocada; PJ com 11 dígitos não tocada; chunk misto
- [x] Atualizar `docs/backlog.md`, `docs/specs/landing.md`, `docs/decisions.md`, `CLAUDE.md`
- [ ] Reextrair `empresas` na landing e reprocessar bronze → gold (**depende de autorização**)

## 4. Critérios de aceite
- [x] `pytest tests/` verde, com os casos antigos inalterados
- [ ] Após reextração: Q8 de `critica.md` restrita a natureza 2135 retorna 0
- [ ] Após reextração: contagem de 11 dígitos em outras naturezas medida e registrada (decidir se vira regra)

## 5. Notas de execução
2026-11-14: só código e testes (27 testes passando). Nada executado na AWS/Databricks.
