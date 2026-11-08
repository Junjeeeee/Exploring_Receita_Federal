# Feature: <nome>

> Copie para `docs/specs/features/<yyyy-mm-dd>-<nome>.md`. Preencha 1–3 antes de codar; aprove com o usuário.

**Status:** rascunho | aprovado | em implementação | concluído
**Camadas afetadas:** landing / bronze / silver_intermediate / silver_final / gold
**Branch:**

## 1. Requisito
- Problema / pergunta de negócio:
- Quem consome (dashboard, ML, análise):
- Fora de escopo:

## 2. Design
- Tabelas/arquivos novos ou alterados (com grão e chaves):
- Fontes e dependências (atualizar DAG em `docs/architecture.md` se mudar):
- Regras de negócio (referenciar/atualizar `docs/glossario.md`):
- Estratégia incremental / reprocessamento (janela, `REPLACE WHERE`, `MERGE`...):
- Impacto LGPD:
- Impacto no TCC (capítulo/seção a atualizar em `/home/Junj/Documents/TCC`, ver `docs/tcc.md`):
- Alternativas consideradas → decisão (registrar em `docs/decisions.md` se relevante):

## 3. Tarefas
- [ ] ...
- [ ] DQ: `tests/dq_<tabela>.sql` (ou DQ Python)
- [ ] Atualizar spec da camada e backlog

## 4. Critérios de aceite
- [ ] Ex.: DQ passa na janela 2023-05..2023-08
- [ ] Ex.: contagem X bate com Y
- [ ] Ex.: query de validação `...` retorna 0 linhas

## 5. Notas de execução
(o que foi rodado, onde, resultados, surpresas)
