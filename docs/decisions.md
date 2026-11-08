# Decisões de design (ADR resumido)

Registre novas decisões no fim: `## D-NN — título` com **Data · Status** (proposta/aceita/revista/removida),
**Contexto**, **Decisão**, **Alternativas consideradas**, **Motivo**, **Consequências**. Ao rever uma decisão,
não apague: marque como revista e diga o que mudou e por quê.

## D-01 — Camadas com responsabilidade única
Landing = cópia fiel tipada como string; Bronze = snapshot + metadados de CDC, **sem regra de negócio**;
Silver intermediate = histórico SCD2 + eventos; Silver final = modelo dimensional; Gold = agregados para consumo.
**Em aberto (2026-11-08):** onde entram nós/arestas de grafo. Preferência do autor: não criar camada nova —
provavelmente na silver_final, ao lado do modelo dimensional (arestas como tabelas de relacionamento/ponte).

## D-02 — S3 como checkpoint da landing
Existência do objeto no S3 define o que já foi processado; `--force` sobrescreve. Unidade incremental = mês.
Orquestração mês-major (todas as tabelas de um mês, DQ, próximo mês).

## D-03 — Hash SHA-256 e pk técnica
`change_hash` SHA-256 sobre JSON dos atributos (não MD5). `pk` é técnica (JSON); nunca derivar CNPJ dela — juntar por `cnpj_basico`.

## D-04 — SCD2 meio-aberto e null-safe
`[valid_since, valid_until)`; comparações com `IS DISTINCT FROM`; ao mudar o hash, atualizar **todos** os atributos.

## D-05 — NULL não é FALSE (tributário)
Ausência de informação no Simples/MEI não gera evento de entrada/saída (evita eventos fantasmas).

## D-06 — Saída do QSA inferida por ausência
Sócio ausente num snapshot completo ⇒ versão `vinculo_ativo = FALSE`. Data de saída = mês da detecção.
Requer snapshot completo do mês anterior à janela; snapshot parcial gera saídas falsas.

## D-07 — LGPD
Nenhum CPF em claro; documentos mantidos como fornecidos (parcialmente mascarados), nunca desmascarados.
Mascaramento do nome MEI na landing é preservado (validado em dados reais).

## D-08 — Reprocessamento (revista em 2026-11-08)
Produção: reprocessar janela preserva versões passadas e futuras (`MERGE`, `REPLACE WHERE`).
**Revisão:** em desenvolvimento, destruir e reconstruir tabelas é aceitável e às vezes preferível (mais simples
e barato que depurar estado incremental). Decidir caso a caso.

## D-09 — Silver final: SKs via join e sentinelas
Fato obtém SKs por JOIN com as dimensões; `-1` não aplicável, `-2` CNAE fora do domínio IBGE.
Sobreposição de vigência é detectada por DQ, não corrigida silenciosamente.

## D-10 — "Grande setor" (revista em 2026-11-08)
Hoje é um `CASE` sobre a seção CNAE em `dim_cnae.sql` (A→Agropecuária, B–E→Indústria, F→Construção,
G→Comércio, H–U→Serviços); **não vem da API do IBGE**, que só traz seção/divisão/grupo/classe/subclasse.
**Revisão:** preferir classificação oficial (seção CNAE, 21 categorias). Manter o grande setor só se houver
fonte citável para o agrupamento (é a convenção usada pelo Sebrae; verificar) — senão, remover.

## D-11 — ~~DQ: não redesenhar~~ (removida em 2026-11-08)
Era regra para outro assistente. DQ pode ser melhorado/redesenhado. Convenção `ASSERT_TRUE` segue como padrão atual.

## D-12 — Modo protótipo dispensa spec prévia
2026-11-08 · aceita
**Contexto:** primeira execução no Databricks e dashboard para a reunião de 09/11; specs completas antes atrasariam.
**Decisão:** durante prototipação/testes, correções que destravam a execução vão direto ao código, cada uma com
commit próprio e item no backlog. Mudanças de arquitetura ou de regra de negócio continuam exigindo spec.
**Alternativas:** spec antes de tudo (lento demais para o prazo); sem registro nenhum (perde rastreabilidade
para o TCC e para a declaração de uso de IA).
**Motivo:** velocidade sem perder rastreabilidade.
**Consequências:** escrever spec retroativa ao sair do protótipo — pendentes: `job-databricks` (runners +
Job YAML) e `carga-inicial` (B9).
