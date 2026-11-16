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
Mascaramento do nome MEI na landing é preservado (validado em dados reais). Condição ampliada em D-16.

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

## D-16 — Máscara de CPF para todo empresário individual
2026-11-14 · aceita (autor, backlog D-a)
**Contexto:** a máscara só valia para `porte = '01' AND natureza = '2135'`; 2.816 razões de natureza 2135 com
porte 03/05 tinham CPF em claro (base real, 4 meses), e ~370 em outras naturezas tinham 11 dígitos seguidos.
**Decisão:** mascarar toda natureza 2135, qualquer porte. Regex e formato de saída inalterados. Outras
naturezas não são mascaradas.
**Alternativas:** mascarar 11 dígitos em qualquer natureza (altera nomes de PJ sem evidência de CPF);
manter a condição antiga (viola LGPD).
**Motivo:** empresário individual é pessoa física — o número é CPF; em PJ o número pode ser legítimo.
**Consequências:** vale só para extrações novas; reextrair `empresas` está pendente (sem autorização ainda).
Medir as ~370 após a reextração. Spec: `specs/features/2026-11-14-mascara-cpf-empresario-individual.md`.

## D-17 — Abertura e baixa no mesmo mês (primeira aparição já BAIXADA)
2026-11-14 · aceita (autor)
**Contexto:** no `fato_empresa_mes` a baixa exigia versão no mês anterior. Empresa que abre e fecha entre dois
snapshots aparece pela primeira vez já BAIXADA e contava só como abertura (base real: 7.818 / 7.386 / 10.533
empresas em jun/jul/ago/2023 aparecem já BAIXADA ou SUSPENSA). O log já registrava o evento de estado.
**Decisão:** primeira aparição pós-baseline já BAIXADA = abertura **e** baixa no mês de detecção (falência e
idade na baixa como em qualquer baixa; porte = o da própria versão). Idem INAPTA/SUSPENSA. Estoque de ativas,
entradas/saídas e `qtd_baixas_de_ativas` não mudam: a empresa nunca esteve ativa num snapshot.
**Alternativas:** contar só a abertura (regra antiga; "não faz sentido" para o autor); não contar nem abertura.
**Consequências:** ~8 mil baixas a mais por mês na base real; aberturas por baixa de agosto cai de 2,09 para
~1,98; taxa de fechamento por mil não muda. O dashboard (`baixas_motivo`) passa a contar a primeira aparição e
fecha com o fato (DQ 2b). Spec: `specs/features/2026-11-14-semantica-eventos-e-fluxos.md` §2.4.
**Complemento D-17b (2026-11-14, autor):** porte dessas baixas pelas datas do Simples, não pela natureza
jurídica (empresário individual pode ser ME): `MEI` se opção pelo MEI ≤ data da baixa e exclusão nula ou ≥ data
da baixa (tolerância 0 dia; amostra: exclusão = data da baixa em 100%). Base real: ~98% das marcadas ME tinham
as duas datas. % MEI das baixas volta ao patamar anterior (amostra: ~62–68% → ~67–71%).
