# Decisões de design (ADR resumido)

Registre novas decisões no fim: `## D-NN — título` + contexto, decisão, consequência.

## D-01 — Camadas com responsabilidade única
Landing = cópia fiel tipada como string; Bronze = snapshot + metadados de CDC, **sem regra de negócio**;
Silver intermediate = histórico SCD2 + eventos; Silver final = modelo dimensional; Gold = agregados para consumo.

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

## D-08 — Reprocessamento não destrutivo
Reprocessar janela preserva versões passadas e futuras (`MERGE`, `REPLACE WHERE`), nada de delete em massa.

## D-09 — Silver final: SKs via join e sentinelas
Fato obtém SKs por JOIN com as dimensões; `-1` não aplicável, `-2` CNAE fora do domínio IBGE.
Sobreposição de vigência é detectada por DQ, não corrigida silenciosamente.

## D-10 — "Grande setor" é analítico
Agrupamento de seções CNAE próprio do projeto, não classificação oficial.

## D-11 — DQ: não redesenhar
Corrigir só problemas demonstrados; manter convenção `ASSERT_TRUE` nos SQL.
