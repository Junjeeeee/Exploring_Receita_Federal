# Spec — Silver Final (modelo dimensional)

**Status:** SQL escrito; não há evidência de execução em cluster.

## Convenções
- `sk = XXHASH64(nk) & Long.MAX`; ghost row `-1`; `dim_cnae` também `-2`.
- Todas com `data_carga_etl`. Dimensões: `INSERT OVERWRITE` (rebuild); fato: `REPLACE WHERE` por janela.
- Granularidade preservada no fato via `entidade_tipo` + `pk_entidade_origem`.
- Sobreposição de períodos é **detectada por DQ**, nunca resolvida silenciosamente.

## Tabelas

| Tabela | Tipo | Grão / chave | Fonte | Regras |
|---|---|---|---|---|
| dim_cnae | Dim SCD1 | subclasse CNAE 7 dígitos (`nk_cnae`) | bronze.cnaes_ibge | hierarquia seção→classe; `grande_setor_economico` |
| dim_grupo_cnae | Dim SCD1 | (primário, lista secundários ordenada) | si.estabelecimentos (matriz) | `SEM_PRIMARIO`/`SEM_SECUNDARIO` |
| ponte_cnae | Bridge N:N | (`sk_grupo_cnae`, `sk_cnae`) | dim_grupo_cnae, dim_cnae | `cnae_principal` flag; código ausente → `sk_cnae=-2` |
| dim_tempo | Dim calendário | mês (`nk_tempo`=YYYYMM) | log (mín.) | de min(2020-01, log) até max(hoje+12m, log) |
| dim_empresa | Dim **SCD2** | `cnpj_raiz` × fatia de vigência `[inicio_vigencia, fim_vigencia)` | si.{estab matriz, empresas, simples} + bronze domínios + ceps + dim_grupo_cnae | interseção de intervalos; CEP tem prioridade p/ localização; `localidade_analitica`; porte cadastral/analítico |
| dim_mudanca_estado | Junk dim | (entidade_tipo, evento, valor_anterior, valor_posterior, info) | log | `detalhe_mudanca_estado = "ant -> post"` |
| fato_mudanca_estado | Fato transacional, partição mês | 1 linha por evento | log + mapa pk→cnpj + dims | `sk_empresa_anterior/posterior`, `sk_grupo_cnae_*`, `sk_tempo`; EMPRESA NASCEU ⇒ anterior = -1 |

Ordem: `dim_cnae → dim_grupo_cnae → ponte_cnae`; `dim_grupo_cnae → dim_empresa`; `dim_tempo, dim_mudanca_estado, dim_empresa → fato_mudanca_estado`.

## DQ (`tests/dq_*.sql`)
Volumetria, exatamente 1 ghost row, chaves nulas, colisão de SK, órfãos (`LEFT ANTI JOIN`).
`dim_empresa`: sem sobreposição, `registro_ativo` coerente, ≤1 versão ativa por `cnpj_raiz`, enriquecimento CEP.
Sem DQ para `dim_tempo`. Arquivo com typo: `dq_dim_mucanca_estado.sql`.
