# Glossário de negócio

| Termo | Definição no projeto | Onde |
|---|---|---|
| **Empresa** | `cnpj_basico` / `cnpj_raiz` (8 primeiros dígitos). | todas as camadas |
| **Estabelecimento** | CNPJ completo (`cnpj_basico`+`cnpj_ordem`+`cnpj_dv`). Matriz quando `identificador_matriz_filial = '1'`. | estabelecimentos |
| **Situação cadastral** | 01=NULA, 02=ATIVA, 03=SUSPENSA, 04=INAPTA, 08=BAIXADA (outros → NULL). | `si/estabelecimentos.sql` |
| **Porte cadastral** | 01=ME, 03=EPP, 05=DEMAIS. | `dim_empresa` |
| **Porte analítico** | `enquadramento_porte_analitico`: MEI (prioridade) > ME > EPP > DEMAIS > NAO IDENTIFICADO. | `dim_empresa` |
| **MEI (landing)** | porte "01" + natureza "2135" → CPF removido da razão social. | `landing_lib.limpar_chunk_empresas_mei` |
| **Empresa ativa** | Matriz com situação ATIVA. `ativa_inicio_mes` = ATIVA no mês anterior (população em risco); `ativa_fim_mes` = ATIVA no fim do mês. | `gold/fato_empresa_mes.sql` |
| **Encerramento** | Situação BAIXADA / INAPTA / NULA (SUSPENSA **não** encerra). | `fato_empresa_mes`, `ds_coorte_sobrevivencia` |
| **Falência** | Categoria `FECHAMENTO POR FALENCIA` (e `EXTINCAO POR LIQUIDACAO` como evento de fim) derivada por regex do motivo. | `obt_dashboard_eventos` |
| **Coorte** | Mês/ano de nascimento da empresa; sobrevivência com censura à direita (Kaplan-Meier / Cox). | `d_coorte_sobrevivencia.sql` → view `ds_coorte_sobrevivencia` |
| **Grande setor econômico** | Seção CNAE: A=AGROPECUARIA, B–E=INDUSTRIA, F=CONSTRUCAO, G=COMERCIO, H–U=SERVICOS, outros=OUTROS. Agrupamento **analítico** (CASE no código), não vem do IBGE — em revisão (D-10). | `dim_cnae` |
| **Grupo CNAE** | Combinação (CNAE principal, lista ordenada de secundários) da matriz. | `dim_grupo_cnae`, `ponte_cnae` |
| **Sentinelas** | `-1` = não aplicável / ghost row; `-2` = CNAE informado mas inexistente no domínio IBGE. | silver_final |
| **pk** | Chave técnica JSON das colunas PK (socios: `cnpj_basico-cnpj_cpf_socio[-nome]`). Não é identidade global. | bronze |
| **Identidade de sócio PF** | Não existe chave confiável: CPF mascarado (6 dígitos) + nome colide e muda — troca de faixa etária pode separar duas pessoas tidas como uma; nome muda (casamento, gênero, vontade). Limitação **a medir** antes de grafo/grupos. | — |
| **change_hash** | SHA-256 do JSON dos atributos de negócio (bronze). | bronze → SCD2 |
| **vinculo_ativo** | Sócio presente no snapshot do mês. Ausência num snapshot completo ⇒ versão `FALSE` (saída do QSA). | `si/socios.sql` |

> Definições podem mudar após a exploração dos dados (etapa exploratória local).

## Eventos de negócio (`silver_intermediate.log_eventos_negocio`)

Transições = self-join de versões SCD2 consecutivas (`antigo.valid_until = novo.valid_since`); mês = `novo.valid_since`.

| Evento | entidade_tipo | Regra |
|---|---|---|
| EMPRESA NASCEU | EMPRESA | `data_inicio_atividade` da matriz (início de atividade, **não** constituição legal) |
| MUDOU NATUREZA JURIDICA | EMPRESA | `natureza_juridica` mudou |
| MUDOU ESTADO CADASTRAL | ESTABELECIMENTO | situação ou motivo mudou; `informacao_adicional` = descrição do motivo |
| MUDOU CNAE PRINCIPAL | ESTABELECIMENTO | `cnae_fiscal_principal` mudou |
| MUDOU LISTA CNAE SECUNDARIO | ESTABELECIMENTO | lista ordenada mudou |
| ENTROU / SAIU DO MEI | TRIBUTARIO | `opcao_pelo_mei_bool` FALSE↔TRUE (NULL ignorado) |
| ENTROU / SAIU DO SIMPLES | TRIBUTARIO | `opcao_pelo_simples_bool` FALSE↔TRUE |
| SOCIO ENTROU NO QSA | SOCIO | `data_entrada_sociedade` com vínculo ativo |
| SOCIO SAIU DO QSA | SOCIO | `vinculo_ativo` TRUE→FALSE (mês da detecção) |
| SOCIO RETORNOU AO QSA | SOCIO | `vinculo_ativo` FALSE→TRUE |
| MUDOU QUALIFICACAO SOCIO | SOCIO | qualificação mudou com vínculo ativo nas duas versões |

Colunas: `pk, entidade_tipo, evento, valor_antigo, valor_novo, informacao_adicional, mes_referencia, gerado_em`.

## Categorias macro de evento (gold, `categoria_macro_evento`)

ABERTURA, FECHAMENTO POR FALENCIA, EXTINCAO POR OBITO (MEI), EXTINCAO POR LIQUIDACAO, INAPTIDAO…,
BAIXA VOLUNTARIA / OUTRAS, SUSPENSAO TEMPORARIA, INSCRICAO NULA — derivadas por regex em
`obt_dashboard_eventos.sql`; `fato_empresa_mes` depende dessas strings.
