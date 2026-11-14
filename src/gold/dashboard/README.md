# Dashboard AI/BI — `tcc_receita.lvdash.json`

**Gerado** por `python -m src.gold.dashboard.gerar_dashboard` (não editar o JSON à mão: mudanças feitas na UI
se perdem na próxima geração; se gostar de um ajuste feito na UI, traga-o para o gerador).
Status: **preparado, não testado** nesta versão. O formato (grade de 12 colunas, `layoutVersion`, caixas de texto) segue
o JSON que o Databricks regravou após um import anterior; datasets e widgets novos ainda não foram importados nem
rodados no workspace. Ao gerar, o script valida referências (datasets, colunas, filtros, grade) e falha com mensagem.
Contrato de colunas: `docs/specs/features/2026-11-14-semantica-eventos-e-fluxos.md` (§ 2.4).

## Como importar
1. Rodar o Job até as tasks `gold_*` ficarem verdes.
2. *Dashboards → Create → ⋮ / seta → Import dashboard from file* → escolher `tcc_receita.lvdash.json`.
3. Selecionar o SQL warehouse no topo do editor e clicar em *Refresh*.
4. Widget com erro de formato: abrir o widget e reconfigurar pela UI (as consultas dos datasets continuam valendo).

## Custo — dashboard AI/BI exige SQL warehouse
Não roda em job cluster. Opção mais barata para a reunião:
- **Serverless 2X-Small**, *auto stop* = 5 min (ou o menor permitido), *scaling* 1–1.
- Ligar ~10 min antes da reunião, dar *Refresh* e deixar parar sozinho depois. Custo esperado: poucos dólares.
- Alternativa sem serverless: *Pro* 2X-Small (sobe mais devagar, ~5 min; auto stop mínimo 10 min).

## Páginas
| Página | Datasets | Conteúdo |
|---|---|---|
| Visão geral | `painel`, `estoque`, `taxas_setor`, `taxas_uf`, `eventos`, `situacao`, `top_cnaes`, `macro` | ativas, aberturas, baixas, reativações e saldo (contadores); fluxos mensais; crescimento líquido por 1.000 ativas por UF; taxa de fechamento por grande área; mudanças de situação (matriz, empresas distintas); eventos por tipo; top CNAEs; Selic/IPCA |
| Mercado ativo | `mercado`, `mercado_bairros`, `mercado_empresas` | concorrência local por UF/município/CNAE; 20 maiores bairros por município; lista das 1.000 maiores empresas com `exibir_razao_social`, sem endereço |

Filtros de página são filtros de campo (aplicam a todos os datasets listados no filtro que têm a coluna).
`taxas_setor`, `top_cnaes` e `macro` são Brasil e não reagem a nenhum filtro; `taxas_uf` só reage ao de UF;
`mercado_bairros` não tem CNAE (filtro de CNAE não vale).

## Regras de leitura
- Fluxos por **mês de detecção** no snapshot (extração ~dia 10); empresa = matriz do CNPJ raiz; 2023-05 é baseline
  (sem fluxo); saldo = entradas − saídas do estoque de ativas (inclui reativações, inaptidões, suspensões).
- Aberturas vêm só do `painel` (`fato_indicadores_setoriais_mes`); a OBT não conta abertura.
- Limitação: `LIMIT` em dataset (`mercado_empresas`, 1.000 maiores do Brasil) é aplicado antes dos filtros de página.
