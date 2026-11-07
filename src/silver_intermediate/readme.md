TCC Receita Federal — Silver Intermediate (Revisão 2)
Arquivos SQL separados por tabela:
- empresas.sql
- estabelecimentos.sql
- dados_simples.sql
- socios.sql
- log_eventos_negocio.sql
- migracao_socios.sql (somente para tabela socios já existente)
O que foi corrigido
1. Empresas, estabelecimentos e dados_simples: mantida a reconstrução SCD2 por PK; comparações de hash e vigência agora são null-safe (IS DISTINCT FROM).
2. Estabelecimentos: o WHEN MATCHED UPDATE agora atualiza todos os atributos de negócio, evitando que uma correção de endereço, telefone, CNAE ou outro campo deixe a versão Silver parcialmente antiga.
3. Socios: DDL corrigido (sem coluna duplicada), incluída qualificacao_socio e incluído vinculo_ativo. O processamento deixa de usar MAX() por coluna e passa a selecionar uma linha de Bronze por PK/mês via ROW_NUMBER().
4. Socios: reprocessamento preserva passado e futuro das PKs afetadas; não usa mais DELETE/`UPDATE` preliminares que poderiam eliminar versões posteriores à janela.
5. Socios: cria uma versão vinculo_ativo = FALSE no primeiro mês em que uma relação presente no snapshot anterior deixa de aparecer no snapshot atual. Isso permite representar saída do QSA.
6. Log: corrigido para INSERT INTO ... REPLACE WHERE; usa PK canônica de empresas no evento de início de atividade; não confunde NULL tributário com FALSE; inclui eventos de entrada pela data declarada, saída, reingresso e mudança de qualificação de sócio.
7. Documentação: change_hash é documentado como SHA-256 herdado da Bronze, de acordo com a implementação corrigida da Bronze.
Pré-requisitos importantes
Ordem de execução
1. Bronze deve estar atualizada para a janela solicitada.
2. Execute os SCD2: empresas, estabelecimentos, dados_simples e socios.
3. Execute log_eventos_negocio.sql por último.
A ordem entre os três primeiros SCD2 independentes pode ser paralelizada; para o log, todos os históricos precisam estar atualizados.
Reprocessamento de socios
A detecção de saída depende de snapshots mensais completos. Para uma janela que começa em YYYY-MM, a Bronze precisa conter também o snapshot completo do mês imediatamente anterior. Exemplo: para reprocessar 2026-03 até 2026-05, é necessário que bronze.socios contenha 2026-02, 2026-03, 2026-04 e 2026-05.

A ausência só é interpretada como saída se o arquivo mensal representar o QSA completo daquele mês. O SQL também verifica se existem linhas para o mês anterior e para todos os meses da janela; se faltar um mês inteiro, a consulta falha em vez de gerar inativações em massa. Essa checagem não prova que um snapshot está completo: se a carga estiver parcialmente carregada, não execute a lógica de inativação até garantir a integridade da carga.

A data de saída não é conhecida com precisão pela simples ausência; valid_since da versão inativa representa o mês em que a ausência foi detectada. Na primeira execução sobre uma tabela antiga, vínculos que estavam indevidamente atuais e já não aparecem nem no mês anterior nem no primeiro mês reprocessado são encerrados no primeiro mês da janela; isso corrige o estado atual, mas não recupera a data histórica exata da saída. Para reconstruir saídas históricas anteriores à janela, faça um backfill desde o primeiro snapshot disponível (incluindo o mês imediatamente anterior ao início do backfill).
Migração de socios
Se a tabela ainda não existe, socios.sql cria o schema correto; não execute migracao_socios.sql.

Se a tabela já existe, consulte primeiro:
DESCRIBE TABLE tcc_receita.silver_intermediate.socios;
Se qualificacao_socio e vinculo_ativo estiverem ausentes, execute migracao_socios.sql. Se apenas uma estiver ausente, remova do ALTER TABLE a coluna que já existe. O UPDATE inicial marca os registros históricos existentes como ativos, pois a versão antiga não registrava o estado do vínculo.
Parâmetros do log de eventos
O REPLACE WHERE aceita um predicado restrito: coluna da tabela, operadores e valores literais. Por isso, diferentemente dos SCD2, log_eventos_negocio.sql exige que {{mes_referencia_inicial}} e {{mes_referencia_final}} sejam substituídos por valores literais YYYY-MM antes da execução. Não deixe o texto vazio. Para o padrão do projeto, resolva ambos para o mês anterior quando a execução não receber intervalo explícito.

Esse REPLACE WHERE (Databricks Runtime 12.2 LTS ou superior) remove os eventos antigos da janela e grava o resultado recalculado na mesma operação. Se a consulta retornar zero eventos, a janela será corretamente esvaziada; por isso, confirme que as Silver de origem estão completas antes de executar.
Limitações e decisões mantidas
- change_hash continua sendo o hash de origem calculado na Bronze, para não alterar silenciosamente a semântica histórica nesta revisão. Uma evolução futura pode separar source_change_hash do hash dos atributos já normalizados na Silver, mas isso requer migração e backfill controlado.
- capital_social permanece DOUBLE para evitar mudança de schema nesta entrega. DECIMAL(p,2) é uma melhoria recomendada antes de consolidar análises financeiras.
- socios.pk permanece a PK mascarada/customizada herdada da Bronze. Ela identifica a relação no contexto atual dos dados disponíveis; não é uma identidade global garantida da pessoa. Nenhum CPF/CNPJ em claro é criado.
- O rótulo EMPRESA NASCEU foi mantido por compatibilidade com consumidores existentes do projeto; o dado disponível é data_inicio_atividade, portanto ele deve ser interpretado como início da atividade, não como data de constituição jurídica.
- Os novos eventos societários precisam ser contemplados no mapeamento/dimensão de eventos da Silver Final/Gold, caso essa dimensão seja controlada por uma lista fechada de tipos.
- is_current é mantido como particionamento em socios para compatibilidade com a tabela existente. Uma mudança de particionamento é uma decisão operacional separada.
- CREATE TABLE IF NOT EXISTS não migra tabelas já criadas. Revise schemas existentes antes da primeira execução desses arquivos.
Validações pós-execução sugeridas
-- Deve haver no máximo uma versão corrente por PK.
SELECT pk, COUNT(*) AS qtd
FROM tcc_receita.silver_intermediate.socios
WHERE is_current = TRUE
GROUP BY pk
HAVING COUNT(*) > 1;

-- Não deve haver intervalos inválidos.
SELECT *
FROM tcc_receita.silver_intermediate.socios
WHERE valid_until IS NOT NULL
  AND valid_until <= valid_since;

-- Conferir saídas detectadas.
SELECT mes_referencia, COUNT(*) AS qtd_saidas
FROM tcc_receita.silver_intermediate.socios
WHERE vinculo_ativo = FALSE AND is_current = TRUE
GROUP BY mes_referencia
ORDER BY mes_referencia;

-- Conferir distribuição de eventos.
SELECT mes_referencia, entidade_tipo, evento, COUNT(*) AS qtd
FROM tcc_receita.silver_intermediate.log_eventos_negocio
GROUP BY mes_referencia, entidade_tipo, evento
ORDER BY mes_referencia, entidade_tipo, evento;
Nota de validação
Os arquivos foram revisados estaticamente e os pontos de consistência entre os scripts foram tratados. Não foram executados em um cluster Databricks com seus dados reais; valide primeiro em uma janela pequena e compare as contagens e transições com os snapshots de origem.