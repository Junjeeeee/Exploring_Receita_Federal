# Visão geral do projeto `tcc_receita`

## 1. Resumo executivo

O projeto `tcc_receita` é uma plataforma de engenharia e análise de dados construída a partir dos dados públicos de CNPJ da Receita Federal.

A ideia central é transformar os arquivos disponibilizados pela Receita Federal — originalmente distribuídos como grandes conjuntos de dados tabulares e snapshots mensais — em uma estrutura organizada, histórica e analítica, capaz de responder não apenas como uma empresa está cadastrada, mas também como ela mudou ao longo do tempo.

O projeto percorre um fluxo de engenharia de dados:

Receita Federal / IBGE → Landing → Bronze → Silver Intermediate → Silver Final → Gold / análises

Ao longo desse processo, os dados são extraídos, armazenados, padronizados, historizados, relacionados e transformados em informações de negócio.

Há três grandes objetivos técnicos:

1. Construir uma base histórica confiável: manter o histórico cadastral, tributário e societário das empresas, estabelecimentos e relações com sócios.
2. Detectar acontecimentos de negócio: identificar mudanças como entrada e saída do Simples Nacional, alterações de CNAE, mudanças cadastrais e movimentações societárias.
3. Disponibilizar os dados para análise: organizar as informações em modelos dimensionais e permitir análises, dashboards, estudos de redes e aplicações de machine learning.

O projeto é, portanto, mais do que uma simples ingestão dos arquivos da Receita Federal. A parte importante é construir uma representação temporal e analítica dos dados, preservando a evolução das entidades.

## 2. Origem dos dados e domínio do projeto

A principal fonte é a base pública de CNPJ da Receita Federal.

Ela contém diferentes conjuntos de informações que, combinados, permitem representar as empresas e suas relações.

Os principais grupos de dados trabalhados são:

| Grupo               | O que representa                                     | Granularidade          |
| ------------------- | ---------------------------------------------------- | ---------------------- |
| Empresas            | Dados gerais da pessoa jurídica                      | CNPJ básico            |
| Estabelecimentos    | Matrizes e filiais, endereços, situação e atividades | Estabelecimento        |
| Sócios              | Quadro societário e administradores                  | Relação empresa–sócio  |
| Dados do Simples    | Enquadramento no Simples Nacional e MEI              | Empresa                |
| CNAEs               | Classificação das atividades econômicas              | Código CNAE            |
| Municípios          | Domínio geográfico dos municípios                    | Código do município    |
| Naturezas jurídicas | Tipos jurídicos das empresas                         | Código da natureza     |
| Motivos de situação | Motivos associados às situações cadastrais           | Código do motivo       |
| Qualificações       | Qualificações de sócios e responsáveis               | Código da qualificação |
| Países              | Domínio de países                                    | Código do país         |
| CEPs                | Dados auxiliares de endereçamento                    | CEP                    |

Além dos dados da Receita Federal, existe a utilização dos domínios de CNAE do IBGE, que permitem enriquecer e organizar as atividades econômicas em uma hierarquia.

Um ponto importante da modelagem é que os dados possuem granularidades diferentes.

Por exemplo:

- Uma empresa é identificada pelo CNPJ básico, isto é, os primeiros oito dígitos.
- Uma empresa pode possuir vários estabelecimentos.
- Um estabelecimento pode ser matriz ou filial.
- Uma empresa pode possuir vários sócios.
- Uma relação societária possui uma identidade própria e deve ser historizada independentemente do cadastro geral da empresa.

Essa distinção é fundamental para evitar relacionamentos incorretos e problemas de duplicidade nas camadas analíticas.

## 3. Arquitetura geral

A arquitetura segue uma organização em camadas, cada uma com uma responsabilidade específica.

Origem

Receita Federal e IBGE

Arquivos públicos, snapshots mensais, tabelas auxiliares e domínios de classificação.

1\. Landing — extração e armazenamento bruto

Python, arquivos compactados, conversão para Parquet, organização mensal e envio para S3.

2\. Bronze — ingestão estruturada

Databricks, Spark, Delta Lake, Unity Catalog, chaves técnicas, hashes e metadados de ingestão.

3\. Silver Intermediate — tratamento e histórico

Padronização, regras de negócio, SCD Tipo 2, histórico de empresas, estabelecimentos, dados tributários e sócios.

4\. Log de eventos de negócio

Registro das transições detectadas: mudanças cadastrais, tributárias, econômicas e societárias.

5\. Silver Final — modelo analítico

Dimensões, fatos, dimensão temporal e ponte para relacionar empresas, grupos e CNAEs.

6\. Gold e aplicações analíticas

Indicadores, agregações, dashboards, análises históricas, exploração de redes e modelos preditivos.

A separação entre as camadas tem uma função importante: cada etapa recebe dados de uma camada anterior, aplica um conjunto específico de transformações e produz uma saída com responsabilidades mais bem definidas.

## 4. Landing: extração e armazenamento dos dados

A Landing é a camada responsável por buscar os dados da Receita Federal e disponibilizá-los em um formato adequado para as próximas etapas.

Ela é executada localmente, utilizando Python, e possui uma biblioteca de extração compartilhada pelas configurações das tabelas.

### Funcionamento

Em linhas gerais, o processo:

1. Identifica os arquivos disponibilizados pela Receita Federal.
2. Localiza os arquivos correspondentes à tabela e ao mês desejado.
3. Baixa e descompacta os arquivos.
4. Interpreta os dados tabulares, incluindo as extensões específicas utilizadas pela Receita.
5. Converte os dados para Parquet.
6. Envia os arquivos para o bucket S3.
7. Executa os testes de qualidade já definidos para a Landing.

O armazenamento segue uma organização por tabela e mês de referência.

O bucket padrão discutido é `lakehouse-receita`, configurável pela variável de ambiente `AWS_BUCKET_NAME`.

Um exemplo conceitual de caminho:

```
s3://lakehouse-receita/
    landing/
        receita_federal/
            empresa/
                mes_referencia=2023-05/
                    dados.parquet
            estabele/
                mes_referencia=2023-05/
                    dados.parquet
            socio/
                mes_referencia=2023-05/
                    dados.parquet
            simples/
                mes_referencia=2023-05/
                    dados.parquet
```

### Organização das tabelas

As configurações foram separadas entre tabelas mensais e estáticas.

Tabelas mensais:

- `empresas`
- `estabelecimentos`
- `socios`
- `dados_simples`

Tabelas estáticas:

- `ceps`
- `cnaes_ibge`
- `motivos_situacao`
- `municipios`
- `naturezas_juridicas`
- `paises`
- `qualificacoes_socios`

As tabelas mensais possuem snapshots correspondentes a cada mês. As estáticas são armazenadas como arquivos únicos, e não como partições mensais.

### Orquestração

Também foi discutida a organização da execução por mês, em vez de processar todos os meses de uma tabela antes de avançar para a próxima.

A lógica desejada é:

```
Maio/2023
    empresas
    estabelecimentos
    dados_simples
    socios
    testes de qualidade

Junho/2023
    empresas
    estabelecimentos
    dados_simples
    socios
    testes de qualidade

Julho/2023
    ...

Agosto/2023
    ...
```

Isso permite concluir a extração e a validação de um mês inteiro antes de avançar.

O orquestrador também aceita `--force`, que força a reextração e substitui os arquivos existentes nas mesmas chaves do S3.

### Problemas encontrados

Durante os testes, apareceram problemas relacionados à interpretação dos arquivos da Receita Federal.

O principal foi a extensão dos arquivos internos dos ZIPs: alguns arquivos utilizam extensões específicas, como `.EMPRECSV` e `.ESTABELE`, em vez de `.csv`.

Também houve uma falha `BadZipFile` durante a extração dos estabelecimentos de agosto de 2023. Isso ocorreu durante a abertura do arquivo ZIP, antes da leitura do conteúdo tabular.

Por isso, foram preparadas correções para:

- Reconhecer as extensões específicas da Receita.
- Melhorar o diagnóstico de arquivos que não são ZIPs válidos.
- Exibir o nome do arquivo problemático.
- Identificar possíveis respostas HTTP inválidas ou downloads incompletos.
- Manter tentativas de repetição em caso de falhas transitórias.

A decisão mais recente foi reprocessar as quatro tabelas mensais de maio a agosto de 2023 com `--force`, usando o orquestrador mês a mês e a biblioteca de extração corrigida.

## 5. Bronze: ingestão estruturada e metadados

A Bronze recebe os Parquets da Landing e os transforma em tabelas Delta dentro do Databricks e do Unity Catalog.

O catálogo principal é:

```
tcc_receita.bronze
```

A Bronze funciona como a primeira representação estruturada dos dados dentro do ambiente analítico.

Ela não é responsável por implementar todas as regras de negócio. Sua função é garantir que os dados estejam organizados, identificáveis, rastreáveis e disponíveis para as transformações posteriores.

### Configuração das tabelas

Foi desenvolvida uma estrutura de configuração chamada `TabelaBronzeConfig`.

A ideia é permitir que cada tabela declare suas particularidades sem precisar duplicar todo o código de ingestão.

Entre as informações consideradas estão:

- Nome da tabela no Unity Catalog.
- Pasta correspondente na Landing.
- Chaves primárias.
- Descrição da tabela e das colunas.
- Colunas esperadas.
- Expressão customizada de chave primária, quando necessária.
- Configurações para ordenação das colunas de origem.
- Limites para validações de volumetria.
- Indicação de snapshot mensal.
- Configuração das colunas utilizadas no hash.

### Metadados importantes

Foram discutidos quatro metadados principais:

| Campo                 | Responsabilidade                                      |
| --------------------- | ----------------------------------------------------- |
| `pk`                  | Identificar logicamente um registro ou relacionamento |
| `change_hash`         | Detectar alterações nos atributos de negócio          |
| `mes_referencia`      | Identificar o mês do snapshot                         |
| `bronze_processed_at` | Registrar quando a Bronze processou aquele registro   |

Um detalhe técnico importante é que a `pk` não deve ser interpretada automaticamente como o CNPJ básico.

Na implementação corrigida, as chaves compostas padrão são serializadas como JSON. Algumas tabelas, como sócios, possuem uma expressão customizada para identificar a relação entre empresa e sócio.

Portanto, a `pk` é uma identificação técnica e deve ser tratada como tal. Para relacionar entidades de negócio, devem ser utilizadas as colunas apropriadas, como `cnpj_basico`, e não a interpretação textual da chave.

Também foi corrigida a documentação dos hashes para refletir o uso de SHA-256, em vez de MD5.

A variável padrão do bucket é `AWS_BUCKET_NAME`, com valor histórico `lakehouse-receita`.

## 6. Silver Intermediate: tratamento, padronização e histórico

Essa é uma das camadas mais importantes do projeto.

Enquanto a Bronze organiza os dados recebidos, a Silver Intermediate começa a representar corretamente o domínio das empresas e suas transformações.

O catálogo é:

```
tcc_receita.silver_intermediate
```

O principal conceito utilizado é o SCD Tipo 2 (Slowly Changing Dimension Type 2).

### O que significa utilizar SCD2 aqui?

Em vez de simplesmente atualizar um registro quando algum atributo muda, o projeto mantém as versões anteriores.

Por exemplo, suponha que uma empresa tenha um capital social de R$ 10 mil em maio e R$ 20 mil em junho.

Em vez de substituir R$ 10 mil por R$ 20 mil, o histórico fica conceitualmente assim:

| Empresa | Capital | Início  | Fim     | Atual |
| ------- | ------- | ------- | ------- | ----- |
| A       | 10.000  | 2023-05 | 2023-06 | Não   |
| A       | 20.000  | 2023-06 | NULL    | Sim   |

Os intervalos são semiabertos:

\\[ [valid\\\_since,\ valid\\\_until) \\]

Isso significa que `valid_since` é inclusivo e `valid_until` é exclusivo. Uma versão com `valid_until = NULL` é a versão atual.

As tabelas utilizam campos como:

- `mes_referencia`
- `inserted_at`
- `updated_at`
- `change_hash`
- `valid_since`
- `valid_until`
- `is_current`

A ideia é preservar o histórico sem precisar armazenar uma cópia integral e independente de cada registro a cada mês.

### 6.1. Tabela `empresas`

Representa o cadastro geral das empresas, na granularidade do CNPJ básico.

Entre os atributos trabalhados estão:

- `cnpj_basico`
- `razao_social`
- `natureza_juridica`
- `qualificacao_responsavel`
- `capital_social`
- `porte_empresa`
- `ente_federativo_responsavel`

Também existe padronização de dados textuais, como a razão social em maiúsculas, preservando caracteres Unicode.

O porte empresarial utiliza os códigos da fonte:

| Código | Significado              |
| ------ | ------------------------ |
| 01     | Microempresa             |
| 03     | Empresa de Pequeno Porte |
| 05     | Demais                   |

A tabela mantém o histórico das versões cadastrais da empresa.

### 6.2. Tabela `estabelecimentos`

Representa as matrizes e filiais.

É uma tabela particularmente importante porque contém informações que não existem necessariamente na granularidade da empresa como um todo.

Entre os atributos trabalhados estão:

- CNPJ básico, ordem e dígito.
- Identificador de matriz ou filial.
- Nome fantasia.
- Situação cadastral.
- Data e motivo da situação cadastral.
- CNAE principal e secundários.
- Endereço.
- Município e UF.
- CEP.
- Telefones.
- E-mail.
- Situação especial.

A implementação também padroniza informações cadastrais, como a descrição da situação, datas e dados de contato.

O SCD2 permite identificar alterações em estabelecimentos sem perder o estado anterior.

Uma decisão importante nas revisões foi garantir que, quando o hash indicar uma alteração, todos os atributos de negócio pertinentes sejam atualizados. Uma atualização parcial poderia deixar informações antigas combinadas com informações novas.

### 6.3. Tabela `dados_simples`

Representa o enquadramento da empresa no Simples Nacional e no MEI.

Entre os campos trabalhados estão:

- Opção pelo Simples Nacional.
- Data de opção pelo Simples.
- Data de exclusão do Simples.
- Opção pelo MEI.
- Data de opção pelo MEI.
- Data de exclusão do MEI.

Os valores de origem `S/N` são convertidos em booleanos, e datas inválidas ou representadas por `00000000` são tratadas.

A tabela mantém histórico SCD2 das mudanças no enquadramento.

Um cuidado discutido foi não confundir ausência de informação com valor falso. Por exemplo, `NULL` não deve ser automaticamente convertido em `FALSE`, porque isso pode criar eventos tributários que não ocorreram ou eliminar a distinção entre estado desconhecido e não enquadramento.

### 6.4. Tabela `socios`

Essa tabela representa o Quadro de Sócios e Administradores (QSA).

É uma das tabelas mais interessantes do ponto de vista de modelagem porque sua granularidade é a relação entre uma empresa e um sócio, e não simplesmente a empresa.

Entre os campos trabalhados estão:

- CNPJ básico da empresa.
- Identificador do tipo de sócio.
- Nome ou razão social do sócio.
- Documento do sócio, conforme fornecido pela origem.
- Qualificação societária.
- Data de entrada na sociedade.
- País.
- Representante legal.
- Nome do representante.
- Qualificação do representante.
- Faixa etária.

O histórico também registra `vinculo_ativo`.

Isso permite representar quando uma relação societária aparece nos snapshots e quando deixa de aparecer.

#### Uma questão importante: a saída de sócios

A fonte fornece informações sobre a composição societária em cada snapshot, mas a saída nem sempre é representada como um evento explícito de encerramento da relação.

A solução discutida foi comparar snapshots mensais completos.

Se uma relação aparece em um mês e deixa de aparecer no snapshot seguinte, o projeto pode marcar a relação como inativa a partir do primeiro snapshot completo em que a ausência foi observada.

Isso exige uma dependência temporal: para processar maio de 2023, por exemplo, é necessário ter também o snapshot completo de abril de 2023.

Essa regra é importante porque evita tratar o primeiro mês da janela como se não existisse histórico anterior.

#### Privacidade e LGPD

Esse ponto foi discutido explicitamente durante a revisão do projeto.

A decisão é não transportar CPF em claro para as camadas do projeto.

Os documentos de sócios e representantes são mantidos conforme recebidos, parcialmente mascarados quando assim fornecidos pela origem. Não se deve tentar reconstruir ou desmascarar esses documentos.

Também foi discutido que informações adicionais dos eventos não devem carregar documentos pessoais em claro.

Existe ainda um tratamento específico na Landing para nomes empresariais de MEIs que podem conter informações pessoais. A expressão regular usada para essa limpeza foi testada repetidamente em dados reais e deve ser preservada, salvo evidência concreta de falha.

## 7. Log de eventos de negócio

Além de manter o estado histórico das entidades, o projeto procura identificar o que aconteceu com elas.

Essa responsabilidade fica concentrada na tabela:

```
tcc_receita.silver_intermediate.log_eventos_negocio
```

Ela transforma mudanças observadas nas versões SCD2 em eventos de negócio.

A estrutura contém:

| Campo                  | Significado                          |
| ---------------------- | ------------------------------------ |
| `pk`                   | Chave da entidade ou relação afetada |
| `entidade_tipo`        | Granularidade da entidade            |
| `evento`               | Tipo do acontecimento                |
| `valor_antigo`         | Estado anterior                      |
| `valor_novo`           | Estado posterior                     |
| `informacao_adicional` | Informação complementar              |
| `mes_referencia`       | Mês do evento                        |
| `gerado_em`            | Momento de geração do evento         |

A presença de `entidade_tipo` é importante porque uma mudança em uma empresa não tem a mesma granularidade de uma mudança em um estabelecimento ou em uma relação societária.

### Eventos identificados na versão mais recente que revisamos

O SQL mais recente contém 13 regras de evento:

| Grupo           | Evento                        |
| --------------- | ----------------------------- |
| Empresa         | `EMPRESA NASCEU`              |
| Estabelecimento | `MUDOU ESTADO CADASTRAL`      |
| Tributário      | `ENTROU NO MEI`               |
| Tributário      | `SAIU DO MEI`                 |
| Tributário      | `ENTROU NO SIMPLES`           |
| Tributário      | `SAIU DO SIMPLES`             |
| Empresa         | `MUDOU NATUREZA JURIDICA`     |
| Estabelecimento | `MUDOU CNAE PRINCIPAL`        |
| Estabelecimento | `MUDOU LISTA CNAE SECUNDARIO` |
| Societário      | `SOCIO ENTROU NO QSA`         |
| Societário      | `SOCIO SAIU DO QSA`           |
| Societário      | `SOCIO RETORNOU AO QSA`       |
| Societário      | `MUDOU QUALIFICACAO SOCIO`    |

log_eventos_negocio.sql

Alguns exemplos ilustram bem a proposta:

Empresa nasceu: é identificado pelo início de atividade informado pela Receita Federal, utilizando o estabelecimento matriz para evitar tratar cada filial como uma nova empresa.

Mudou estado cadastral: é identificado comparando versões consecutivas do estabelecimento, inclusive considerando mudanças no motivo da situação cadastral.

Entrou ou saiu do MEI: é identificado por transições entre versões SCD2 dos dados tributários.

Mudou CNAE: é identificado comparando os CNAEs principal e secundários entre versões do estabelecimento.

Sócio entrou: utiliza a data de entrada na sociedade informada pela Receita Federal.

Sócio saiu: é inferido a partir do primeiro snapshot completo em que a relação deixa de aparecer.

Sócio retornou: registra uma transição de vínculo inativo para ativo.

Essa distinção entre a data informada pela fonte e a data em que uma ausência foi detectada é importante. Uma entrada pode ter uma data de negócio declarada; uma saída inferida pode ter como referência o mês em que a ausência foi identificada.

### Por que o log é relevante?

Sem o log de eventos, seria necessário consultar várias tabelas históricas e implementar novamente as regras para cada análise.

Com ele, as aplicações analíticas podem trabalhar diretamente com acontecimentos interpretáveis.

Por exemplo:

- Quantas empresas entraram no Simples em determinado período?
- Quantas saíram do MEI?
- Quais setores apresentaram maior movimentação de CNAE?
- Quantos estabelecimentos mudaram de situação cadastral?
- Como evoluiu a composição societária das empresas?

O log é, assim, uma representação intermediária entre o histórico técnico das tabelas e as análises de negócio.

## 8. Silver Final: organização para análise

A Silver Final é a camada em que as informações são reorganizadas para facilitar consultas analíticas.

O catálogo é:

```
tcc_receita.silver_final
```

A modelagem discutida segue uma abordagem dimensional, com dimensões, fato e uma tabela-ponte.

O conjunto revisado contém sete objetos principais.

| Objeto                | Responsabilidade                    |
| --------------------- | ----------------------------------- |
| `dim_cnae`            | Catálogo hierárquico de CNAEs       |
| `dim_grupo_cnae`      | Agrupamentos de CNAEs               |
| `dim_empresa`         | Histórico cadastral das empresas    |
| `dim_mudanca_estado`  | Catálogo de tipos de mudança        |
| `dim_tempo`           | Dimensão temporal                   |
| `ponte_cnae`          | Relacionamento entre grupos e CNAEs |
| `fato_mudanca_estado` | Registro analítico dos eventos      |

### 8.1. `dim_cnae`

Representa a classificação CNAE, enriquecida com a hierarquia do IBGE.

O objetivo não é armazenar apenas o código e a descrição da subclasse, mas também permitir agregações em níveis superiores, como:

- Seção.
- Divisão.
- Grupo.
- Classe.
- Subclasse.

Também foi discutida a criação de uma classificação de grandes setores econômicos derivada da Seção CNAE. Esse agrupamento é analítico e não deve ser confundido com uma classificação oficial do IBGE.

### 8.2. `dim_grupo_cnae`

Representa agrupamentos de atividades econômicas associados às empresas.

Ela permite trabalhar com conjuntos de CNAEs e facilita análises que exigem uma visão consolidada das atividades.

### 8.3. `ponte_cnae`

É a tabela responsável por relacionar grupos de CNAEs aos códigos individuais.

Isso é importante porque um grupo pode estar associado a vários CNAEs, e uma relação muitos-para-muitos não deve ser artificialmente comprimida em uma única coluna.

A ponte também distingue CNAEs encontrados no domínio oficial daqueles que não foram localizados.

Na modelagem corrigida:

- `-1` representa uma situação não aplicável ou ausente.
- `-2` representa um CNAE informado, mas não localizado no domínio IBGE.

Essa distinção evita tratar como equivalentes duas situações diferentes.

### 8.4. `dim_empresa`

Representa a empresa em uma perspectiva histórica.

A dimensão foi pensada para consolidar informações da matriz e do CNPJ básico, permitindo analisar o estado da empresa ao longo do tempo.

Um ponto técnico importante identificado na revisão foi que os relacionamentos devem utilizar `cnpj_basico`, e não comparar esse campo com a `pk` técnica serializada da Bronze ou da Intermediate.

Também foi destacado que sobreposições de intervalos temporais devem ser investigadas por validações, em vez de serem resolvidas silenciosamente escolhendo uma linha arbitrária.

### 8.5. `dim_mudanca_estado`

Armazena os diferentes tipos de eventos identificados pelo log de negócio.

Por exemplo, duas mudanças podem apresentar os mesmos valores anterior e posterior, mas representar acontecimentos de granularidades diferentes.

Por isso, `entidade_tipo` faz parte da identificação do evento.

A dimensão permite classificar e agrupar as transições de forma consistente.

### 8.6. `dim_tempo`

Organiza as referências temporais para as análises.

A proposta utiliza uma dimensão mensal, com chaves substitutas e atributos temporais, possibilitando análises por mês, ano e período.

### 8.7. `fato_mudanca_estado`

É o centro da modelagem analítica de eventos.

Ela relaciona:

- A mudança ocorrida.
- A empresa associada.
- O estado anterior da empresa.
- O estado posterior.
- O período da mudança.
- A entidade e a chave de origem do evento.

Uma decisão importante da revisão foi preservar a granularidade da entidade original por meio de campos como `entidade_tipo` e `pk_entidade_origem`.

Isso permite que um evento originado por um estabelecimento ou por uma relação societária continue rastreável, mesmo quando é associado à empresa para análises consolidadas.

Também foi corrigida a estratégia de relacionamento com as dimensões: em vez de recalcular as chaves substitutas na fato de maneira independente, a fato deve localizar os registros correspondentes nas dimensões por meio de JOINs.

### Validações da Silver Final

O pacote revisado contém consultas para validar, entre outras coisas:

- Sobreposição de períodos de vigência.
- Colisões de chaves substitutas.
- Duplicidade no grão das dimensões.
- Duplicidade na ponte CNAE.
- Chaves estrangeiras sem correspondência.
- Eventos que não conseguiram ser relacionados a um CNPJ básico.

A documentação também deixa explícito que a revisão dos SQLs foi estática: os arquivos preparados não foram executados em um cluster Databricks. Portanto, ainda é necessário validá-los no ambiente real.&#x20;

README.md



## 9. Gold e consumo analítico

A Gold representa o nível de consumo do projeto.

A intenção discutida é disponibilizar os dados de forma mais direta para consultas, indicadores, visualizações e aplicações.

A Silver Final fornece dimensões e fatos mais genéricos. A Gold pode utilizar essas estruturas para construir tabelas agregadas e indicadores específicos, sem obrigar cada dashboard a refazer as mesmas transformações.

Algumas análises que combinam naturalmente com a estrutura construída são:

Evolução empresarial: quantidade de empresas por período, porte, natureza jurídica e localização.

Movimentação cadastral: quantidade de mudanças de situação cadastral, por região e período.

Movimentação tributária: entradas e saídas do Simples Nacional e do MEI.

Atividades econômicas: alterações de CNAE principal e secundário, evolução dos setores e concentração de atividades.

Estrutura societária: composição do QSA, entrada e saída de sócios e mudanças nas qualificações.

Histórico empresarial: comparação entre os atributos da empresa antes e depois de determinado evento.

Uma ideia que aparece naturalmente é materializar agregações específicas para facilitar dashboards, inclusive seguindo uma organização de consumo por nível de agregação.

Entretanto, não trataria como concluído um conjunto específico de tabelas Gold: nas conversas, a construção e revisão detalhada se concentrou principalmente na Landing, Bronze e nas duas camadas Silver.

## 10. Aplicação de machine learning: previsão de falência

Outro ponto relevante discutido foi a utilização da base de empresas para construir um modelo preditivo.

Foi desenvolvido um experimento de previsão de falência utilizando empresas de Niterói.

### Definição do problema

A base utilizada contém 33.920 empresas que estavam vivas em janeiro de 2024.

O objetivo é prever se uma empresa viria a falir até janeiro de 2026.

A variável-alvo foi definida como:

```
y_falencia_futura
```

A proposta é utilizar informações disponíveis no período inicial para tentar identificar características associadas a uma falência futura.

Você também destacou que a construção da variável-alvo não apresentava vazamento de informação, pois os dados utilizados como atributos não incorporavam o resultado futuro.

### Engenharia de atributos

Entre os tratamentos discutidos estavam:

- Remover `empresa_id`, por ser um identificador e não uma característica preditiva adequada.
- Remover município e bairro na configuração do experimento.
- Aplicar `log1p` sobre o capital social.
- Preservar porte empresarial.
- Preservar natureza jurídica.
- Criar tempo de existência da empresa em meses.
- Utilizar informações do Simples e MEI.
- Criar atributos baseados nos prefixos de CNAE.

O algoritmo utilizado foi o CatBoost Classifier.

### Resultados obtidos

ROC-AUC médio

# 0,8635

Desvio: ± 0,0062

PR-AUC médio

# 0,4219

Desvio: ± 0,0136

Recall observado

# 0,78–0,80

Aproximadamente

F1 observado

# 0,40

Aproximadamente

Os resultados foram obtidos com validação cruzada K-Fold.

A ROC-AUC indica que o modelo apresenta boa capacidade de ordenação das empresas entre as classes. Já a PR-AUC é especialmente importante nesse tipo de problema, pois a classe de falência é minoritária.

O recall relativamente alto mostra que o modelo consegue identificar uma parcela significativa das empresas que falirão, embora isso venha acompanhado de um número considerável de falsos positivos, refletido no F1 observado.

O experimento mostra uma aplicação concreta do valor da base histórica construída: além de consultas descritivas, ela pode servir de fundamento para análises preditivas.

É importante separar esse resultado experimental de um modelo já implantado ou validado operacionalmente. O que discutimos foi o treinamento e a avaliação do classificador, não sua implantação em produção.

## 11. Discussão sobre grafos e relações societárias

Também discutimos a representação dos dados relacionais como grafos.

Esse caminho é particularmente interessante para a tabela de sócios.

Uma representação natural seria:

- Empresas como vértices.
- Sócios como vértices.
- Relações societárias como arestas.
- Atributos das entidades e das relações como propriedades.
- Datas de entrada e saída como informações temporais.

Isso permitiria estudar estruturas como:

- Empresas que compartilham sócios.
- Sócios que participam de várias empresas.
- Comunidades de empresas relacionadas.
- Evolução da rede societária.
- Conexões entre empresas de diferentes setores econômicos.

Também houve uma discussão anterior sobre um projeto de TCC baseado em uma biblioteca Python configurável para transformar bases relacionais em grafos.

A ideia era permitir que um usuário técnico configurasse:

- Quais tabelas seriam utilizadas como vértices.
- Quais tabelas representariam arestas.
- Quais atributos seriam associados a cada tipo de entidade.
- Como combinar informações de diferentes fontes.
- Quais fontes teriam prioridade em caso de conflitos.

A saída seria um conjunto de tabelas de vértices e arestas, que poderia ser consumido por ferramentas de visualização ou modelos de machine learning, incluindo GNNs.

Essa ideia de biblioteca genérica não deve ser confundida com uma etapa já implementada no pipeline `tcc_receita`. A conexão mais direta entre os dois assuntos é que o QSA fornece um caso real e relevante para experimentar a transformação relacional em grafos.

## 12. Decisões técnicas e cuidados importantes

Ao longo das revisões, alguns princípios ficaram especialmente importantes para a consistência do projeto.

### 12.1. Chaves técnicas não são identificadores de negócio

A `pk` da Bronze pode ser uma chave composta serializada em JSON.

Ela não deve ser interpretada como CNPJ básico ou manipulada com regex para recuperar a raiz do CNPJ.

Os relacionamentos devem usar os campos de negócio apropriados, como `cnpj_basico`, e manter a PK técnica para rastreabilidade.

### 12.2. A granularidade precisa ser preservada

Empresa, estabelecimento e relação societária são entidades diferentes.

Uma mudança em uma filial não deve ser automaticamente tratada como uma mudança na matriz. Da mesma forma, a entrada de um sócio não é simplesmente uma atualização de atributo da empresa.

Por isso, `entidade_tipo` e `pk_entidade_origem` são importantes para preservar o significado dos eventos.

### 12.3. O histórico precisa continuar correto após reprocessamentos

O pipeline foi discutido para permitir reprocessamento de janelas históricas.

Isso significa que recalcular maio a agosto não pode simplesmente apagar todas as versões futuras das entidades afetadas.

O processamento deve reconstruir as transições dentro da janela solicitada e preservar corretamente os períodos anteriores e posteriores.

### 12.4. A ausência de dados não significa necessariamente valor falso

Um campo ausente não deve ser convertido automaticamente em `FALSE`.

Isso é particularmente relevante para os dados tributários, pois a inexistência de uma versão pode representar falta de informação, e não necessariamente que a empresa não está enquadrada.

### 12.5. O QSA precisa de snapshots completos

A detecção de saída de sócio depende da comparação entre snapshots consecutivos completos.

Um mês faltante ou incompleto pode gerar uma falsa saída de sócio.

Por isso, o snapshot anterior ao início de uma janela de processamento também é um pré-requisito importante.

### 12.6. As transformações precisam ser reproduzíveis

O projeto utiliza parâmetros de referência mensal:

```
{{mes_referencia_inicial}}
{{mes_referencia_final}}
```

Quando não são informados, a convenção discutida é assumir o mês anterior.

Isso permite que as transformações sejam executadas para períodos específicos ou para a carga mais recente, sem precisar alterar manualmente toda a lógica.

Também foi necessário ajustar detalhes da sintaxe SQL do Databricks, especialmente em operações de substituição de partições, `MERGE`, `INSERT OVERWRITE` e `REPLACE WHERE`.

### 12.7. Qualidade e rastreabilidade

A qualidade é verificada ao longo do processamento, e não somente ao final.

Na Landing, os testes existentes são executados após a extração. A Bronze mantém validações de schema e volumetria. Nas camadas Silver, são necessárias verificações de consistência temporal, unicidade, relacionamentos e integridade referencial.

Você também deixou claro que a implementação de Data Quality existente não deve ser redesenhada sem necessidade; o foco das revisões é corrigir os problemas demonstrados e preservar os mecanismos já existentes.

## 13. Estado atual do projeto

É importante separar os componentes discutidos, os códigos preparados e aquilo cuja execução ainda precisa ser confirmada.

| Componente                              | Estado conforme nossas conversas                                                    |
| --------------------------------------- | ----------------------------------------------------------------------------------- |
| Arquitetura Landing → Bronze → Silver   | Estrutura definida                                                                  |
| Extração local da Landing               | Implementada, com correções em andamento                                            |
| Orquestrador mês a mês                  | Código corrigido preparado                                                          |
| Tratamento das extensões ZIP da Receita | Biblioteca corrigida preparada                                                      |
| Bronze                                  | Implementação desenvolvida e revisada em diferentes pontos                          |
| SCD2 de empresas                        | SQL revisado e versão corrigida preparada                                           |
| SCD2 de estabelecimentos                | SQL revisado e versão corrigida preparada                                           |
| SCD2 de dados Simples/MEI               | SQL revisado e versão corrigida preparada                                           |
| SCD2 de sócios                          | SQL revisado e versão corrigida preparada                                           |
| Log de eventos                          | SQL revisado, incluindo eventos societários                                         |
| Silver Final                            | Pacote de SQLs corrigidos e validações preparado                                    |
| Gold                                    | Consumo analítico planejado, sem detalhamento equivalente ao das camadas anteriores |
| Modelo de falência                      | Experimento CatBoost desenvolvido e avaliado                                        |
| Geração de grafos                       | Conceito discutido; não tratá-lo como pipeline concluído                            |

### 13.1. Situação específica do backfill de maio a agosto de 2023

O período que está sendo reprocessado é:

```
2023-05 até 2023-08
```

Na execução anterior:

- `empresas` foi extraída para os quatro meses e passou nos testes de qualidade.
- `estabelecimentos` foi extraída para maio, junho e julho.
- `estabelecimentos` falhou em agosto com `BadZipFile`.

A correção preparada para a biblioteca de extração contempla o diagnóstico dessa falha, além da leitura das extensões especiais dos arquivos da Receita.

A decisão mais recente foi executar novamente as quatro tabelas mensais:

```
poetry run python -m src.landing.orquestrador \
    --tabela empresas,estabelecimentos,dados_simples,socios \
    --mes_referencia_inicial 2023-05 \
    --mes_referencia_final 2023-08 \
    --force
```

Esse comando pressupõe que a versão atualizada do orquestrador e a biblioteca corrigida já tenham substituído os respectivos arquivos em `src/landing/`.

A intenção é que o processamento seja mês a mês, com os testes de qualidade após cada tabela.

As tabelas estáticas não precisam ser reprocessadas para esse backfill.

### 13.2. Próximas etapas do reprocessamento

Depois que a Landing estiver consistente, a ordem planejada é:

1. Finalizar a Landing

   Reprocessar as quatro tabelas de maio a agosto e confirmar os testes de qualidade.
2. Atualizar a Bronze

   Reingerir os Parquets atualizados para as quatro tabelas no Databricks.
3. Reprocessar a Silver Intermediate

   Executar os SCD2 de `empresas`, `estabelecimentos`, `dados_simples` e `socios`.
4. Reprocessar o log de eventos

   Gerar novamente `log_eventos_negocio`, considerando o histórico atualizado.
5. Atualizar a Silver Final

   Executar as dimensões, a ponte CNAE e, por último, a fato de mudanças.
6. Executar as validações

   Verificar intervalos, duplicidades, colisões de chaves e relacionamentos órfãos.

Há uma dependência importante para o SCD2 de sócios: a Bronze precisa conter também o snapshot completo de abril de 2023. Se esse snapshot já estiver correto, não é preciso reextraí-lo; se for alterado, deverá ser reprocessado nas camadas seguintes também.&#x20;

ORDEM_REPROCESSAMENTO_2023.md



Os pacotes SQL corrigidos da Silver Intermediate e Silver Final foram revisados estaticamente. Isso não equivale a uma execução bem-sucedida no Databricks: ainda é necessário validar os schemas reais, a compatibilidade de runtime e os resultados das consultas no ambiente.

## 14. Qual é a contribuição técnica do projeto?

O que considero mais relevante é que o projeto combina diferentes problemas de engenharia de dados em uma aplicação concreta.

Não se limita a baixar arquivos e colocá-los em um data lake. Ele procura resolver questões que aparecem quando queremos transformar snapshots públicos em uma base histórica e analítica consistente.

A contribuição técnica está na combinação de:

Ingestão de grandes volumes: extração automatizada de arquivos da Receita Federal, tratamento de arquivos heterogêneos, organização no S3 e ingestão no Databricks.

Historização de entidades: aplicação de SCD2 para representar mudanças cadastrais, tributárias e societárias.

Detecção de eventos: criação de regras capazes de transformar diferenças entre snapshots em eventos com significado de negócio.

Modelagem analítica: construção de dimensões, fatos e relacionamentos que facilitam consultas e agregações.

Governança e privacidade: cuidado com documentos pessoais, preservação de rastreabilidade e separação entre chaves técnicas e identificadores de negócio.

Reaproveitamento analítico: disponibilização de uma base que pode alimentar dashboards, estudos de redes e modelos preditivos.

O diferencial não está isoladamente no uso de Spark, Delta Lake, SCD2 ou CatBoost. Está na construção de um fluxo integrado que preserva o significado e a evolução dos dados desde a origem até o consumo.

## 15. Síntese final

Eu descreveria o projeto da seguinte forma:

> O `tcc_receita` é uma plataforma de engenharia de dados baseada nos dados públicos do Cadastro Nacional da Pessoa Jurídica da Receita Federal. A plataforma realiza a extração e ingestão dos snapshots cadastrais, tributários e societários, organiza os dados em uma arquitetura em camadas e aplica transformações para padronizar e historizar as entidades por meio de SCD Tipo 2. A partir da comparação temporal dos registros, identifica eventos de negócio, como alterações cadastrais, mudanças de atividade econômica, transições de enquadramento tributário e movimentações societárias. Os dados históricos são posteriormente organizados em um modelo dimensional, permitindo análises temporais e de negócio, além de servirem como base para aplicações de machine learning e exploração de relações entre empresas.

Em termos de visão geral, existem três níveis de valor:

1. Infraestrutura de dados: transformar arquivos públicos volumosos e heterogêneos em tabelas estruturadas, rastreáveis e reutilizáveis.
2. Inteligência temporal: reconstruir a evolução das empresas e identificar eventos que não estão necessariamente explícitos nos snapshots originais.
3. Aplicações analíticas: utilizar essa história para entender o comportamento das empresas, estudar setores econômicos, analisar relações societárias e desenvolver modelos preditivos.

A meu ver, a parte mais característica do projeto é a segunda: transformar snapshots mensais em uma representação histórica de entidades e eventos de negócio. É isso que conecta a engenharia da Landing e da Bronze, o SCD2 da Intermediate, o log de eventos e a modelagem analítica da Silver Final.

Esse é o retrato mais completo que consigo reconstruir das nossas discussões até aqui, distinguindo o que foi implementado ou preparado do que ainda é planejado.
