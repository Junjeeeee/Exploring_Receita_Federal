# Scripts AWS — ligar e desligar o NAT Gateway

## Por quê
Os clusters clássicos do Databricks rodam em subnets privadas da VPC do workspace (us-east-2) e saem para a
internet (control plane do Databricks) pelo **NAT Gateway**. Ele custa ~US$ 36/mês ligado 24 h
(NAT US$ 0,045/h + Elastic IP US$ 0,005/h), mais que todo o resto do projeto. Como o Job roda de vez em
quando, o NAT é apagado entre execuções e recriado antes de rodar.

O SQL warehouse serverless e o dashboard **não** dependem do NAT.

## Quando usar
- **Antes de rodar o Job:** `scripts/aws/nat_ligar.sh` (leva ~2 min).
- **Quando o Job terminar:** `scripts/aws/nat_desligar.sh`.

## Pré-requisito
Perfil AWS CLI `tcc-admin` com `AmazonVPCFullAccess`, **ou** AWS CloudShell na região us-east-2
(lá não há perfil: rode com `AWS_PROFILE= scripts/aws/nat_ligar.sh`).

## Comandos
```bash
scripts/aws/nat_ligar.sh              # cria EIP + NAT e aponta a rota 0.0.0.0/0 para ele (idempotente)
scripts/aws/nat_ligar.sh --simular    # só mostra o que faria

scripts/aws/nat_desligar.sh           # mostra o NAT, pede para digitar "sim", apaga NAT e libera o EIP
scripts/aws/nat_desligar.sh --sim     # sem pergunta
scripts/aws/nat_desligar.sh --simular # só mostra o que faria
scripts/aws/nat_desligar.sh --forcar  # ignora clusters (instâncias EC2) ativos
```
Variáveis de ambiente opcionais (padrões no topo dos scripts): `AWS_PROFILE`, `AWS_REGION`, `VPC_ID`,
`SUBNET_PUBLICA`, `TABELA_ROTAS_PRIVADA`. Os scripts só mexem em recursos da `VPC_ID`.

## O que acontece se esquecer de ligar
O cluster do Job não sobe: fica em "Pending" até estourar o tempo e falha com mensagem de timeout, algo como
"cluster não conseguiu alcançar o control plane" / "Cluster launch timed out ... unable to reach the
Databricks control plane". Solução: rodar `nat_ligar.sh` e repetir o Job.

## Observações
- Os recursos criados levam as tags `Projeto=tcc_receita` e `Name=tcc-receita-nat`.
- Depois de apagar o NAT, a rota 0.0.0.0/0 da tabela privada fica em `blackhole` (não custa nada);
  o `nat_ligar.sh` a reaponta para o NAT novo.
- Se o `nat_ligar.sh` falhar após alocar o IP, ele avisa o ID do Elastic IP órfão (IP solto também cobra).
