#!/usr/bin/env bash
# Cria o NAT Gateway (com Elastic IP) na VPC do workspace Databricks e aponta a rota 0.0.0.0/0
# da tabela de rotas privada para ele. Rode antes do Job. Veja scripts/aws/README.md.
#
# Uso: scripts/aws/nat_ligar.sh [--simular]
#   --simular   só imprime os comandos que alterariam algo (os describe rodam de verdade)
# Idempotente: se já existe NAT available na VPC, só garante a rota.
set -Eeuo pipefail

# --- Configuração (pode ser sobrescrita por variável de ambiente) ---
# AWS_PROFILE vazio (ex.: no CloudShell) = não passa --profile.
AWS_PROFILE="${AWS_PROFILE-tcc-admin}"
AWS_REGION="${AWS_REGION:-us-east-2}"
VPC_ID="${VPC_ID:-vpc-039d455d914677b57}"
SUBNET_PUBLICA="${SUBNET_PUBLICA:-subnet-0ee1f19e4fd13b9da}"
TABELA_ROTAS_PRIVADA="${TABELA_ROTAS_PRIVADA:-rtb-03ec9cbd942abaf44}"

TAG_PROJETO="tcc_receita"
TAG_NAME="tcc-receita-nat"

SIMULAR=0
for argumento in "$@"; do
  case "$argumento" in
    --simular|--dry-run) SIMULAR=1 ;;
    *) echo "Argumento desconhecido: $argumento" >&2; exit 1 ;;
  esac
done

# Chama o AWS CLI com região e (se houver) perfil.
aws_cli() {
  if [ -n "$AWS_PROFILE" ]; then
    aws --profile "$AWS_PROFILE" --region "$AWS_REGION" "$@"
  else
    aws --region "$AWS_REGION" "$@"
  fi
}

# Roda um comando que ALTERA a AWS; em modo simulação só imprime.
executar() {
  if [ "$SIMULAR" -eq 1 ]; then
    echo "[simulação] aws $*"
  else
    aws_cli "$@"
  fi
}

# Tags no formato do --tag-specifications, para o tipo de recurso informado.
tags() {
  echo "ResourceType=$1,Tags=[{Key=Projeto,Value=$TAG_PROJETO},{Key=Name,Value=$TAG_NAME}]"
}

EIP_ID=""
NAT_ID=""

# Se algo falhar depois de alocar o EIP, avisa o que ficou para trás (custa dinheiro).
avisar_orfaos() {
  echo >&2
  echo "FALHA no meio do processo. Recursos que podem ter ficado cobrando:" >&2
  if [ -n "$EIP_ID" ]; then
    echo "  Elastic IP: $EIP_ID (libere com: aws ec2 release-address --allocation-id $EIP_ID)" >&2
  fi
  if [ -n "$NAT_ID" ]; then
    echo "  NAT Gateway: $NAT_ID (apague com nat_desligar.sh)" >&2
  fi
}

# Devolve o ID de um NAT available/pending da VPC (vazio se não houver).
buscar_nat_existente() {
  aws_cli ec2 describe-nat-gateways \
    --filter "Name=vpc-id,Values=$VPC_ID" "Name=state,Values=available,pending" \
    --query 'NatGateways[0].NatGatewayId' --output text | sed 's/^None$//'
}

# 1 e 2. Aloca o Elastic IP e cria o NAT na subnet pública. Preenche EIP_ID e NAT_ID.
criar_nat() {
  echo "Alocando Elastic IP ..."
  if [ "$SIMULAR" -eq 1 ]; then
    executar ec2 allocate-address --domain vpc --tag-specifications "$(tags elastic-ip)"
    EIP_ID="eipalloc-SIMULADO"
  else
    EIP_ID=$(aws_cli ec2 allocate-address --domain vpc \
      --tag-specifications "$(tags elastic-ip)" --query AllocationId --output text)
  fi
  # A partir daqui, qualquer erro avisa sobre o EIP órfão.
  trap avisar_orfaos ERR

  echo "Criando NAT Gateway na $SUBNET_PUBLICA ..."
  if [ "$SIMULAR" -eq 1 ]; then
    executar ec2 create-nat-gateway --subnet-id "$SUBNET_PUBLICA" --allocation-id "$EIP_ID" \
      --tag-specifications "$(tags natgateway)"
    NAT_ID="nat-SIMULADO"
  else
    NAT_ID=$(aws_cli ec2 create-nat-gateway --subnet-id "$SUBNET_PUBLICA" --allocation-id "$EIP_ID" \
      --tag-specifications "$(tags natgateway)" --query NatGateway.NatGatewayId --output text)
  fi
}

# 4. Aponta 0.0.0.0/0 da tabela privada para o NAT (replace se a rota existe, senão create).
garantir_rota() {
  local nat_id="$1" rota_atual
  rota_atual=$(aws_cli ec2 describe-route-tables --route-table-ids "$TABELA_ROTAS_PRIVADA" \
    --query "RouteTables[0].Routes[?DestinationCidrBlock=='0.0.0.0/0'].NatGatewayId" --output text)
  if [ "$rota_atual" = "$nat_id" ]; then
    echo "A rota 0.0.0.0/0 já aponta para $nat_id."
  elif [ -n "$rota_atual" ]; then
    # A rota existe (blackhole do NAT apagado ou outro NAT): troca o destino.
    echo "Trocando o destino da rota 0.0.0.0/0 para $nat_id ..."
    executar ec2 replace-route --route-table-id "$TABELA_ROTAS_PRIVADA" \
      --destination-cidr-block 0.0.0.0/0 --nat-gateway-id "$nat_id"
  else
    echo "Criando a rota 0.0.0.0/0 -> $nat_id ..."
    executar ec2 create-route --route-table-id "$TABELA_ROTAS_PRIVADA" \
      --destination-cidr-block 0.0.0.0/0 --nat-gateway-id "$nat_id"
  fi
}

# 5. Confere que a rota está 'active'.
conferir_rota() {
  if [ "$SIMULAR" -eq 1 ]; then
    echo "[simulação] conferência da rota pulada."
    return
  fi
  local estado
  estado=$(aws_cli ec2 describe-route-tables --route-table-ids "$TABELA_ROTAS_PRIVADA" \
    --query "RouteTables[0].Routes[?DestinationCidrBlock=='0.0.0.0/0'].State" --output text)
  if [ "$estado" != "active" ]; then
    echo "ERRO: a rota 0.0.0.0/0 está '$estado', esperado 'active'." >&2
    return 1
  fi
  echo "Rota 0.0.0.0/0 da $TABELA_ROTAS_PRIVADA: active."
}

main() {
  NAT_ID=$(buscar_nat_existente)
  if [ -n "$NAT_ID" ]; then
    echo "Já existe NAT na VPC: $NAT_ID. Só vou garantir a rota."
  else
    criar_nat
  fi
  # 3. Espera ficar available (~1-2 min; se já estava, volta na hora).
  echo "Esperando o NAT ficar available ..."
  executar ec2 wait nat-gateway-available --nat-gateway-ids "$NAT_ID"
  garantir_rota "$NAT_ID"
  conferir_rota
  trap - ERR

  echo
  echo "=== Resumo ==="
  echo "NAT Gateway: $NAT_ID | EIP criado agora: ${EIP_ID:-nenhum}"
  echo "Pronto: o cluster do Job já consegue subir. Ao terminar, rode scripts/aws/nat_desligar.sh."
  if [ "$SIMULAR" -eq 1 ]; then
    echo "(modo simulação: nada foi alterado)"
  fi
}

main
