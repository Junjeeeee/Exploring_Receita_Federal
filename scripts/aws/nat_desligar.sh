#!/usr/bin/env bash
# Apaga o NAT Gateway da VPC do workspace Databricks e libera o(s) Elastic IP(s) dele.
# Motivo: o NAT custa ~US$ 36/mês ligado 24 h. Veja scripts/aws/README.md.
#
# Uso: scripts/aws/nat_desligar.sh [--sim] [--forcar] [--simular]
#   --sim       não pergunta confirmação
#   --forcar    apaga mesmo com instâncias EC2 (clusters) rodando
#   --simular   só imprime os comandos que alterariam algo (os describe rodam de verdade)
set -euo pipefail

# --- Configuração (pode ser sobrescrita por variável de ambiente) ---
# AWS_PROFILE vazio (ex.: no CloudShell) = não passa --profile.
AWS_PROFILE="${AWS_PROFILE-tcc-admin}"
AWS_REGION="${AWS_REGION:-us-east-2}"
VPC_ID="${VPC_ID:-vpc-039d455d914677b57}"

SIMULAR=0
CONFIRMAR=1
FORCAR=0

for argumento in "$@"; do
  case "$argumento" in
    --simular|--dry-run) SIMULAR=1 ;;
    --sim) CONFIRMAR=0 ;;
    --forcar) FORCAR=1 ;;
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

# Procura NATs available/pending dentro da VPC (com ou sem tag).
NATS=""
EIPS=""
buscar_nats() {
  NATS=$(aws_cli ec2 describe-nat-gateways \
    --filter "Name=vpc-id,Values=$VPC_ID" "Name=state,Values=available,pending" \
    --query 'NatGateways[].NatGatewayId' --output text)
  # Guarda os allocation IDs dos EIPs (NAT privado não tem EIP: vem "None").
  EIPS=$(aws_cli ec2 describe-nat-gateways \
    --filter "Name=vpc-id,Values=$VPC_ID" "Name=state,Values=available,pending" \
    --query 'NatGateways[].NatGatewayAddresses[].AllocationId' --output text \
    | tr '\t' '\n' | grep -v '^None$' || true)
}

# 1. Aborta se houver clusters (instâncias EC2) ativos na VPC.
checar_instancias() {
  local instancias
  instancias=$(aws_cli ec2 describe-instances \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=instance-state-name,Values=running,pending" \
    --query 'Reservations[].Instances[].InstanceId' --output text)
  if [ -z "$instancias" ]; then
    return
  fi
  if [ "$FORCAR" -eq 0 ]; then
    echo "ERRO: há instâncias EC2 ativas na VPC $VPC_ID (provavelmente clusters Databricks):" >&2
    echo "  $instancias" >&2
    echo "Espere o Job terminar (ou use --forcar) e rode de novo." >&2
    exit 1
  fi
  echo "Aviso: instâncias ativas ignoradas por --forcar: $instancias"
}

confirmar() {
  echo "Região: $AWS_REGION | VPC: $VPC_ID"
  echo "NAT Gateway(s) a apagar: $NATS"
  echo "Elastic IP(s) a liberar: ${EIPS:-nenhum}"
  if [ "$CONFIRMAR" -eq 0 ] || [ "$SIMULAR" -eq 1 ]; then
    return
  fi
  read -r -p "Digite 'sim' para continuar: " resposta
  if [ "$resposta" != "sim" ]; then
    echo "Cancelado."
    exit 1
  fi
}

# 3. Apaga o NAT e espera o estado 'deleted' (o EIP só pode ser liberado depois).
apagar_nats() {
  local nat
  for nat in $NATS; do
    echo "Apagando $nat ..."
    executar ec2 delete-nat-gateway --nat-gateway-id "$nat" --query NatGateway.NatGatewayId --output text
  done
  # shellcheck disable=SC2086  # $NATS é uma lista separada por espaço, de propósito
  executar ec2 wait nat-gateway-deleted --nat-gateway-ids $NATS
}

# 4. Libera os EIPs que eram do NAT (EIP solto também cobra).
liberar_eips() {
  local eip
  for eip in $EIPS; do
    echo "Liberando $eip ..."
    executar ec2 release-address --allocation-id "$eip"
  done
}

main() {
  buscar_nats
  if [ -z "$NATS" ]; then
    echo "Nenhum NAT Gateway available/pending na VPC $VPC_ID. Nada a fazer."
    exit 0
  fi
  checar_instancias
  confirmar
  apagar_nats
  liberar_eips

  echo
  echo "=== Resumo ==="
  echo "NAT(s) apagado(s): $NATS"
  echo "EIP(s) liberado(s): ${EIPS:-nenhum}"
  # 5. A rota 0.0.0.0/0 da tabela privada vira 'blackhole'; não custa nada, o nat_ligar.sh a corrige.
  echo "A rota 0.0.0.0/0 da tabela de rotas privada ficou em 'blackhole' (sem custo); foi mantida."
  echo "ATENÇÃO: o cluster do Job NÃO sobe até rodar scripts/aws/nat_ligar.sh."
  if [ "$SIMULAR" -eq 1 ]; then
    echo "(modo simulação: nada foi alterado)"
  fi
}

main
