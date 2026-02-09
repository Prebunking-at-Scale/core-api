#!/usr/bin/env bash
set -euo pipefail

ENV="${1:-}"

if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
  echo "Usage: $0 <dev|prod>"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "$ENV" in
  dev)
    gcloud container clusters get-credentials dev-cluster --project pas-development-1 --location europe-west4-b
    ;;
  prod)
    gcloud container clusters get-credentials prod-cluster --project pas-production-1 --location europe-west4-b
    ;;
esac

kubectl apply -k "$SCRIPT_DIR/overlays/$ENV"
