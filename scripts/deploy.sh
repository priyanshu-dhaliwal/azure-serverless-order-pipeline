#!/usr/bin/env bash
# Deploy the order pipeline.
#
#   ./scripts/deploy.sh dev what-if
#   ./scripts/deploy.sh dev
set -euo pipefail

ENV="${1:-dev}"
MODE="${2:-deploy}"
RG="${RG:-rg-orders-${ENV}}"
PARAMS="infra/params/${ENV}.bicepparam"

[ -f "$PARAMS" ] || { echo "missing $PARAMS" >&2; exit 1; }

az bicep build --file infra/main.bicep --stdout > /dev/null
echo "==> bicep ok"

az group create --name "$RG" --location "${LOCATION:-centralindia}" --output none

if [ "$MODE" = "what-if" ]; then
  az deployment group what-if -g "$RG" -f infra/main.bicep -p "$PARAMS" \
    --result-format FullResourcePayloads
else
  az deployment group create -g "$RG" -f infra/main.bicep -p "$PARAMS" -o table

  APP=$(az deployment group show -g "$RG" -n main \
    --query properties.outputs.functionAppName.value -o tsv 2>/dev/null || true)
  if [ -z "$APP" ]; then
    APP=$(az deployment group list -g "$RG" --query '[0].name' -o tsv | head -1)
    APP=$(az deployment group show -g "$RG" -n "$APP" \
      --query properties.outputs.functionAppName.value -o tsv)
  fi

  echo "==> publishing function code to $APP"
  (cd src && func azure functionapp publish "$APP" --python)
fi
