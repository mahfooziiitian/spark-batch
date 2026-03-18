#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Starting REST Catalog (local filesystem)..."
docker compose up -d --build

echo "==> Waiting for REST catalog to be healthy..."
timeout 60 bash -c 'until curl -sf http://localhost:${REST_CATALOG_PORT:-8181}/v1/config > /dev/null 2>&1; do sleep 2; done'
echo "    REST catalog is ready."

echo ""
echo "==> Verifying REST catalog..."
curl -s http://localhost:${REST_CATALOG_PORT:-8181}/v1/config | head -c 500
echo ""

echo ""
echo "============================================"
echo "  REST Catalog is running!"
echo "============================================"
echo ""
echo "  REST Catalog: http://localhost:${REST_CATALOG_PORT:-8181}"
echo ""
echo "  Example commands:"
echo "    docker exec -it spark-rest bash"
echo "    docker compose -f infra/rest/docker-compose.yml logs -f"
echo "    curl http://localhost:${REST_CATALOG_PORT:-8181}/v1/namespaces"
echo ""
