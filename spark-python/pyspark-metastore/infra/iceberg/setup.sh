#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Starting Iceberg REST Catalog with MinIO..."
docker compose up -d --build

echo "==> Waiting for MinIO to be healthy..."
timeout 60 bash -c 'until docker inspect --format="{{.State.Health.Status}}" iceberg-minio 2>/dev/null | grep -q healthy; do sleep 2; done'
echo "    MinIO is ready."

echo "==> Waiting for MinIO init to complete..."
timeout 60 bash -c 'until [ "$(docker inspect --format="{{.State.Status}}" iceberg-minio-init 2>/dev/null)" = "exited" ]; do sleep 2; done'
echo "    MinIO bucket initialized."

echo "==> Waiting for Iceberg REST catalog..."
timeout 60 bash -c 'until curl -sf http://localhost:${REST_CATALOG_PORT:-8181}/v1/config > /dev/null 2>&1; do sleep 2; done'
echo "    Iceberg REST catalog is ready."

echo ""
echo "==> Verifying REST catalog..."
curl -s http://localhost:${REST_CATALOG_PORT:-8181}/v1/config | head -c 500
echo ""

echo ""
echo "============================================"
echo "  Iceberg REST Catalog is running!"
echo "============================================"
echo ""
echo "  REST Catalog:  http://localhost:${REST_CATALOG_PORT:-8181}"
echo "  MinIO API:     http://localhost:9000"
echo "  MinIO Console: http://localhost:9001"
echo ""
echo "  Example commands:"
echo "    docker exec -it spark-iceberg bash"
echo "    docker compose -f infra/iceberg/docker-compose.yml logs -f"
echo "    curl http://localhost:${REST_CATALOG_PORT:-8181}/v1/namespaces"
echo ""
