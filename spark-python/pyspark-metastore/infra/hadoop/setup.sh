#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Hadoop HDFS Cluster Setup ==="

docker compose up -d

echo "Waiting for NameNode to be healthy..."
until docker inspect --format='{{.State.Health.Status}}' hadoop-namenode 2>/dev/null | grep -q "healthy"; do
    sleep 3
done

echo "Creating /warehouse directory in HDFS..."
docker compose exec namenode hdfs dfs -mkdir -p /warehouse

echo ""
echo "=== HDFS Cluster Ready ==="
echo ""
echo "Connection info:"
echo "  NameNode RPC:  hdfs://localhost:8020"
echo "  NameNode Web:  http://localhost:9870"
echo "  Warehouse:     hdfs://namenode:8020/warehouse"
echo ""
echo "Example commands:"
echo "  docker compose exec namenode hdfs dfs -ls /"
echo "  docker compose exec spark bash"
echo "  docker compose exec spark python src/metastore/hadoop/hadoop_metastore.py"
