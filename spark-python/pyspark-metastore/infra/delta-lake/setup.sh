#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Starting Delta Lake Spark container..."
docker compose up -d --build

echo "==> Waiting for Spark container to start..."
timeout 60 bash -c 'until docker inspect --format="{{.State.Running}}" spark-delta 2>/dev/null | grep -q true; do sleep 2; done'
echo "    Spark container is running."

echo "==> Running Delta Lake smoke test..."
docker exec spark-delta python3 -c "
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('delta-smoke-test') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()

df = spark.range(10)
df.write.format('delta').mode('overwrite').save('/tmp/delta-warehouse/smoke_test')
result = spark.read.format('delta').load('/tmp/delta-warehouse/smoke_test')
print(f'Delta Lake smoke test: {result.count()} rows written and read successfully.')
spark.stop()
" && echo "    Smoke test passed." || echo "    Smoke test failed (Delta JARs may not be in the image)."

echo ""
echo "============================================"
echo "  Delta Lake Spark is running!"
echo "============================================"
echo ""
echo "  Warehouse: /tmp/delta-warehouse"
echo ""
echo "  Example commands:"
echo "    docker exec -it spark-delta bash"
echo "    docker compose -f infra/delta-lake/docker-compose.yml logs -f"
echo ""
