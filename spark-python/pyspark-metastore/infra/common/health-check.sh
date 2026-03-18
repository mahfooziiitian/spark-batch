#!/usr/bin/env bash
set -euo pipefail

# Usage: ./health-check.sh [service]
# Services: postgres, mysql, hive-metastore, spark, iceberg-rest, hdfs

SERVICE="${1:-all}"
TIMEOUT="${TIMEOUT:-30}"

check_port() {
    local host="$1" port="$2" name="$3"
    if nc -z -w 5 "$host" "$port" 2>/dev/null; then
        echo "✓ $name ($host:$port) is reachable"
        return 0
    else
        echo "✗ $name ($host:$port) is NOT reachable" >&2
        return 1
    fi
}

wait_for_port() {
    local host="$1" port="$2" name="$3"
    local elapsed=0
    echo "Waiting for $name at $host:$port (timeout: ${TIMEOUT}s)..."
    until nc -z -w 2 "$host" "$port" 2>/dev/null; do
        sleep 2
        elapsed=$((elapsed + 2))
        if [ "$elapsed" -ge "$TIMEOUT" ]; then
            echo "✗ Timed out waiting for $name" >&2
            return 1
        fi
    done
    echo "✓ $name is ready (${elapsed}s)"
}

case "$SERVICE" in
    postgres)
        wait_for_port "${HIVE_METASTORE_DB_HOST:-localhost}" "${HIVE_METASTORE_DB_PORT:-5432}" "PostgreSQL"
        ;;
    mysql)
        wait_for_port "${MYSQL_HOST:-localhost}" "${MYSQL_PORT:-3306}" "MySQL"
        ;;
    hive-metastore)
        wait_for_port "${HIVE_METASTORE_HOST:-localhost}" "${HIVE_METASTORE_PORT:-9083}" "Hive Metastore"
        ;;
    iceberg-rest)
        wait_for_port "${REST_CATALOG_HOST:-localhost}" "${REST_CATALOG_PORT:-8181}" "Iceberg REST Catalog"
        ;;
    hdfs)
        wait_for_port "${HDFS_NAMENODE_HOST:-localhost}" "${HDFS_NAMENODE_PORT:-8020}" "HDFS NameNode"
        ;;
    spark)
        # Smoke test Spark
        python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').config('spark.ui.enabled','false').getOrCreate()
print('✓ Spark', spark.version, 'is working')
spark.stop()
"
        ;;
    all)
        echo "=== Checking all services ==="
        check_port "${HIVE_METASTORE_DB_HOST:-localhost}" "${HIVE_METASTORE_DB_PORT:-5432}" "PostgreSQL" || true
        check_port "${HIVE_METASTORE_HOST:-localhost}" "${HIVE_METASTORE_PORT:-9083}" "Hive Metastore" || true
        check_port "${REST_CATALOG_HOST:-localhost}" "${REST_CATALOG_PORT:-8181}" "Iceberg REST" || true
        check_port "${HDFS_NAMENODE_HOST:-localhost}" "${HDFS_NAMENODE_PORT:-8020}" "HDFS NameNode" || true
        check_port "${JDBC_HOST:-localhost}" "${JDBC_PORT:-5432}" "JDBC Database" || true
        ;;
    *)
        echo "Unknown service: $SERVICE"
        echo "Usage: $0 [postgres|mysql|hive-metastore|iceberg-rest|hdfs|spark|all]"
        exit 1
        ;;
esac
