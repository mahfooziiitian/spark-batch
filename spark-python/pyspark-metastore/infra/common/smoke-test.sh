#!/usr/bin/env bash
set -euo pipefail

echo "=== PySpark Metastore Smoke Test ==="

python3 - <<'PYEOF'
import sys
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("smoke-test")
         .master("local[2]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.warehouse.dir", "/tmp/smoke-test-warehouse")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

errors = []

# Test 1: Catalog listing
try:
    catalogs = spark.sql("SHOW CATALOGS").collect()
    print(f"✓ SHOW CATALOGS: {[c[0] for c in catalogs]}")
except Exception as e:
    errors.append(f"SHOW CATALOGS: {e}")
    print(f"✗ SHOW CATALOGS: {e}")

# Test 2: Database operations
try:
    spark.sql("CREATE DATABASE IF NOT EXISTS smoke_test_db")
    dbs = [db.name for db in spark.catalog.listDatabases()]
    assert "smoke_test_db" in dbs
    print("✓ CREATE DATABASE")
except Exception as e:
    errors.append(f"CREATE DATABASE: {e}")
    print(f"✗ CREATE DATABASE: {e}")

# Test 3: Table operations
try:
    spark.sql("CREATE TABLE IF NOT EXISTS smoke_test_db.test_tbl (id INT, name STRING)")
    spark.sql("INSERT INTO smoke_test_db.test_tbl VALUES (1, 'smoke'), (2, 'test')")
    count = spark.sql("SELECT * FROM smoke_test_db.test_tbl").count()
    assert count == 2
    print(f"✓ Table CRUD (rows: {count})")
except Exception as e:
    errors.append(f"Table CRUD: {e}")
    print(f"✗ Table CRUD: {e}")

# Test 4: Catalog API
try:
    tables = spark.catalog.listTables("smoke_test_db")
    assert any(t.name == "test_tbl" for t in tables)
    print("✓ Catalog API (listTables)")
except Exception as e:
    errors.append(f"Catalog API: {e}")
    print(f"✗ Catalog API: {e}")

# Cleanup
spark.sql("DROP TABLE IF EXISTS smoke_test_db.test_tbl")
spark.sql("DROP DATABASE IF EXISTS smoke_test_db CASCADE")
spark.stop()

if errors:
    print(f"\n✗ {len(errors)} test(s) failed")
    sys.exit(1)
else:
    print("\n✓ All smoke tests passed")
PYEOF
