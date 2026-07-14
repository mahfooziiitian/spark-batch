---
applyTo: "{**/Dockerfile,**/*.sh,**/docker-compose*.yml,pyproject.toml}"
---

# Infrastructure — PySpark Metastore

## Docker Compose for Metastore Services

When adding Docker infrastructure, use a `docker-compose.yml` that provides both a Hive
Metastore backend database and the Spark environment.

### Standard service topology

```yaml
services:
  metastore-db:
    image: postgres:15
    environment:
      POSTGRES_DB: metastore
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: "${METASTORE_DB_PASSWORD:-hive}"
    ports:
      - "5432:5432"
    volumes:
      - metastore-data:/var/lib/postgresql/data

  spark:
    image: apache/spark:3.5.0-python3
    environment:
      PYSPARK_PYTHON: python3
      PYSPARK_DRIVER_PYTHON: python3
      SPARK_LOCAL_IP: "127.0.0.1"
      JAVA_HOME_11: "/usr/lib/jvm/java-11-openjdk-amd64"
    volumes:
      - ./src:/workspace/src
      - ./tests:/workspace/tests
    working_dir: /workspace
    depends_on:
      - metastore-db

volumes:
  metastore-data:
```

### Naming conventions

- Service names: lowercase with hyphens (`metastore-db`, `hive-metastore`).
- Volume names: lowercase with hyphens.
- Use `${VAR:-default}` for passwords and configurable values.

## Dockerfile — Metastore Image

Extend the base Spark image with metastore dependencies:

```dockerfile
FROM apache/spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir \
      "pyspark<4.0.0"   \
      "pyarrow>=4.0.0"  \
      "pandas>=1.3.0"   \
    && ln -s "$(which python3)" /usr/local/bin/python

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

WORKDIR /workspace
COPY src/ ./src/
COPY tests/ ./tests/
COPY pyproject.toml .

USER spark
```

Include Hive/Iceberg JARs when the image needs catalog connectivity:

```dockerfile
RUN curl -sL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar \
    -o "${SPARK_HOME}/jars/iceberg-spark-runtime.jar"
```

## Shell Scripts

### Shebang & safety

```bash
#!/usr/bin/env bash
set -euo pipefail
```

### Metastore health check

```bash
METASTORE_HOST="${METASTORE_HOST:-localhost}"
METASTORE_PORT="${METASTORE_PORT:-9083}"

echo "Checking Hive Metastore at ${METASTORE_HOST}:${METASTORE_PORT}..."
if nc -z "$METASTORE_HOST" "$METASTORE_PORT" 2>/dev/null; then
  echo "✓ Metastore is reachable"
else
  echo "✗ Metastore is NOT reachable" >&2
  exit 1
fi
```

### Database initialisation

```bash
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-metastore}"
DB_USER="${DB_USER:-hive}"

echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -q; do
  sleep 2
done
echo "PostgreSQL is ready."
```

### Spark smoke test with metastore

```bash
python3 - <<'PYEOF'
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("metastore-smoke-test")
         .master("local[*]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())

catalogs = spark.sql("SHOW CATALOGS").collect()
print(f"Catalogs: {[c[0] for c in catalogs]}")

spark.sql("CREATE TABLE IF NOT EXISTS default.smoke_test (id INT, val STRING)")
spark.sql("INSERT INTO default.smoke_test VALUES (1, 'ok')")
assert spark.sql("SELECT * FROM default.smoke_test").count() == 1
spark.sql("DROP TABLE IF EXISTS default.smoke_test")

print("Metastore smoke test passed ✓")
spark.stop()
PYEOF
```

## pyproject.toml — Task Runner

Use taskipy tasks for all operations:

```bash
uv run task test            # pytest
uv run task quality         # isort + ruff + flake8 + mypy + sqlfluff
uv run task secure          # bandit + safety
uv run task build           # uv build
uv run task sql_lint        # sqlfluff lint
uv run task sql_format      # sqlfluff fix
```

When adding new tasks, follow the existing echo-delimiter pattern:

```toml
new_task = "echo '========== Starting new_task =========='; uv run <command>; echo '========== Finished new_task =========='"
```

## Environment Variables Reference

| Variable | Default | Purpose |
|---|---|---|
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `SPARK_WAREHOUSE` | `/tmp/spark-warehouse` | Warehouse directory |
| `JAVA_HOME_11` | — | Java 11 home path |
| `JAVA_HOME_17` | — | Java 17 home path |
| `JDBC_URL` | `jdbc:postgresql://localhost:5432/metastore` | JDBC connection |
| `JDBC_USER` | `username` | JDBC username |
| `JDBC_PASSWORD` | `password` | JDBC password |
| `METASTORE_HOST` | `localhost` | Hive Metastore hostname |
| `METASTORE_PORT` | `9083` | Hive Metastore Thrift port |
| `PYSPARK_PYTHON` | `python3` | Python binary for executors |
| `PYSPARK_DRIVER_PYTHON` | `python3` | Python binary for driver |
| `SPARK_LOCAL_IP` | `127.0.0.1` | Bind address for local mode |

## .dockerignore

Always include alongside any Dockerfile:

```
.venv/
__pycache__/
*.pyc
.git/
*.egg-info/
dist/
build/
.pytest_cache/
metastore_db/
spark-warehouse/
derby.log
```
