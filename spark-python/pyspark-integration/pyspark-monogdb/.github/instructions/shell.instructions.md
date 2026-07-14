---
applyTo: "**/*.sh"
---

# PySpark MongoDB — Shell Script Instructions

## Shebang & Safety

Every executable shell script must start with:

```bash
#!/usr/bin/env bash
set -euo pipefail
```

- `set -e` — exit immediately on any error.
- `set -u` — treat unset variables as errors.
- `set -o pipefail` — propagate errors through pipes.

**Exception:** scripts designed to be *sourced* (e.g. `source setup-env.sh`) must not
use `set -euo pipefail` at the top level. Add a comment explaining the sourcing intent.

## Script-Relative Paths

```bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
```

Use `$SCRIPT_DIR/...` for all relative file references. Never rely on `$PWD`.

## Checking Prerequisites

### Java

```bash
if ! command -v java &>/dev/null; then
  echo "ERROR: Java is not installed. Install Java 11 first." >&2
  exit 1
fi
java -version 2>&1 | head -1
```

### Python

```bash
PYTHON_BIN="$(command -v python3 2>/dev/null || command -v python)"
if [[ -z "$PYTHON_BIN" ]]; then
  echo "ERROR: Python 3 is not installed." >&2
  exit 1
fi
"$PYTHON_BIN" --version
```

### Docker

```bash
if ! command -v docker &>/dev/null; then
  echo "ERROR: Docker is not installed." >&2
  exit 1
fi
docker --version
```

## Default Values

Use `${VAR:-default}` for optional env vars:

```bash
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"
MONGO_DB="${MONGO_DB:-tutorial}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
```

## Argument Parsing

For scripts that accept options, use a `while` + `case` loop:

```bash
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mongo-uri) MONGO_URI="$2"; shift 2 ;;
    --database)  MONGO_DB="$2";  shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done
```

## Smoke Tests

Inline Python smoke test to verify PySpark + MongoDB connectivity:

```bash
python3 - <<'PYEOF'
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .master("local[*]")
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.1.1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
print("Spark", spark.version, "OK")
spark.stop()
PYEOF
```

## Common Script Patterns

### Start infrastructure

```bash
echo "Starting MongoDB stack..."
docker compose -f "$SCRIPT_DIR/../infra/docker/docker-compose.yml" up -d
echo "Waiting for MongoDB to be ready..."
sleep 5
```

### Run PySpark job

```bash
uv run python "$SCRIPT_DIR/../src/mongondb/mongodb_collection.py"
```
