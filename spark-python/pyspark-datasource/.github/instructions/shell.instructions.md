---
applyTo: "**/*.sh"
---

# Shell Script Conventions

## Shebang and Safety

Every shell script starts with:

```bash
#!/usr/bin/env bash
set -euo pipefail
```

- `set -e` — exit on first error.
- `set -u` — treat unset variables as errors.
- `set -o pipefail` — propagate pipe failures.

## Script-Relative Paths

Resolve the script's own directory for reliable relative paths:

```bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
```

## Prerequisite Checks

Verify required tools are installed before proceeding:

```bash
check_java() {
    if ! command -v java &>/dev/null; then
        echo "ERROR: Java is not installed. Install JDK 11+." >&2
        exit 1
    fi
    local java_version
    java_version=$(java -version 2>&1 | head -1)
    echo "Java: ${java_version}"
}

check_python() {
    if ! command -v python3 &>/dev/null; then
        echo "ERROR: Python 3 is not installed." >&2
        exit 1
    fi
    echo "Python: $(python3 --version)"
}

check_spark() {
    if ! command -v spark-submit &>/dev/null; then
        echo "WARNING: spark-submit not found in PATH. Using PySpark bundled Spark."
    fi
}
```

## Argument Parsing

Use `while` + `case` for argument parsing:

```bash
VERBOSE=false
INPUT_PATH=""
OUTPUT_FORMAT="parquet"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--input)
            INPUT_PATH="$2"
            shift 2
            ;;
        -f|--format)
            OUTPUT_FORMAT="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done
```

## Default Values

Use `${VAR:-default}` for default values:

```bash
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
INPUT_PATH="${INPUT_PATH:-/tmp/spark-input}"
OUTPUT_PATH="${OUTPUT_PATH:-/tmp/spark-output}"
LOG_LEVEL="${LOG_LEVEL:-WARN}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
```

## Logging Functions

```bash
info()  { echo "[INFO]  $(date '+%H:%M:%S') $*"; }
warn()  { echo "[WARN]  $(date '+%H:%M:%S') $*" >&2; }
error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }
die()   { error "$@"; exit 1; }
```

## Inline Python Smoke Tests

Run a quick PySpark validation using a heredoc:

```bash
run_smoke_test() {
    info "Running PySpark smoke test..."
    "${PYTHON_BIN}" <<'PYTHON'
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("smoke-test")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.createDataFrame([("ok",)], ["status"])
assert df.count() == 1, "Smoke test failed"
print("Smoke test passed")

spark.stop()
PYTHON
}
```

## Running Child Project Scripts

```bash
run_child_project() {
    local project_dir="$1"
    local script="$2"

    info "Running ${project_dir}/${script}..."
    cd "${PROJECT_DIR}/${project_dir}"

    if [[ -f "pyproject.toml" ]] && command -v uv &>/dev/null; then
        uv run python "${script}"
    elif [[ -f "pyproject.toml" ]] && command -v poetry &>/dev/null; then
        poetry run python "${script}"
    else
        python3 "${script}"
    fi
}
```

## Cleanup Traps

Register cleanup on exit:

```bash
cleanup() {
    info "Cleaning up temporary files..."
    rm -rf "${WORK_DIR:-}"
}
trap cleanup EXIT
```
