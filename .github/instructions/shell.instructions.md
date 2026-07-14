---
applyTo: "**/*.sh"
---

# Shell Script Instructions

## Shebang & Safety

Every executable shell script must start with:

```bash
#!/usr/bin/env bash
set -euo pipefail
```

- `set -e` — exit immediately on any error.
- `set -u` — treat unset variables as errors.
- `set -o pipefail` — propagate errors through pipes.

**Exception:** scripts designed to be *sourced* (e.g. `source setup-yarn-env.sh`) must
not use `set -euo pipefail` at the top level, as this would affect the caller's shell.
Add a comment explaining the sourcing intent instead.

## Script-Relative Paths

To reference files relative to the script's own location:

```bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
```

Use `$SCRIPT_DIR/...` for all relative file references. Never rely on `$PWD`.

## Checking Prerequisites

### Java
```bash
if ! command -v java &>/dev/null; then
  echo "ERROR: Java is not installed. Install Java 11 or 17 first." >&2
  echo "  macOS:   brew install openjdk@11" >&2
  echo "  Ubuntu:  sudo apt-get install -y openjdk-11-jdk" >&2
  exit 1
fi
java -version 2>&1 | head -1
export JAVA_HOME="${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which java)")")")}"
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

## Argument Parsing

For scripts that accept options, use a `while` + `case` loop:

```bash
PYSPARK_VERSION="${PYSPARK_VERSION:-3.5.0}"
PROXY_URL=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) PYSPARK_VERSION="$2"; shift 2 ;;
    --proxy)   PROXY_URL="$2";       shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done
```

## Default Values

Use `${VAR:-default}` for optional env vars:

```bash
YARN_QUEUE="${YARN_QUEUE:-default}"
OUTPUT_PATH="${OUTPUT_PATH:-/tmp/output}"
```

## Environment Variable Exports (sourced scripts)

Sourced setup scripts should print what they set:

```bash
echo "JAVA_HOME       = ${JAVA_HOME:-<not set>}"
echo "SPARK_HOME      = $SPARK_HOME"
echo "PYSPARK_PYTHON  = $PYSPARK_PYTHON"
```

## Smoke Tests

Inline Python smoke tests using a heredoc avoid temporary files:

```bash
python3 - <<'PYEOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").config("spark.ui.enabled","false").getOrCreate()
print("Spark", spark.version, "OK")
spark.stop()
PYEOF
```
