---
applyTo: "**/*.sh"
---

# Shell Script Instructions (Root-Level Defaults)

These are baseline shell script conventions for all child projects.

## Shebang & Safety

Always start with:

```bash
#!/usr/bin/env bash
set -euo pipefail
```

- `set -e` — exit on first error.
- `set -u` — treat unset variables as errors.
- `set -o pipefail` — propagate pipe failures.

## Environment Variables

Use environment variables with sensible defaults:

```bash
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
INPUT_PATH="${INPUT_PATH:-/tmp/input}"
OUTPUT_PATH="${OUTPUT_PATH:-/tmp/output}"
```

## Java Dependency

PySpark requires Java. Check before running:

```bash
if ! command -v java &> /dev/null; then
    echo "ERROR: Java is required but not found. Install Java 11." >&2
    exit 1
fi
```

## Python Environment

Activate the appropriate virtual environment or use the project's package manager:

```bash
# For pip-based projects
python3 -m pip install -r requirements.txt

# For poetry-based projects
poetry install

# For uv-based projects
uv sync
```

## Logging

Use `echo` to stderr for diagnostic messages, stdout for data:

```bash
echo "INFO: Starting job..." >&2
echo "ERROR: File not found: ${file}" >&2
```

## Quoting

Always quote variable expansions to prevent word splitting:

```bash
spark-submit --master "${SPARK_MASTER}" "${SCRIPT_PATH}"
```

## Temporary Files

Clean up temporary files on exit:

```bash
TMPDIR=$(mktemp -d)
trap 'rm -rf "${TMPDIR}"' EXIT
```
