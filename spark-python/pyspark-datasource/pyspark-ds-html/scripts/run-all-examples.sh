#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Run all examples in a category or all categories
#
# Usage:
#   ./scripts/run-all-examples.sh                      # Run all examples
#   ./scripts/run-all-examples.sh 01_data_source        # Run only data-source examples
#
# Java is auto-detected by pys_html.config.configure_env() (JAVA_HOME if already
# set, otherwise the "java" executable on PATH). Set JAVA_HOME explicitly only if
# auto-detection picks the wrong JDK.
#
# Environment variables:
#   JAVA_HOME     - (optional) explicit JDK 17+ home, auto-detected otherwise
#   DATA_HOME     - Data directory
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
EXAMPLES_DIR="${PROJECT_ROOT}/examples"

export DATA_HOME="${DATA_HOME:-${PROJECT_ROOT}/data}"

CATEGORY="${1:-}"
if [[ -n "${CATEGORY}" ]]; then
    SEARCH_DIR="${EXAMPLES_DIR}/${CATEGORY}"
else
    SEARCH_DIR="${EXAMPLES_DIR}"
fi

if [[ ! -d "${SEARCH_DIR}" ]]; then
    echo "✗ Directory not found: ${SEARCH_DIR}"
    exit 1
fi

PASSED=0
FAILED=0
FAILURES=()

echo "╭─────────────────────────────────────────────────╮"
echo "│  Running PySpark HTML Examples                   │"
echo "├─────────────────────────────────────────────────┤"
echo "│  Directory: ${SEARCH_DIR}"
echo "╰─────────────────────────────────────────────────╯"
echo ""

while IFS= read -r -d '' py_file; do
    if [[ ! -s "${py_file}" ]]; then
        continue
    fi

    relative="${py_file#"${PROJECT_ROOT}/"}"
    printf "  %-60s " "${relative}"

    if uv run python "${py_file}" > /dev/null 2>&1; then
        echo "✓"
        PASSED=$((PASSED + 1))
    else
        echo "✗"
        FAILED=$((FAILED + 1))
        FAILURES+=("${relative}")
    fi
done < <(find "${SEARCH_DIR}" -name "*.py" -type f -print0 | sort -z)

echo ""
echo "────────────────────────────────────────────────────"
echo "  Results: ${PASSED} passed, ${FAILED} failed"
echo "────────────────────────────────────────────────────"

if [[ ${FAILED} -gt 0 ]]; then
    echo ""
    echo "  Failed:"
    for f in "${FAILURES[@]}"; do
        echo "    ✗ ${f}"
    done
    exit 1
fi
