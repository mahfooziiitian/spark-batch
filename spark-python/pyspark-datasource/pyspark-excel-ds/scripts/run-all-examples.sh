#!/usr/bin/env bash
set -euo pipefail

# Runs every example script under examples/, printing a pass/fail summary.
# Usage:
#   ./scripts/run-all-examples.sh            # run every example
#   ./scripts/run-all-examples.sh 02_table_integration   # run one category

cd "$(dirname "$0")/.."

CATEGORY="${1:-}"
SEARCH_DIR="examples"
if [[ -n "$CATEGORY" ]]; then
    SEARCH_DIR="examples/${CATEGORY}"
fi

pass=0
fail=0
failed_files=()

while IFS= read -r -d '' file; do
    echo "▶ Running ${file}"
    if uv run python "${file}" >/tmp/pys_excel_example.log 2>&1; then
        pass=$((pass + 1))
        echo "  ✓ passed"
    else
        fail=$((fail + 1))
        failed_files+=("${file}")
        echo "  ✗ failed (see /tmp/pys_excel_example.log)"
    fi
done < <(find "${SEARCH_DIR}" -name "*.py" -print0 | sort -z)

echo ""
echo "Summary: ${pass} passed, ${fail} failed"
if [[ ${fail} -gt 0 ]]; then
    printf '  - %s\n' "${failed_files[@]}"
    exit 1
fi
