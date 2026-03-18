#!/usr/bin/env bash
# Run all standalone example scripts under src/ and report pass/fail.
#
# Usage:
#   ./scripts/run_all_examples.sh
#   ./scripts/run_all_examples.sh src/data_frame/joins/   # limit to a sub-tree
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SEARCH_ROOT="${PROJECT_ROOT}/${1:-src}"

export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH:-}"
export SPARK_MASTER="${SPARK_MASTER:-local[*]}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-python3}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"

passed=0
failed=0
skipped=0
failures=()

while IFS= read -r script; do
  # Only run files that have an if __name__ == "__main__" entry point
  if ! grep -q '__name__.*__main__' "$script"; then
    skipped=$((skipped + 1))
    continue
  fi

  rel="${script#"${PROJECT_ROOT}/"}"
  printf "%-70s " "$rel"

  if python3 "$script" > /tmp/pyspark_example_out.txt 2>&1; then
    echo "PASS"
    passed=$((passed + 1))
  else
    echo "FAIL"
    failed=$((failed + 1))
    failures+=("$rel")
  fi
done < <(find "$SEARCH_ROOT" -name "*.py" ! -path "*__pycache__*" | sort)

echo ""
echo "Results: ${passed} passed, ${failed} failed, ${skipped} skipped (no entry point)"

if [ ${#failures[@]} -gt 0 ]; then
  echo ""
  echo "Failed scripts:"
  for f in "${failures[@]}"; do
    echo "  - $f"
  done
  exit 1
fi
