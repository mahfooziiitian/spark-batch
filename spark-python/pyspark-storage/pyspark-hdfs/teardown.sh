#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

HA_MODE=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --ha) HA_MODE=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

if [[ "$HA_MODE" == true ]]; then
  echo "Stopping HDFS HA cluster..."
  docker compose -f docker-compose.ha.yml down -v
else
  echo "Stopping HDFS cluster..."
  docker compose -f docker-compose.yml down -v
fi
