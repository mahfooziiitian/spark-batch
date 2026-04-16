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

# ── Prerequisites ──────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
  echo "ERROR: docker is not installed or not in PATH" >&2
  exit 1
fi

if [[ "$HA_MODE" == true ]]; then
  COMPOSE_FILE="docker-compose.ha.yml"
  echo "Starting HDFS HA cluster (2 NameNodes, 3 JournalNodes)..."

  # ── Start all services (depends_on handles ordering) ────────
  docker compose -f "$COMPOSE_FILE" up -d

  # ── Wait for NameNode 1 ────────────────────────────────────
  echo "Waiting for NameNode 1 to become healthy..."
  for i in $(seq 1 60); do
    if curl -sf http://localhost:9870/ >/dev/null 2>&1; then
      echo "  NameNode 1 is healthy."
      break
    fi
    if [ "$i" -eq 60 ]; then
      echo "ERROR: NameNode 1 did not become healthy in time." >&2
      docker compose -f "$COMPOSE_FILE" logs namenode1 | tail -20
      exit 1
    fi
    sleep 5
  done

  # ── Wait for NameNode 2 ────────────────────────────────────
  echo "Waiting for NameNode 2 to become healthy..."
  for i in $(seq 1 60); do
    if curl -sf http://localhost:9871/ >/dev/null 2>&1; then
      echo "  NameNode 2 is healthy."
      break
    fi
    if [ "$i" -eq 60 ]; then
      echo "ERROR: NameNode 2 did not become healthy in time." >&2
      docker compose -f "$COMPOSE_FILE" logs namenode2 | tail -20
      exit 1
    fi
    sleep 5
  done

  # ── Transition NameNode 1 to Active ────────────────────────
  echo "Transitioning NameNode 1 to active..."
  docker compose -f "$COMPOSE_FILE" exec namenode1 \
    hdfs haadmin -transitionToActive nn1

  # ── Create HDFS directories and upload data ────────────────
  echo "Creating HDFS directories..."
  docker compose -f "$COMPOSE_FILE" exec namenode1 hdfs dfs -mkdir -p /user/data/input

  echo "Uploading sample data to HDFS..."
  docker compose -f "$COMPOSE_FILE" cp data/sample.csv namenode1:/tmp/sample.csv
  docker compose -f "$COMPOSE_FILE" exec namenode1 \
    hdfs dfs -put -f /tmp/sample.csv /user/data/input/sample.csv

  echo ""
  echo "HDFS HA cluster is ready!"
  echo "  NameNode 1 Web UI : http://localhost:9870  (active)"
  echo "  NameNode 2 Web UI : http://localhost:9871  (standby)"
  echo "  NameNode 1 RPC    : localhost:8020"
  echo "  NameNode 2 RPC    : localhost:8021"
  echo "  Nameservice       : mycluster"
  echo "  Sample data       : hdfs://mycluster/user/data/input/sample.csv"
  echo ""
  echo "Set the following environment variables:"
  echo ""
  echo "  export HDFS_NN1=localhost:8020"
  echo "  export HDFS_NN2=localhost:8021"
  echo "  export INPUT_PATH=hdfs://mycluster/user/data/input/sample.csv"
  echo "  export OUTPUT_PATH=hdfs://mycluster/user/data/output"

else
  COMPOSE_FILE="docker-compose.yml"
  echo "Starting HDFS single-node cluster..."
  docker compose -f "$COMPOSE_FILE" up -d

  echo "Waiting for NameNode to become healthy..."
  MAX_RETRIES=30
  RETRY_INTERVAL=5
  for i in $(seq 1 "$MAX_RETRIES"); do
    if curl -sf http://localhost:9870/ >/dev/null 2>&1; then
      echo "NameNode is healthy."
      break
    fi
    if [ "$i" -eq "$MAX_RETRIES" ]; then
      echo "ERROR: NameNode did not become healthy in time." >&2
      exit 1
    fi
    echo "  Attempt $i/$MAX_RETRIES — retrying in ${RETRY_INTERVAL}s..."
    sleep "$RETRY_INTERVAL"
  done

  echo "Creating HDFS directories..."
  docker compose -f "$COMPOSE_FILE" exec namenode hdfs dfs -mkdir -p /user/data/input

  echo "Uploading sample data to HDFS..."
  docker compose -f "$COMPOSE_FILE" cp data/sample.csv namenode:/tmp/sample.csv
  docker compose -f "$COMPOSE_FILE" exec namenode \
    hdfs dfs -put -f /tmp/sample.csv /user/data/input/sample.csv

  echo ""
  echo "HDFS cluster is ready!"
  echo "  NameNode Web UI : http://localhost:9870"
  echo "  HDFS RPC        : hdfs://localhost:8020"
  echo "  Sample data     : hdfs:///user/data/input/sample.csv"
  echo ""
  echo "Set the following environment variables:"
  echo ""
  echo "  export HDFS_NAMENODE=localhost:8020"
  echo "  export INPUT_PATH=hdfs:///user/data/input/sample.csv"
  echo "  export OUTPUT_PATH=hdfs:///user/data/output"
fi
