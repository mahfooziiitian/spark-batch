#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Docker: Run examples in a containerized Spark environment
#
# Usage:
#   ./scripts/docker-run.sh examples/06_schema/01_struct_type_schema.py
#   ./scripts/docker-run.sh  # Interactive shell
#
# Builds a local image with PySpark 4 + pys_json library installed.
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_NAME="pyspark-ds-json:local"
PY_FILE="${1:-}"

# Build image if it doesn't exist or Dockerfile changed
if [[ ! "$(docker images -q "${IMAGE_NAME}" 2>/dev/null)" ]]; then
    echo "Building Docker image: ${IMAGE_NAME}"
    docker build -t "${IMAGE_NAME}" -f - "${PROJECT_ROOT}" <<'DOCKERFILE'
FROM eclipse-temurin:17-jre AS base

# Install Python and pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 python3.11-venv python3-pip curl && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3.11 /usr/bin/python

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src/ src/
COPY examples/ examples/
COPY data/ data/

# Install uv and project
RUN pip install uv && uv sync --no-dev

ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_MASTER=local[*]
ENV DATA_HOME=/app/data
ENV PYS_JSON_LOG_LEVEL=INFO

ENTRYPOINT ["uv", "run", "python"]
DOCKERFILE
fi

echo "╭─────────────────────────────────────────────────╮"
echo "│  Docker: PySpark JSON Examples                  │"
echo "├─────────────────────────────────────────────────┤"
echo "│  Image: ${IMAGE_NAME}"
echo "╰─────────────────────────────────────────────────╯"

if [[ -n "${PY_FILE}" ]]; then
    exec docker run --rm \
        -v "${PROJECT_ROOT}/data:/app/data" \
        "${IMAGE_NAME}" "${PY_FILE}"
else
    exec docker run --rm -it \
        -v "${PROJECT_ROOT}/data:/app/data" \
        --entrypoint /bin/bash \
        "${IMAGE_NAME}"
fi
