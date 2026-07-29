---
applyTo: "**/Dockerfile"
---

# Dockerfile Instructions (Root-Level Defaults)

These are baseline Dockerfile conventions for all child projects. Each child project
may override these via its own `.github/instructions/dockerfile.instructions.md`.

## Base Image

Prefer the official Apache Spark image for new Dockerfiles:

```dockerfile
FROM apache/spark:3.5.0-python3
```

## Required Environment Variables

```dockerfile
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1
```

## PYTHONPATH

Set `PYTHONPATH` so imports work for both `spark-submit` and `python3`:

```dockerfile
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
```

Update the py4j version to match the bundled Spark release.

## User Security

Never leave the container running as root:

```dockerfile
USER root
RUN pip install --no-cache-dir <packages>
WORKDIR /workspace
COPY . .
USER spark
```

## .dockerignore

Always include a `.dockerignore`:

```
.venv/
__pycache__/
*.pyc
.git/
*.egg-info/
dist/
build/
.pytest_cache/
.mypy_cache/
.ruff_cache/
```

## Multi-Stage Builds

For production images, use multi-stage builds to keep the final image small:

```dockerfile
FROM python:3.11-slim AS builder
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM apache/spark:3.5.0-python3
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY src/ /workspace/src/
```
