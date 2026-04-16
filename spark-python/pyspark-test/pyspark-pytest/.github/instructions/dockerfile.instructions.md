---
applyTo: "**/Dockerfile"
---

# Dockerfile Instructions — pyspark-pytest

## Base Image

The project uses Ubuntu 18.04 with manually installed Spark 3.0.1.
For new Dockerfiles, prefer the official Apache Spark image:

```dockerfile
FROM apache/spark:3.5.0-python3
```

## Required ENV Settings

```dockerfile
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1
```

## PYTHONPATH

Set PYTHONPATH so `import` works for both `spark-submit` and `python3` invocations:

```dockerfile
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
```

Update the py4j version to match the bundled Spark release.

## User Pattern

```dockerfile
USER root
RUN pip install --no-cache-dir <packages>
WORKDIR /workspace
USER spark
```

Never leave the container running as root.

## docker-compose

Mount source as `PYTHONPATH=./src` and run pytest:

```yaml
services:
  spark-test:
    build: .
    environment:
      - PYTHONPATH=./src
    command: pytest tests/
```

## .dockerignore

```
.venv/
__pycache__/
*.pyc
.git/
*.egg-info/
dist/
build/
.pytest_cache/
```
