---
applyTo: "{**/Dockerfile,**/docker-compose*.yml}"
---

# Docker & Docker Compose Instructions

## Dockerfile — Base Image

Use the official Apache Spark image for PySpark containers:

```dockerfile
FROM apache/spark:3.5.0-python3
```

## Required ENV Settings

```dockerfile
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1
```

`SPARK_LOCAL_IP=127.0.0.1` prevents hostname-resolution failures in container networks.

## PYTHONPATH for `python3 script.py` invocation

```dockerfile
# py4j version must match the one bundled with this Spark release.
# Spark 3.5.x → py4j 0.10.9.7
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
```

## Standard Dockerfile Template

```dockerfile
FROM apache/spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir \
      "pyarrow>=4.0.0" \
      "pandas>=1.3.0"  \
    && ln -s "$(which python3)" /usr/local/bin/python

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

WORKDIR /workspace

USER spark
```

## docker-compose.yml — Database Services

### MySQL (used for JDBC parallel ingestion examples)

```yaml
services:
  mysql:
    image: mysql:8.0.32
    container_name: mysql
    restart: unless-stopped
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD}
      MYSQL_DATABASE: ${MYSQL_DATABASE:-tutorials}
      MYSQL_USER: ${MYSQL_USER:-mysql}
      MYSQL_PASSWORD: ${MYSQL_PASSWORD}
    volumes:
      - mysql_data:/var/lib/mysql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s

volumes:
  mysql_data:
```

### Rules

- **Never** hard-code passwords in `docker-compose.yml` — use env var substitution (`${VAR}`) backed by a `.env` file.
- **Always** add a `volumes:` entry for stateful services so data survives container restarts.
- **Always** add a `healthcheck:` so dependent containers wait for the service to be ready.
- Use `restart: unless-stopped` for development services; `restart: no` for one-shot containers.
- Prefer `image:` over `build:` when using standard upstream images.

### .env file template (never commit to version control)

```dotenv
MYSQL_ROOT_PASSWORD=change_me
MYSQL_DATABASE=tutorials
MYSQL_USER=mysql
MYSQL_PASSWORD=change_me
```

Add `.env` to `.gitignore`.

## .dockerignore

Always include a `.dockerignore` alongside each Dockerfile:

```
.venv/
__pycache__/
*.pyc
*.pyo
.git/
*.egg-info/
dist/
build/
.pytest_cache/
spark-warehouse/
```
