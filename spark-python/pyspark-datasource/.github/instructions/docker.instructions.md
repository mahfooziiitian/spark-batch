---
applyTo: "**/Dockerfile,**/docker-compose*.yml"
---

# Docker and Container Patterns

## Base Image

Use the official Apache Spark Python image:

```dockerfile
FROM apache/spark:3.5.0-python3
```

## Environment Variables

```dockerfile
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_LOCAL_IP=127.0.0.1
```

## PYTHONPATH for py4j

Ensure py4j is on the Python path so PySpark can communicate with the JVM:

```dockerfile
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH:-}"
```

## Python Symlink

The Spark base image may only have `python3`. Create a `python` symlink
if scripts or tools expect it:

```dockerfile
USER root
RUN ln -sf /usr/bin/python3 /usr/bin/python
```

## USER Pattern

Use `root` for installation steps, then switch to `spark` for runtime:

```dockerfile
# --- Install phase (root) ---
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/ /app/src/

# --- Runtime phase (spark) ---
USER spark
WORKDIR /app

ENTRYPOINT ["spark-submit"]
CMD ["src/main.py"]
```

### Why switch users?

- **root** — needed for `apt-get`, `pip install`, filesystem setup.
- **spark** — the Spark base image creates this non-root user; running as
  `spark` follows the principle of least privilege and matches the image's
  default user.

## .dockerignore

Every project with a Dockerfile should include a `.dockerignore`:

```
.git
.github
.venv
__pycache__
*.pyc
.pytest_cache
.mypy_cache
.ruff_cache
docs/
*.egg-info
dist/
build/
uv.lock
```

## docker-compose Patterns

### Spark standalone cluster (development)

```yaml
services:
  spark-master:
    image: apache/spark:3.5.0-python3
    command: /opt/spark/sbin/start-master.sh
    ports:
      - "8080:8080"
      - "7077:7077"
    environment:
      SPARK_MASTER_HOST: spark-master

  spark-worker:
    image: apache/spark:3.5.0-python3
    command: /opt/spark/sbin/start-worker.sh spark://spark-master:7077
    depends_on:
      - spark-master
    environment:
      SPARK_WORKER_MEMORY: 2g
      SPARK_WORKER_CORES: 2
```

### Application with external services

```yaml
services:
  app:
    build: .
    environment:
      SPARK_MASTER: "local[*]"
      INPUT_PATH: /data/input
      OUTPUT_PATH: /data/output
    volumes:
      - ./data:/data

  database:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: ${DB_PASSWORD:-dev}
      MYSQL_DATABASE: sparkdb
    ports:
      - "3306:3306"
```

## Best Practices

- Pin image tags — never use `latest` in production Dockerfiles.
- Combine `RUN` commands to reduce layers.
- Use `--no-install-recommends` with `apt-get` and `--no-cache-dir` with `pip`.
- Mount data volumes instead of copying large datasets into the image.
- Use `.env` files for compose variables — never hardcode credentials.
- Set `restart: "no"` for development/one-shot containers.
