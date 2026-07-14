---
applyTo: "{**/docker-compose.yml,**/docker-compose.yaml,**/Dockerfile,**/.dockerignore}"
---

# PySpark MongoDB — Infrastructure Instructions

## Docker Compose — MongoDB Stack

The `infra/docker/docker-compose.yml` provides:

| Service         | Image             | Port  | Purpose                    |
| --------------- | ----------------- | ----- | -------------------------- |
| `mongo`         | `mongo:5.0.17`    | 27017 | MongoDB server             |
| `mongo-express` | `mongo-express`   | 8081  | Web-based MongoDB admin UI |

### Conventions

- Always define named volumes for data persistence (`mongo-data`).
- Pin image tags — never use `latest` for MongoDB itself.
- Use `depends_on` to express service startup ordering.
- Include `restart: always` for dev convenience.
- Default credentials: `mongo` / `mongo` (dev only — never use in production).

### Environment variables for MongoDB init

```yaml
environment:
  - MONGO_INITDB_DATABASE=tutorial
  - MONGO_INITDB_ROOT_USERNAME=mongo
  - MONGO_INITDB_ROOT_PASSWORD=mongo
```

### Starting & stopping

```bash
cd infra/docker
docker compose up -d          # start
docker compose down           # stop containers
docker compose down -v        # stop + remove volumes (data loss)
```

## Dockerfile Conventions

### Base image

Use the official Apache Spark image for PySpark containers:

```dockerfile
FROM apache/spark:3.5.0-python3
```

### Required ENV settings

```dockerfile
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1
```

### PYTHONPATH for direct `python3` invocation

```dockerfile
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
```

### USER pattern

```dockerfile
USER root
RUN pip install --no-cache-dir <packages> \
    && ln -s "$(which python3)" /usr/local/bin/python
# ... ENV, WORKDIR ...
USER spark   # drop back to non-root for runtime
```

Never leave the container running as `root`.

## .dockerignore

Always pair a Dockerfile with a `.dockerignore`:

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
.mypy_cache/
```

## Port Reference

| Port  | Service       | Notes                                    |
| ----- | ------------- | ---------------------------------------- |
| 27017 | MongoDB       | Primary MongoDB wire protocol port       |
| 8081  | Mongo Express | Web admin UI (depends on `mongo` service)|
| 4040  | Spark UI      | Only when `spark.ui.enabled=true`        |
