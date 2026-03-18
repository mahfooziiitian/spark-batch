---
applyTo: "**/Dockerfile"
---

# Dockerfile Instructions

## Base Image

Use the official Apache Spark image for PySpark containers:

```dockerfile
FROM apache/spark:3.5.0-python3
```

Update the tag when upgrading Spark. Available tags follow the pattern
`<spark-version>-python3`.

## Required ENV Settings

Always set these three ENV vars to make PySpark work correctly inside the container:

```dockerfile
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1
```

`SPARK_LOCAL_IP=127.0.0.1` prevents hostname-resolution failures inside container networks.

## PYTHONPATH for Direct `python3` Invocation

The `apache/spark` entrypoint only sets `PYTHONPATH` for `spark-submit` / `pyspark` calls.
To make `import pyspark` work when running `python3 script.py` directly, add:

```dockerfile
# py4j version must match the one bundled with the Spark release.
# Spark 3.5.x → py4j 0.10.9.7
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
```

Update the py4j version whenever the base image changes.

## `python` → `python3` Symlink

The base image ships `python3` only. Create a symlink so both names work:

```dockerfile
RUN pip install --no-cache-dir <packages> \
    && ln -s "$(which python3)" /usr/local/bin/python
```

## USER Pattern

```dockerfile
USER root   # install packages as root

RUN pip install --no-cache-dir \
      "pyarrow>=4.0.0" \
      "pandas>=1.3.0"  \
    && ln -s "$(which python3)" /usr/local/bin/python

ENV ...

WORKDIR /workspace

USER spark  # drop back to the non-root spark user for runtime
```

Never leave the container running as `root`.

## Standard Template

```dockerfile
FROM apache/spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir \
      "pyarrow>=4.0.0" \
      "pandas>=1.3.0"  \
      "numpy>=1.21.0"  \
    && ln -s "$(which python3)" /usr/local/bin/python

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1

# Expose PySpark to plain `python3 script.py` calls
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

WORKDIR /workspace

USER spark
```

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
```
