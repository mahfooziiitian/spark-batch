# ┌──────────────────────────────────────────────────────────────────────────────┐
# │ Multi-stage Dockerfile — portable PySpark development image                │
# │                                                                            │
# │ Stages:                                                                    │
# │   base    — shared runtime: Spark + pip deps + env vars                    │
# │   app     — copies project code, runs scripts / tests via entrypoint       │
# │   jupyter — adds JupyterLab, designed for volume-mounted notebooks         │
# │                                                                            │
# │ Build:                                                                     │
# │   docker build --target app     -t pyspark-app     ../..                   │
# │   docker build --target jupyter  -t pyspark-jupyter ../..                  │
# └──────────────────────────────────────────────────────────────────────────────┘

# ── Build arguments (overridable from .env / CLI) ─────────────────────────────
ARG SPARK_VERSION=3.5.0
ARG PY4J_VERSION=0.10.9.7


# ══════════════════════════════════════════════════════════════════════════════
# Stage 1 — base
# ══════════════════════════════════════════════════════════════════════════════
FROM apache/spark:${SPARK_VERSION}-python3 AS base

USER root

RUN pip install --no-cache-dir \
      "pyarrow>=4.0.0"  \
      "pandas>=1.3.0"   \
      "numpy>=1.21.0"   \
      "pyyaml>=6.0"     \
      "chispa>=0.10.0"  \
    && ln -s "$(which python3)" /usr/local/bin/python

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1

ARG PY4J_VERSION
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-${PY4J_VERSION}-src.zip:/workspace/src"

# Ensure the spark user has a writable home (needed by Jupyter, pip, etc.)
RUN mkdir -p /home/spark/.local /home/spark/.jupyter /home/spark/.cache \
    && chown -R spark:spark /home/spark

WORKDIR /workspace


# ══════════════════════════════════════════════════════════════════════════════
# Stage 2 — app  (default target)
# ══════════════════════════════════════════════════════════════════════════════
FROM base AS app

COPY --chown=spark:spark src/    src/
COPY --chown=spark:spark configs/ configs/
COPY --chown=spark:spark data/   data/
COPY --chown=spark:spark tests/  tests/
COPY --chown=spark:spark pyproject.toml .

COPY --chown=spark:spark infra/docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

USER spark

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["run", "src/data_frame/etl/etl.py"]


# ══════════════════════════════════════════════════════════════════════════════
# Stage 3 — jupyter
# ══════════════════════════════════════════════════════════════════════════════
FROM base AS jupyter

RUN pip install --no-cache-dir \
      "jupyterlab>=4.0"  \
      "ipykernel>=6.0"   \
      "ipywidgets>=8.0"  \
      "matplotlib>=3.5"

EXPOSE 8888

USER spark

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--ServerApp.token=''", \
     "--ServerApp.password=''", \
     "--notebook-dir=/workspace/notebooks"]