# PySpark development image
#
# Build:
#   docker build -t pyspark-dev:3.5 .
#
# Run interactively:
#   docker run --rm -it -v "$(pwd)":/workspace -p 4040:4040 pyspark-dev:3.5 bash
#
# Run a script:
#   docker run --rm -v "$(pwd)":/workspace pyspark-dev:3.5 python3 /workspace/my_job.py

FROM apache/spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir \
      "pyarrow>=4.0.0" \
      "pandas>=1.3.0" \
      "numpy>=1.21.0" \
    && ln -s "$(which python3)" /usr/local/bin/python

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1

# PYTHONPATH exposes PySpark to plain `python3` calls (not only spark-submit).
# py4j version is bundled with Spark 3.5.x — update if changing the base image.
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

WORKDIR /workspace

USER spark
