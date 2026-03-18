FROM apache/spark:3.5.0-python3

USER root

# Install Python dependencies
COPY pyproject.toml /tmp/pyproject.toml
RUN pip install --no-cache-dir \
      "pyspark<4.0.0" \
      "pyarrow>=4.0.0" \
      "pandas>=1.3.0" \
    && ln -s "$(which python3)" /usr/local/bin/python

# Download catalog JARs
ARG ICEBERG_VERSION=1.5.0
ARG DELTA_VERSION=3.1.0
ARG SCALA_VERSION=2.12
ARG AWS_SDK_VERSION=2.23.19

RUN curl -sL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
      -o "${SPARK_HOME}/jars/iceberg-spark-runtime.jar" && \
    curl -sL "https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" \
      -o "${SPARK_HOME}/jars/delta-spark.jar" && \
    curl -sL "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
      -o "${SPARK_HOME}/jars/delta-storage.jar" && \
    curl -sL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      -o "${SPARK_HOME}/jars/iceberg-aws-bundle.jar" && \
    curl -sL "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar" \
      -o "${SPARK_HOME}/jars/postgresql.jar" && \
    curl -sL "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar" \
      -o "${SPARK_HOME}/jars/mysql-connector.jar"

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2

WORKDIR /workspace

USER spark
