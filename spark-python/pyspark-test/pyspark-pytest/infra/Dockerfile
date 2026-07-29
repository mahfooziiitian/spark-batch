FROM apache/spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        pytest>=8.0 \
        pytest-sugar>=1.1 \
        pyspark==3.5.4 \
        faker>=25.8 \
        pandas>=2.0,<2.1 \
        pyarrow>=16.1 \
        numpy<2.0

WORKDIR /app
COPY . /app

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONPATH=/app/src

USER spark

CMD ["python3", "-m", "pytest", "tests/", "-v", "--tb=short"]