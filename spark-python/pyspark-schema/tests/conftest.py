import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON",        "python3")
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python3")
    os.environ.setdefault("SPARK_LOCAL_IP",        "127.0.0.1")

    session = (SparkSession.builder
               .appName("test-pyspark-schema")
               .master("local[2]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
