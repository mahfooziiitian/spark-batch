import os
import sys

from pyspark.sql import SparkSession
import pytest

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"


@pytest.fixture
def spark() -> SparkSession:
    # Create a SparkSession (the entry point to Spark functionality) on
    # the cluster in the remote Databricks workspace. Unit tests do not
    # have access to this SparkSession by default.
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(
        data=[
            ('a', 1),
            ('b', 2),
            ('c', 3),
        ],
        schema=['letter', 'number'],
    )

    spark.sql('CREATE DATABASE IF NOT EXISTS default')
    spark.sql('USE default')
    df.createOrReplaceTempView("diamonds")

    yield spark


def test_spark(spark):
    spark.sql('USE default')
    data = spark.sql('SELECT * FROM diamonds')
    assert data.collect()[0][1] == 1
