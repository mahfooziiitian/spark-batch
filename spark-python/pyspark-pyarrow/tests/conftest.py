from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder.master("local[*]").appName("pyspark-pyarrow").getOrCreate()
    yield spark
    spark.stop()

