import pytest
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


@pytest.fixture()
def spark():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    yield spark
    spark.stop()
