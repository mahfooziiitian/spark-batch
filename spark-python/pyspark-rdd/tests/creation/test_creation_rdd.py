import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("functions") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_simple_rdd(spark):
    rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7])
    assert rdd.stdev() == 2.0


def test_simple_rdd_partition(spark):
    rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7], 2)
    assert rdd.getNumPartitions() == 2
