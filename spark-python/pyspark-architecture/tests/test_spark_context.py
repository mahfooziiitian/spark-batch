import pytest
from pyspark import SparkConf, SparkContext


@pytest.fixture(scope="session")
def spark_context():
    conf = SparkConf().setAppName("pytest_spark_test").setMaster("local[2]")
    sc = SparkContext.getOrCreate(conf)
    yield sc


def test_my_spark_function(spark_context):
    rdd = spark_context.parallelize([1, 2, 3, 4, 5])
    result = rdd.sum()
    print(rdd.getNumPartitions())
    assert result == 15


def test_multiple_spark_context():
    """Multiple SparkContexts per JVM is not allowed.
    Apache Spark provides getOrCreate() to safely reuse the existing SparkContext.
    """
    spark_context1 = SparkContext.getOrCreate(SparkConf().setAppName("SparkContext#1").setMaster("local[*]"))
    spark_context2 = SparkContext.getOrCreate(SparkConf().setAppName("SparkContext#2").setMaster("local[*]"))

    spark_context1.parallelize([1, 2, 3])
    spark_context2.parallelize([4, 5, 6])

    assert spark_context1 == spark_context2
