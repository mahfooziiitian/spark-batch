import pytest
from pyspark import SparkConf, SparkContext


@pytest.fixture(scope="session")
def spark_context():
    conf = SparkConf().setAppName("pytest_spark_test")
    sc = SparkContext(conf=conf)
    yield sc
    sc.stop()


def test_my_spark_function(spark_context):
    rdd = spark_context.parallelize([1, 2, 3, 4, 5])
    result = rdd.sum()
    print(rdd.getNumPartitions())
    assert result == 15


"""
Multiple SparkContexts per JVM is technically possible but at the same time it's considered as a bad practice.
Apache Spark provides a factory method getOrCreate() to prevent against creating multiple SparkContext.
"""


def test_multiple_spark_context():
    spark_context1 = SparkContext.getOrCreate(SparkConf().setAppName("SparkContext#1").setMaster("local[*]"))
    spark_context2 = SparkContext.getOrCreate(SparkConf().setAppName("SparkContext#2").setMaster("local[*]"))

    spark_context1.parallelize([1, 2, 3])
    spark_context2.parallelize([4, 5, 6])

    assert spark_context1 == spark_context2
