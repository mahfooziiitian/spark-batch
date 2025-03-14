import pytest
from pyspark import SparkConf, SparkContext


@pytest.fixture(scope="session")
def spark_context_yarn():
    conf = SparkConf().setAppName("pytest_spark_test").setMaster("yarn")
    sc = SparkContext(conf=conf)
    yield sc
    sc.stop()


@pytest.fixture(scope="session")
def spark_context_kubernetes():
    conf = SparkConf().setAppName("pytest_spark_test").setMaster("k8s://...")
    sc = SparkContext(conf=conf)
    yield sc
    sc.stop()


def test_yarn_cluster(spark_context_yarn):
    rdd = spark_context_yarn.parallelize([1, 2, 3, 4, 5])
    result = rdd.sum()
    assert result == 15


def test_kubernetes_cluster(spark_context_kubernetes):
    rdd = spark_context_kubernetes.parallelize([1, 2, 3, 4, 5])
    result = rdd.sum()
    assert result == 15
