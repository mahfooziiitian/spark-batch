import pytest
from pyspark import SparkConf, SparkContext


@pytest.mark.skip(reason="Requires a running YARN cluster")
def test_yarn_cluster():
    conf = SparkConf().setAppName("pytest_spark_test").setMaster("yarn")
    sc = SparkContext.getOrCreate(conf)
    try:
        rdd = sc.parallelize([1, 2, 3, 4, 5])
        result = rdd.sum()
        assert result == 15
    finally:
        sc.stop()


@pytest.mark.skip(reason="Requires a running Kubernetes cluster")
def test_kubernetes_cluster():
    conf = SparkConf().setAppName("pytest_spark_test").setMaster("k8s://...")
    sc = SparkContext.getOrCreate(conf)
    try:
        rdd = sc.parallelize([1, 2, 3, 4, 5])
        result = rdd.sum()
        assert result == 15
    finally:
        sc.stop()
