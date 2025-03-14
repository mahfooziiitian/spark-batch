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


def test_collect_action(spark):
    pythonList = [2.3, 3.4, 4.3, 2.4, 2.3, 4.0]
    parPythonData = spark.sparkContext.parallelize(pythonList, 2)
    assert parPythonData.collect() == [2.3, 3.4, 4.3, 2.4, 2.3, 4.0]
