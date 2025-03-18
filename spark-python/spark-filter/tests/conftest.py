import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("sparksession").getOrCreate()
    yield spark
