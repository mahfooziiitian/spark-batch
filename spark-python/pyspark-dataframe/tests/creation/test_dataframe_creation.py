import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


@pytest.fixture(scope='session')
def spark():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("test_column_transformation")
             .getOrCreate())
    yield spark
    spark.stop()

