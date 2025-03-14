import pytest
from pyspark.sql import SparkSession

from transformation.transformation_map import fahrenheitToCentigrade


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("functions") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_map_transformation(spark):
    tempData = [59, 57.2, 53.6, 55.4, 51.8, 53.6, 55.4]
    parTempData = spark.sparkContext.parallelize(tempData, 2)
    parCentigradeData = parTempData.map(fahrenheitToCentigrade)
    expected_data = [15, 14.000000000000002, 12.0, 13.0, 10.999999999999998, 12.0, 13.0]
    assert parCentigradeData.collect() == expected_data


