import pytest
from pyspark.sql import SparkSession

from transformation.transformation_filter import temp_more_than_thirteen
from transformation.transformation_map import fahrenheit_to_centigrade


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("functions").getOrCreate()
    yield spark
    spark.stop()


def test_filter_using_function(spark):
    tempData = [59, 57.2, 53.6, 55.4, 51.8, 53.6, 55.4]
    parTempData = spark.sparkContext.parallelize(tempData, 2)
    parCentigradeData = parTempData.map(fahrenheit_to_centigrade)
    expected_data = [15, 14.000000000000002, 13.0, 13.0]
    filteredTemperature = parCentigradeData.filter(temp_more_than_thirteen)
    assert filteredTemperature.collect() == expected_data


def test_filter_using_lambda(spark):
    tempData = [59, 57.2, 53.6, 55.4, 51.8, 53.6, 55.4]
    parTempData = spark.sparkContext.parallelize(tempData, 2)
    parCentigradeData = parTempData.map(fahrenheit_to_centigrade)
    expected_data = [15, 14.000000000000002, 13.0, 13.0]
    filteredTemperature = parCentigradeData.filter(lambda x: x >= 13)
    assert filteredTemperature.collect() == expected_data
