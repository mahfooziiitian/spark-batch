# test_xml_processing.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


# Fixture to create SparkSession
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("XML Processing Test") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    yield spark
    spark.stop()


# Test function to check XML processing logic
def test_xpath_processing(spark):
    data = [
        "<Msg><Header><tag1>some str1</tag1><tag2>2</tag2><tag3>2022-02-16 "
        "10:39:26.730</tag3></Header><Body><Pair><N>N1</N><V>V1</V></Pair><Pair><N>N2</N><V>V2</V></Pair><Pair><N>N3"
        "</N><V>V3</V></Pair></Body></Msg>",
        "<Msg><Header><tag1>some str2</tag1><tag2>5</tag2><tag3>2022-02-17 "
        "10:39:26.730</tag3></Header><Body><Pair><N>N4</N><V>V4</V></Pair><Pair><N>N5</N><V>V5</V></Pair></Body></Msg>"
    ]

    xml_df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    filtered_df = xml_df.select("/Msg/Header")

    # Assertion
    assert filtered_df.count() > 0, "No data found with the given XPath expression"
