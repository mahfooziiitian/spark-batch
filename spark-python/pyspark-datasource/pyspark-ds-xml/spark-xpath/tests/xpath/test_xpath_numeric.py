"""Tests for numeric XPath extraction patterns."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


@pytest.fixture(scope="session")
def spark():
    """Create a shared SparkSession for all tests."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("XPath Numeric Tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_xpath_int_extracts_integer(spark):
    """xpath_int returns an integer value from XML."""
    data = ["<order><quantity>42</quantity></order>"]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_num_int")

    result = spark.sql(
        "SELECT xpath_int(data, 'order/quantity') AS qty FROM test_num_int"
    )
    assert result.collect()[0].qty == 42


def test_xpath_double_extracts_decimal(spark):
    """xpath_double returns a double value from XML."""
    data = ["<order><price>29.99</price></order>"]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_num_double")

    result = spark.sql(
        "SELECT xpath_double(data, 'order/price') AS price FROM test_num_double"
    )
    assert abs(result.collect()[0].price - 29.99) < 0.001


def test_xpath_numeric_arithmetic(spark):
    """Arithmetic operations on xpath-extracted numeric values."""
    data = [
        "<order><quantity>5</quantity><unit_price>10.00</unit_price></order>"
    ]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_num_arith")

    result = spark.sql("""
        SELECT
            xpath_int(data, 'order/quantity')
            * xpath_double(data, 'order/unit_price') AS total
        FROM test_num_arith
    """)
    assert abs(result.collect()[0].total - 50.0) < 0.001


def test_xpath_int_missing_element_returns_zero(spark):
    """xpath_int returns 0 when the element does not exist."""
    data = ["<order><name>Widget</name></order>"]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_num_missing")

    result = spark.sql(
        "SELECT xpath_int(data, 'order/quantity') AS qty FROM test_num_missing"
    )
    assert result.collect()[0].qty == 0


def test_xpath_numeric_aggregation(spark):
    """SUM/AVG on xpath_int across multiple rows."""
    data = [
        "<item><qty>10</qty></item>",
        "<item><qty>20</qty></item>",
        "<item><qty>30</qty></item>",
    ]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_num_agg")

    result = spark.sql("""
        SELECT
            SUM(xpath_int(data, 'item/qty'))   AS total,
            AVG(xpath_int(data, 'item/qty'))   AS average
        FROM test_num_agg
    """)
    row = result.collect()[0]
    assert row.total == 60
    assert abs(row.average - 20.0) < 0.001
