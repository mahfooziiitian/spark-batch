"""Tests for conditional XPath extraction patterns."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


@pytest.fixture(scope="session")
def spark():
    """Create a shared SparkSession for all tests."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("XPath Conditional Tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_xpath_boolean_where_filter(spark):
    """xpath_boolean in WHERE clause filters rows correctly."""
    data = [
        "<emp><name>Alice</name><salary>120000</salary></emp>",
        "<emp><name>Bob</name><salary>85000</salary></emp>",
        "<emp><name>Carol</name><salary>95000</salary></emp>",
    ]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_cond_where")

    result = spark.sql("""
        SELECT xpath_string(data, 'emp/name') AS name
        FROM test_cond_where
        WHERE xpath_boolean(data, 'emp[salary >= 100000]')
    """)
    names = [row.name for row in result.collect()]
    assert names == ["Alice"]


def test_case_on_xpath_values(spark):
    """CASE expression categorizes xpath-extracted values."""
    data = [
        "<emp><name>Alice</name><salary>150000</salary></emp>",
        "<emp><name>Bob</name><salary>85000</salary></emp>",
        "<emp><name>Dave</name><salary>55000</salary></emp>",
    ]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_cond_case")

    result = spark.sql("""
        SELECT
            xpath_string(data, 'emp/name') AS name,
            CASE
                WHEN xpath_int(data, 'emp/salary') >= 100000 THEN 'Senior'
                WHEN xpath_int(data, 'emp/salary') >= 80000  THEN 'Mid'
                ELSE 'Entry'
            END AS band
        FROM test_cond_case
    """)
    rows = {row.name: row.band for row in result.collect()}
    assert rows == {"Alice": "Senior", "Bob": "Mid", "Dave": "Entry"}


def test_coalesce_fallback_for_missing_element(spark):
    """COALESCE provides fallback when xpath_string returns empty."""
    data = [
        "<emp><name>Alice</name><title>VP</title></emp>",
        "<emp><name>Bob</name><level>Mid</level></emp>",
    ]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_cond_coalesce")

    result = spark.sql("""
        SELECT
            xpath_string(data, 'emp/name') AS name,
            COALESCE(
                NULLIF(xpath_string(data, 'emp/title'), ''),
                NULLIF(xpath_string(data, 'emp/level'), ''),
                'Unknown'
            ) AS display_title
        FROM test_cond_coalesce
    """)
    rows = {row.name: row.display_title for row in result.collect()}
    assert rows == {"Alice": "VP", "Bob": "Mid"}


def test_combined_boolean_and_case(spark):
    """Combine xpath_boolean in CASE for conditional computation."""
    data = [
        "<emp><name>Alice</name><salary>100000</salary><remote>true</remote></emp>",
        "<emp><name>Bob</name><salary>100000</salary><remote>false</remote></emp>",
    ]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_cond_combined")

    result = spark.sql("""
        SELECT
            xpath_string(data, 'emp/name') AS name,
            CASE
                WHEN xpath_boolean(data, 'emp[remote="true"]')
                    THEN CAST(xpath_int(data, 'emp/salary') * 1.1 AS DOUBLE)
                ELSE CAST(xpath_int(data, 'emp/salary') * 1.0 AS DOUBLE)
            END AS adjusted
        FROM test_cond_combined
    """)
    rows = {row.name: row.adjusted for row in result.collect()}
    assert abs(rows["Alice"] - 110000.0) < 0.01
    assert abs(rows["Bob"] - 100000.0) < 0.01
