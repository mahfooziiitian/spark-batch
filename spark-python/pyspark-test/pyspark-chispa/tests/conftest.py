"""Shared SparkSession and common fixtures for all chispa tests."""

import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all test files."""
    session = (
        SparkSession.builder.appName("chispa-test-suite")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame for general-purpose testing."""
    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    schema = StructType(
        [
            StructField("name", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=False),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture
def empty_df(spark):
    """Create an empty DataFrame with a standard schema."""
    schema = StructType(
        [
            StructField("id", StringType(), nullable=False),
            StructField("value", IntegerType(), nullable=True),
        ]
    )
    return spark.createDataFrame([], schema)


def pytest_sessionfinish(session, exitstatus):
    """Force exit after tests complete to avoid hanging on Spark/JVM daemon threads."""
    os._exit(exitstatus)
