"""Shared fixtures for spark-xml tests."""

import os
import sys

import pytest
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ.get(
    "JAVA_HOME_17", os.environ.get("JAVA_HOME", "")
)
os.environ["PYSPARK_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("spark-xml-tests")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
