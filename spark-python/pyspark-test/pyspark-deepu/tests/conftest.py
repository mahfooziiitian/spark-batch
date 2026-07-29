"""Shared SparkSession fixture with PyDeequ JAR for all tests."""

import os

os.environ["SPARK_VERSION"] = "3.5"

import pydeequ
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a session-scoped SparkSession with Deequ JAR configured."""
    session = (
        SparkSession.builder.appName("pyspark-deepu-tests")
        .master("local[2]")
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def pytest_sessionfinish(session, exitstatus):
    """Force exit after tests complete to avoid hanging on Spark/JVM daemon threads."""
    os._exit(exitstatus)
