"""Shared pytest configuration and fixtures for all test modules."""

import os

import pytest
from pyspark.sql import SparkSession

from pys_html.config import configure_env


def pytest_configure(config):
    """Configure JAVA_HOME/PYSPARK_PYTHON portably before any test collection."""
    _ = config  # required by pytest hook signature
    configure_env()


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession fixture to avoid repeated JVM startup overhead."""
    session = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName("pys-html-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
