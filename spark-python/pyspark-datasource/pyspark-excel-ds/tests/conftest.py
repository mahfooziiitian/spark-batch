"""Shared pytest configuration for all test modules."""

import importlib.util
import os

import pytest
from pyspark.sql import SparkSession

DELTA_AVAILABLE = importlib.util.find_spec("delta") is not None


def pytest_configure(config):
    """Set JAVA_HOME to Java 17 before any test collection."""
    _ = config  # required by pytest hook signature
    java_home_17 = os.environ.get("JAVA_HOME_17")
    if java_home_17:
        os.environ["JAVA_HOME"] = java_home_17


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession with Hive support for table-integration tests.

    Delta Lake support (extensions + catalog) is configured upfront when the
    optional ``delta-spark`` dependency is installed. This must happen before
    the first ``getOrCreate()`` call: once the JVM/SparkContext is started,
    later attempts to bolt on ``spark.jars.packages``/catalog config for a
    *different* SparkSession in the same process are silently ignored, which
    previously caused ``ClassNotFoundException: DeltaCatalog`` for tests that
    tried to build a second, Delta-enabled session mid-suite.
    """
    builder = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName("pys-excel-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
    )

    if DELTA_AVAILABLE:
        from delta import configure_spark_with_delta_pip

        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        builder = configure_spark_with_delta_pip(builder)

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
