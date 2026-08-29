from __future__ import annotations

import os

import pytest

try:
    from pyspark.sql import SparkSession

    _HAS_PYSPARK = True
except ImportError:
    _HAS_PYSPARK = False


@pytest.fixture(scope="session")
def spark():
    if not _HAS_PYSPARK:
        pytest.skip("pyspark not installed — install with: uv sync --extra spark")

    session = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName("custom-ds-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
