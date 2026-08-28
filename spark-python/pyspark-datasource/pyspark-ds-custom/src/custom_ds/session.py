"""SparkSession helpers shared by examples, tests, and library code."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "custom-ds") -> SparkSession:
    """Create (or fetch) a local SparkSession for examples and tests.

    Uses the ``SPARK_MASTER`` env var with a ``local[*]`` fallback so every
    script runs locally without modification.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
