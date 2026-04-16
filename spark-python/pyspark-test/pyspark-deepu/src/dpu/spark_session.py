"""Shared SparkSession builder for PyDeequ examples."""

import os

os.environ["SPARK_VERSION"] = "3.5"

import pydeequ  # must be imported after setting SPARK_VERSION
from pyspark.sql import SparkSession


def create_spark(app_name: str) -> SparkSession:
    """Create a SparkSession preconfigured for PyDeequ.

    Reads ``SPARK_MASTER`` from the environment, falling back to ``local[*]``.
    The Deequ Maven JAR is added automatically.

    Args:
        app_name: Human-readable application name.

    Returns:
        A ready-to-use SparkSession.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
