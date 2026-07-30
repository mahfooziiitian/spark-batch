"""SparkSession factory with sensible defaults for JSON workloads.

This module provides create_spark_session() for library/test usage (no env setup).
For examples that need full env configuration, use pys_json.config.get_spark() instead.
"""

import os

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "pys-json",
    master: str | None = None,
    log_level: str = "WARN",
    configs: dict[str, str] | None = None,
) -> SparkSession:
    """Create a SparkSession configured for local JSON processing.

    Unlike get_spark(), this does NOT call configure_env() — it's intended
    for library/test code where the environment is already configured.

    Args:
        app_name: Application name shown in Spark UI.
        master: Spark master URL. Defaults to SPARK_MASTER env var or local[*].
        log_level: Log level for SparkContext (WARN, ERROR, INFO).
        configs: Additional Spark configuration key-value pairs.

    Returns:
        Configured SparkSession instance.
    """
    resolved_master = master or os.environ.get("SPARK_MASTER", "local[*]")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
    )
    if configs:
        for key, value in configs.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
