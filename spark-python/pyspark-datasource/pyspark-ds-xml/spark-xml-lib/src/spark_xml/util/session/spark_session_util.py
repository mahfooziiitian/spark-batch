"""Shared SparkSession factory for spark-xml example scripts."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str,
    scala_version: str = "2.12",
    spark_xml_version: str = "0.18.0",
    master: str = "local[*]",
    log_level: str = "WARN",
    warehouse_dir: str | None = None,
    enable_ui: bool = False,
    **extra_configs: str,
) -> SparkSession:
    """Create or retrieve a SparkSession pre-configured for spark-xml.

    Parameters
    ----------
    app_name:
        Spark application name.
    scala_version:
        Scala binary version used to resolve the spark-xml Maven
        artifact (``"2.12"`` or ``"2.13"``).
    spark_xml_version:
        Version of the ``com.databricks:spark-xml`` JAR to download.
    master:
        Spark master URL. Defaults to ``"local[*]"`` for local
        development.  Pass ``None`` to omit (e.g. when using
        ``spark-submit``).
    log_level:
        Root log level applied after session creation. Common
        values: ``"ERROR"``, ``"WARN"``, ``"INFO"``.
    warehouse_dir:
        Optional ``spark.sql.warehouse.dir`` path.  Must be set at
        build time because it is a static Spark config.
    enable_ui:
        Whether to start the Spark Web UI. Disabled by default to
        speed up local scripts.
    **extra_configs:
        Arbitrary ``key=value`` pairs forwarded to
        ``SparkSession.builder.config()``.

    Returns
    -------
    SparkSession
        A running SparkSession instance.
    """
    builder = SparkSession.builder.appName(app_name)

    if master is not None:
        builder = builder.master(master)

    builder = builder.config(
        "spark.jars.packages",
        f"com.databricks:spark-xml_{scala_version}:{spark_xml_version}",
    )

    if warehouse_dir is not None:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)

    if not enable_ui:
        builder = builder.config("spark.ui.enabled", "false")

    for key, value in extra_configs.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
