"""Enterprise-style :class:`~pyspark.sql.SparkSession` factory.

Centralizes the conventions used across this project's examples: local-vs-cluster
master resolution via ``SPARK_MASTER``, a configurable warehouse directory, adaptive
query execution, a sane default log level, and (for callers still targeting Spark 3.x)
optional configuration of the legacy Databricks ``spark-xml`` JAR via Maven coordinates.

On Spark 4.0+ the ``xml`` format is a built-in data source, so
``scala_version``/``spark_xml_version`` are unused by default and retained only for
backward compatibility with Spark 3.x deployments that still need the external JAR.
"""

import os
from pathlib import Path
from typing import Mapping, Optional

from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str,
    master: Optional[str] = None,
    scala_version: str = "2.12",
    spark_xml_version: str = "0.17.0",
    use_external_spark_xml_jar: bool = False,
    warehouse_dir: Optional[str] = None,
    log_level: str = "WARN",
    extra_conf: Optional[Mapping[str, str]] = None,
) -> SparkSession:
    """Build (or fetch) a :class:`SparkSession` with production-friendly defaults.

    Parameters
    ----------
    app_name:
        Spark application name.
    master:
        Spark master URL. Defaults to the ``SPARK_MASTER`` env var, falling back
        to ``local[*]`` so every example runs locally without modification.
    scala_version, spark_xml_version:
        Retained for backward compatibility with Spark 3.x, where the
        Databricks ``spark-xml`` package must be added as a ``--packages``
        dependency. Ignored unless ``use_external_spark_xml_jar`` is ``True``.
    use_external_spark_xml_jar:
        Set to ``True`` only when running against Spark < 4.0, where the
        ``xml`` format is not built in.
    warehouse_dir:
        Directory for ``spark.sql.warehouse.dir``. Defaults to the
        ``SPARK_WAREHOUSE`` env var, falling back to ``/tmp/spark-warehouse``.
        Created automatically if missing.
    log_level:
        Passed to ``sparkContext.setLogLevel`` after session creation.
    extra_conf:
        Additional Spark configuration key/value pairs applied before
        ``getOrCreate()``.

    Returns
    -------
    SparkSession
    """
    resolved_master = master or os.environ.get("SPARK_MASTER", "local[*]")
    resolved_warehouse = Path(warehouse_dir or os.environ.get("SPARK_WAREHOUSE", "/tmp/spark-warehouse"))
    resolved_warehouse.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.sql.warehouse.dir", str(resolved_warehouse))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if use_external_spark_xml_jar:
        builder = builder.config(
            "spark.jars.packages",
            f"com.databricks:spark-xml_{scala_version}:{spark_xml_version}",
        )

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
