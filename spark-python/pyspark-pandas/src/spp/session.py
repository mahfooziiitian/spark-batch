"""Reusable SparkSession factory.

Provides ``create_spark_session`` — a single function that builds an
Arrow-enabled, AQE-enabled SparkSession controlled by environment
variables.  Every script and test in this project should use this
instead of duplicating the builder boilerplate.

Usage::

    from spp.session import create_spark_session

    spark = create_spark_session("my-job")
    # ... work ...
    spark.stop()
"""

import os

from pyspark.sql import SparkSession

import spp._env  # noqa: F401  (early env setup)


def create_spark_session(
    app_name: str = "pyspark-pandas",
    *,
    master: str | None = None,
    shuffle_partitions: int = 4,
    log_level: str = "WARN",
    enable_arrow: bool = True,
    enable_hive: bool = False,
    extra_configs: dict[str, str] | None = None,
) -> SparkSession:
    """Create a configured SparkSession.

    Parameters
    ----------
    app_name:
        Spark application name shown in the UI.
    master:
        Spark master URL.  Falls back to the ``SPARK_MASTER`` env var,
        then to ``local[*]``.
    shuffle_partitions:
        ``spark.sql.shuffle.partitions`` value.  Defaults to ``4`` for
        local examples; use ``200`` for cluster workloads.
    log_level:
        Root log level — ``"WARN"`` for scripts, ``"ERROR"`` for tests.
    enable_arrow:
        Enable Arrow-based columnar transfer for ``toPandas()`` /
        ``createDataFrame(pdf)``.
    enable_hive:
        Call ``.enableHiveSupport()`` on the builder.
    extra_configs:
        Additional ``key: value`` pairs passed to ``.config()``.
    """
    resolved_master = master or os.environ.get("SPARK_MASTER", "local[*]")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config(
            "spark.sql.shuffle.partitions",
            str(shuffle_partitions),
        )
    )

    if enable_arrow:
        builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")

    if enable_hive:
        builder = builder.enableHiveSupport()

    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
