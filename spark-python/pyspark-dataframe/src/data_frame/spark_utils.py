import os
from typing import Optional

from pyspark.sql import SparkSession


def get_spark(
    app_name: str,
    extra_configs: Optional[dict] = None,
    log_level: str = "WARN",
) -> SparkSession:
    """Return a configured SparkSession.

    When the ``CONFIG_PROFILE`` environment variable is set, all settings are
    read from ``configs/<profile>.yaml`` via
    :func:`data_frame._shared.config_loader.get_spark_from_config`.

    Without ``CONFIG_PROFILE``, the function applies a minimal set of
    hard-coded defaults so that every script works locally without any
    configuration file:

    - ``SPARK_MASTER`` env var (fallback: ``local[*]``)
    - ``SPARK_SHUFFLE_PARTITIONS`` env var (fallback: ``4``)
    - AQE enabled, Web UI disabled

    ``extra_configs`` are always applied last and override both the YAML and
    the defaults.

    Args:
        app_name: Value for ``spark.app.name``.
        extra_configs: Optional dict of additional Spark config key/value pairs.
        log_level: Spark log level (default ``"WARN"``).  Ignored when
            ``CONFIG_PROFILE`` is set — use the ``spark.log_level`` YAML key.

    Returns:
        A ready-to-use :class:`SparkSession`.
    """
    if os.environ.get("CONFIG_PROFILE"):
        from data_frame._shared.config_loader import get_spark_from_config

        return get_spark_from_config(
            app_name,
            profile=os.environ["CONFIG_PROFILE"],
            extra_configs=extra_configs,
        )

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config(
            "spark.sql.shuffle.partitions",
            os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"),
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
    )

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
