"""
Shared SparkSession factory.

Every example in this project calls ``get_spark()`` instead of repeating
the full builder configuration.  The factory:

- Reads ``SPARK_MASTER`` from the environment (defaults to ``local[*]``).
- Enables Adaptive Query Execution (AQE) unconditionally.
- Optionally enables the FAIR scheduler and auto-discovers
  ``scheduling/fairscheduler.xml`` relative to this file.
- Sets a predictable ``shuffle.partitions`` for local runs.
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession

_SCHEDULER_XML = (
    Path(__file__).parent.parent / "scheduling" / "fairscheduler.xml"
)


def get_spark(
    app_name: str,
    *,
    fair: bool = True,
    shuffle_partitions: int = 4,
    ui: bool = False,
) -> SparkSession:
    """Create or retrieve the shared SparkSession.

    Args:
        app_name: Name shown in the Spark Web UI and logs.
        fair: Enable FAIR scheduler (required for parallel jobs).
        shuffle_partitions: ``spark.sql.shuffle.partitions`` — keep low
            for local examples; use 200 for cluster workloads.
        ui: Enable the Spark Web UI (disabled by default to speed up startup).

    Returns:
        A configured, ready-to-use SparkSession.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.enabled", str(ui).lower())
    )

    if fair:
        builder = builder.config("spark.scheduler.mode", "FAIR")
        if _SCHEDULER_XML.exists():
            builder = builder.config(
                "spark.scheduler.allocation.file",
                str(_SCHEDULER_XML.resolve()),
            )

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
