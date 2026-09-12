"""Shared helper for the example scripts: build a local :class:`SparkSession`.

Examples are standalone scripts (not library code), so — unlike the ``spark_sql``
package — they are allowed to *create* a session here. Spark artifacts
(``spark-warehouse``, ``metastore_db``, ``derby.log``) are rooted under the
``DATA_HOME`` directory, mirroring ``tests/conftest.py``.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession


def data_home() -> Path:
    """Resolve the base directory for Spark artifacts from ``DATA_HOME``.

    Falls back to ``/tmp/spark-sql-examples`` when the variable is unset.
    """
    base = Path(os.environ.get("DATA_HOME", "/tmp/spark-sql-examples"))
    base.mkdir(parents=True, exist_ok=True)
    return base


def local_session(app_name: str = "spark-sql-examples") -> SparkSession:
    """Create (or reuse) a local :class:`SparkSession` for the examples.

    Honors the ``SPARK_MASTER`` env var with a ``local[*]`` fallback and roots all
    Spark artifacts under :func:`data_home`.
    """
    base = data_home()
    session = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.warehouse.dir", str(base / "spark-warehouse"))
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={base / 'metastore_db'};create=true")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={base}")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    return session
