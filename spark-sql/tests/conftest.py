from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from collections.abc import Generator

from spark_sql._helpers import REPO_ROOT

if os.environ.get("JAVA_HOME") is None:
    os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_17", "")


def _data_home() -> Path:
    """Resolve the base directory for Spark artifacts.

    Uses the ``DATA_HOME`` environment variable when set, otherwise falls back to
    the repository root so ``make clean`` / ``just clean`` keep working.
    """
    data_home = Path(os.environ.get("DATA_HOME", str(REPO_ROOT)))
    data_home.mkdir(parents=True, exist_ok=True)
    return data_home


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    data_home = _data_home()
    warehouse_dir = data_home / "spark-warehouse"
    metastore_dir = data_home / "metastore_db"

    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-session")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore_dir};create=true")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={data_home}")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
