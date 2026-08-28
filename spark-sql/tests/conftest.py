from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-session")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
