import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Shared SparkSession for the entire test suite."""
    return (
        SparkSession.builder.master("local[2]")
        .appName("test-session")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
