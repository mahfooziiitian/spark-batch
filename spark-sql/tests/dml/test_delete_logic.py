import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.mark.unit
def test_delete_by_condition(spark: SparkSession) -> None:
    orders = spark.createDataFrame(
        [
            ("o1", "2023-12-31"),
            ("o2", "2024-01-15"),
            ("o3", "2024-03-01"),
        ],
        ["order_id", "order_date"],
    )
    cutoff = "2024-01-01"
    result = orders.filter(F.col("order_date") >= cutoff)

    assert result.count() == 2
    assert result.filter(F.col("order_id") == "o1").count() == 0


@pytest.mark.unit
def test_delete_orphaned_rows(spark: SparkSession) -> None:
    orders = spark.createDataFrame(
        [
            ("o1", "c1"),
            ("o2", "c2"),
            ("o3", "c99"),  # orphan — customer does not exist
        ],
        ["order_id", "customer_id"],
    )
    customers = spark.createDataFrame(
        [("c1",), ("c2",)],
        ["customer_id"],
    )
    # Keep only orders whose customer_id exists in the customers reference set
    result = orders.join(customers, on="customer_id", how="inner")

    assert result.count() == 2
    assert result.filter(F.col("order_id") == "o3").count() == 0


@pytest.mark.unit
def test_delete_all_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c")],
        ["id", "val"],
    )
    result = df.filter(F.lit(False))
    assert result.count() == 0
