"""
Tests for SCD Type 2 logic.

SCD2 preserves full history: when a tracked attribute changes, the current
record is closed (is_current=False) and a new record is opened (is_current=True).
"""

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

SCD2_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("effective_from", StringType(), True),
        StructField("effective_to", StringType(), True),
    ]
)

SOURCE_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
    ]
)


def _apply_scd2(target: DataFrame, source: DataFrame) -> DataFrame:
    """
    Simulate SCD Type 2 merge on customer_id, tracking name and city.

    Rules:
    - Unchanged rows: keep is_current=True, no modification.
    - Changed rows: close current record (is_current=False, effective_to set)
                    and insert a new current record from source.
    - New source rows: insert as current records.
    - Pre-existing historical rows: preserved as-is.
    """
    current = target.filter(F.col("is_current"))
    historical = target.filter(~F.col("is_current"))

    joined = current.alias("t").join(
        source.alias("s"),
        on=F.col("t.customer_id") == F.col("s.customer_id"),
        how="outer",
    )

    exists_in_current = F.col("t.customer_id").isNotNull()
    exists_in_source = F.col("s.customer_id").isNotNull()
    changed = (F.col("t.name") != F.col("s.name")) | (F.col("t.city") != F.col("s.city"))

    unchanged = joined.filter(exists_in_current & exists_in_source & ~changed).select(
        F.col("t.customer_id"),
        F.col("t.name"),
        F.col("t.city"),
        F.col("t.is_current"),
        F.col("t.effective_from"),
        F.col("t.effective_to"),
    )

    closed = joined.filter(exists_in_current & exists_in_source & changed).select(
        F.col("t.customer_id"),
        F.col("t.name"),
        F.col("t.city"),
        F.lit(False).alias("is_current"),
        F.col("t.effective_from"),
        F.lit("2024-12-31").alias("effective_to"),
    )

    new_versions = joined.filter(exists_in_current & exists_in_source & changed).select(
        F.col("s.customer_id"),
        F.col("s.name"),
        F.col("s.city"),
        F.lit(True).alias("is_current"),
        F.lit("2025-01-01").alias("effective_from"),
        F.lit(None).cast("string").alias("effective_to"),
    )

    new_records = joined.filter(~exists_in_current & exists_in_source).select(
        F.col("s.customer_id"),
        F.col("s.name"),
        F.col("s.city"),
        F.lit(True).alias("is_current"),
        F.lit("2025-01-01").alias("effective_from"),
        F.lit(None).cast("string").alias("effective_to"),
    )

    return unchanged.union(closed).union(new_versions).union(new_records).union(historical)


@pytest.mark.unit
def test_scd2_unchanged_rows_not_closed(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY", True, "2024-01-01", None)],
        SCD2_SCHEMA,
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "NY")],  # identical to target
        SOURCE_SCHEMA,
    )
    result = _apply_scd2(target, source)
    row = result.filter(F.col("customer_id") == "c1").first()
    assert row["is_current"] is True


@pytest.mark.unit
def test_scd2_changed_rows_closed(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY", True, "2024-01-01", None)],
        SCD2_SCHEMA,
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "LA")],  # city changed NY → LA
        SOURCE_SCHEMA,
    )
    result = _apply_scd2(target, source)
    # The original NY record must be closed
    closed_row = result.filter((F.col("customer_id") == "c1") & (F.col("city") == "NY")).first()
    assert closed_row is not None
    assert closed_row["is_current"] is False
    assert closed_row["effective_to"] == "2024-12-31"


@pytest.mark.unit
def test_scd2_new_versions_inserted(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY", True, "2024-01-01", None)],
        SCD2_SCHEMA,
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "LA")],
        SOURCE_SCHEMA,
    )
    result = _apply_scd2(target, source)
    # A new current record with the updated city must exist
    new_row = result.filter((F.col("customer_id") == "c1") & (F.col("city") == "LA")).first()
    assert new_row is not None
    assert new_row["is_current"] is True
    assert new_row["effective_to"] is None


@pytest.mark.unit
def test_scd2_new_customers_inserted(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY", True, "2024-01-01", None)],
        SCD2_SCHEMA,
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "NY"), ("c3", "Carol", "TX")],  # c3 is brand new
        SOURCE_SCHEMA,
    )
    result = _apply_scd2(target, source)
    new_row = result.filter(F.col("customer_id") == "c3").first()
    assert new_row is not None
    assert new_row["is_current"] is True
    assert new_row["city"] == "TX"


@pytest.mark.unit
def test_scd2_current_snapshot(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [
            ("c1", "Alice", "NY", True, "2024-01-01", None),
            ("c2", "Bob", "CA", True, "2024-01-01", None),
        ],
        SCD2_SCHEMA,
    )
    source = spark.createDataFrame(
        [
            ("c1", "Alice", "LA"),  # c1 changed
            ("c2", "Bob", "CA"),  # c2 unchanged
        ],
        SOURCE_SCHEMA,
    )
    result = _apply_scd2(target, source)
    current_snapshot = result.filter(F.col("is_current"))

    # Current snapshot should have exactly one row per customer
    assert current_snapshot.count() == 2
    cities = {r["customer_id"]: r["city"] for r in current_snapshot.collect()}
    assert cities["c1"] == "LA"
    assert cities["c2"] == "CA"


@pytest.mark.unit
def test_scd2_history_preserved(spark: SparkSession) -> None:
    # Target already contains a pre-existing closed (historical) record for c1
    target = spark.createDataFrame(
        [
            ("c1", "Alice", "TX", False, "2023-01-01", "2023-12-31"),  # old history
            ("c1", "Alice", "NY", True, "2024-01-01", None),  # current
        ],
        SCD2_SCHEMA,
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "LA")],  # c1 moves again
        SOURCE_SCHEMA,
    )
    result = _apply_scd2(target, source)

    # All three versions must be present: TX (hist), NY (closed), LA (new current)
    assert result.count() == 3
    tx_row = result.filter((F.col("customer_id") == "c1") & (F.col("city") == "TX")).first()
    assert tx_row is not None
    assert tx_row["is_current"] is False
