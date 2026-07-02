"""
Tests for MERGE (upsert) logic implemented via DataFrame join + union pattern.

The pattern mirrors MERGE INTO semantics:
  - WHEN MATCHED THEN UPDATE → source values replace target
  - WHEN NOT MATCHED THEN INSERT → new source rows are added
  - Unmatched target rows are preserved unchanged
"""

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType


def _upsert(target: DataFrame, source: DataFrame, key_col: str) -> DataFrame:
    """Simulate MERGE INTO: source updates matched rows; new rows inserted; unmatched target kept."""
    # Matched rows: take values from source (source overrides target on key match)
    matched = source.join(target.select(key_col), on=key_col, how="inner")
    # Target rows whose key is absent in source — keep as-is
    unmatched_target = target.join(source.select(key_col), on=key_col, how="left_anti")
    # Source rows whose key is absent in target — insert them
    new_rows = source.join(target.select(key_col), on=key_col, how="left_anti")
    return matched.union(unmatched_target).union(new_rows)


@pytest.mark.unit
def test_upsert_updates_matched(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY"), ("c2", "Bob", "CA")],
        ["customer_id", "name", "city"],
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "LA")],  # c1 moves from NY → LA
        ["customer_id", "name", "city"],
    )
    result = _upsert(target, source, "customer_id")
    row = result.filter(F.col("customer_id") == "c1").first()
    assert row["city"] == "LA"


@pytest.mark.unit
def test_upsert_inserts_unmatched(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY")],
        ["customer_id", "name", "city"],
    )
    source = spark.createDataFrame(
        [("c2", "Bob", "CA")],  # brand new customer
        ["customer_id", "name", "city"],
    )
    result = _upsert(target, source, "customer_id")

    assert result.count() == 2
    assert result.filter(F.col("customer_id") == "c2").count() == 1


@pytest.mark.unit
def test_upsert_leaves_unmatched_target(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY"), ("c2", "Bob", "CA"), ("c3", "Charlie", "TX")],
        ["customer_id", "name", "city"],
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "LA")],  # only c1 in source
        ["customer_id", "name", "city"],
    )
    result = _upsert(target, source, "customer_id")

    # c2 and c3 are not in source — they must be preserved
    assert result.count() == 3
    assert result.filter(F.col("customer_id") == "c2").count() == 1
    assert result.filter(F.col("customer_id") == "c3").count() == 1


@pytest.mark.unit
def test_conditional_update_only_when_changed(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY"), ("c2", "Bob", "CA")],
        ["customer_id", "name", "city"],
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "TX"), ("c2", "Bob", "CA")],  # c1 changed, c2 identical
        ["customer_id", "name", "city"],
    )
    # Only update rows where the tracked value has changed
    target_prep = target.select(
        F.col("customer_id"),
        F.col("city").alias("target_city"),
    )
    source_prep = source.select(
        F.col("customer_id"),
        F.col("name"),
        F.col("city").alias("source_city"),
    )
    joined = target_prep.join(source_prep, on="customer_id", how="left")
    result = joined.withColumn(
        "city",
        F.when(F.col("target_city") != F.col("source_city"), F.col("source_city")).otherwise(F.col("target_city")),
    ).select("customer_id", "name", "city")

    rows = {r["customer_id"]: r["city"] for r in result.collect()}
    assert rows["c1"] == "TX"  # changed
    assert rows["c2"] == "CA"  # unchanged — source and target were identical


@pytest.mark.unit
def test_delete_matched_rows(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("_delete", BooleanType(), True),
        ]
    )
    target = spark.createDataFrame(
        [("c1", "Alice"), ("c2", "Bob"), ("c3", "Charlie")],
        ["customer_id", "name"],
    )
    source = spark.createDataFrame(
        [("c1", False), ("c2", True)],  # c2 is flagged for deletion
        schema,
    )
    delete_keys = source.filter(F.col("_delete")).select("customer_id")
    result = target.join(delete_keys, on="customer_id", how="left_anti")

    assert result.count() == 2
    assert result.filter(F.col("customer_id") == "c2").count() == 0
    assert result.filter(F.col("customer_id") == "c1").count() == 1
    assert result.filter(F.col("customer_id") == "c3").count() == 1
