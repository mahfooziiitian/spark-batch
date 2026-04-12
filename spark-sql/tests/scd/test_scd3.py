"""
Tests for SCD Type 3 logic.

SCD3 keeps only the current and one previous value in the same row.
When a tracked attribute changes, the old current value shifts to the
previous_* column, and the new value becomes the current_* column.
"""

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

SCD3_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("current_city", StringType(), True),
        StructField("previous_city", StringType(), True),
    ]
)

SOURCE_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("current_city", StringType(), True),
    ]
)


def _apply_scd3(target: DataFrame, source: DataFrame) -> DataFrame:
    """
    Simulate SCD Type 3 merge on customer_id, tracking current_city.

    Rules:
    - Unchanged rows: no modification.
    - Changed rows: old current_city → previous_city; source city → current_city.
    - New source rows: inserted with previous_city = NULL.
    """
    joined = target.alias("t").join(
        source.alias("s"),
        on=F.col("t.customer_id") == F.col("s.customer_id"),
        how="outer",
    )

    has_target = F.col("t.customer_id").isNotNull()
    has_source = F.col("s.customer_id").isNotNull()
    changed = F.col("t.current_city") != F.col("s.current_city")

    unchanged = joined.filter(has_target & has_source & ~changed).select(
        F.col("t.customer_id"),
        F.col("t.name"),
        F.col("t.current_city"),
        F.col("t.previous_city"),
    )

    updated = joined.filter(has_target & has_source & changed).select(
        F.col("t.customer_id"),
        F.col("s.name"),
        F.col("s.current_city"),
        F.col("t.current_city").alias("previous_city"),  # shift old → previous
    )

    new_records = joined.filter(~has_target & has_source).select(
        F.col("s.customer_id"),
        F.col("s.name"),
        F.col("s.current_city"),
        F.lit(None).cast("string").alias("previous_city"),
    )

    return unchanged.union(updated).union(new_records)


@pytest.mark.unit
def test_scd3_previous_shifts_to_old_column(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY", "TX")],  # currently in NY, was in TX
        SCD3_SCHEMA,
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "LA")],  # moves to LA
        SOURCE_SCHEMA,
    )
    result = _apply_scd3(target, source)
    row = result.filter(F.col("customer_id") == "c1").first()
    assert row["previous_city"] == "NY"  # old current shifts to previous


@pytest.mark.unit
def test_scd3_new_value_in_current_column(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY", "TX")],
        SCD3_SCHEMA,
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "LA")],
        SOURCE_SCHEMA,
    )
    result = _apply_scd3(target, source)
    row = result.filter(F.col("customer_id") == "c1").first()
    assert row["current_city"] == "LA"  # new city is in current_city


@pytest.mark.unit
def test_scd3_unchanged_rows_not_modified(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY", "TX")],  # city is still NY
        SCD3_SCHEMA,
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "NY")],  # same city as current
        SOURCE_SCHEMA,
    )
    result = _apply_scd3(target, source)
    row = result.filter(F.col("customer_id") == "c1").first()
    assert row["current_city"] == "NY"
    assert row["previous_city"] == "TX"  # previous must not be overwritten


@pytest.mark.unit
def test_scd3_new_records_inserted(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [("c1", "Alice", "NY", "TX")],
        SCD3_SCHEMA,
    )
    source = spark.createDataFrame(
        [("c1", "Alice", "NY"), ("c2", "Bob", "SF")],  # c2 is brand new
        SOURCE_SCHEMA,
    )
    result = _apply_scd3(target, source)

    assert result.count() == 2
    new_row = result.filter(F.col("customer_id") == "c2").first()
    assert new_row is not None
    assert new_row["current_city"] == "SF"
    assert new_row["previous_city"] is None  # no prior address
