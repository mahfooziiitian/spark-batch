import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_append_rows(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [(1, "Alice"), (2, "Bob")],
        ["id", "name"],
    )
    source = spark.createDataFrame(
        [(3, "Charlie"), (4, "Diana")],
        ["id", "name"],
    )
    result = target.union(source)
    assert result.count() == 4


@pytest.mark.unit
def test_overwrite_replaces_data(spark: SparkSession) -> None:
    spark.createDataFrame(
        [(1, "Alice"), (2, "Bob")],
        ["id", "name"],
    )
    new_data = spark.createDataFrame(
        [(10, "Zara")],
        ["id", "name"],
    )
    # overwrite: target is fully replaced by new_data
    result = new_data
    expected = spark.createDataFrame([(10, "Zara")], ["id", "name"])
    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_partition_insert_logic(spark: SparkSession) -> None:
    target = spark.createDataFrame(
        [(1, "Alice", "US"), (2, "Bob", "UK")],
        ["id", "name", "region"],
    )
    source = spark.createDataFrame(
        [(3, "Charlie", "US"), (4, "Diana", "EU"), (5, "Eve", "US")],
        ["id", "name", "region"],
    )
    # Insert only rows belonging to the 'US' partition
    us_source = source.filter(source["region"] == "US")
    result = target.union(us_source)

    assert result.count() == 4
    assert result.filter(result["region"] == "US").count() == 3
