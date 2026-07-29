"""Example: DataFrame equality comparisons with chispa.

Demonstrates asserting DataFrame equality, approximate equality,
row diffs, and column sorting utilities.

Run:
    PYTHONPATH=src uv run python examples/dataframe_equality.py
"""

import os

from chispa import assert_df_equality
from chispa.dataframe_comparer import DataFramesNotEqualError, assert_approx_df_equality
from pyspark.sql import SparkSession

from data_frame.equality.df_equality import columns_match, row_diff, sort_columns, union_dedup


def main() -> None:
    """Demonstrate DataFrame equality patterns with chispa."""
    spark = (
        SparkSession.builder.appName("example-df-equality")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Exact equality
    df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    df2 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    assert_df_equality(df1, df2)
    print("✅ Exact equality: DataFrames match")

    # Approximate equality (useful for floating point)
    df_actual = spark.createDataFrame([(1.001,), (2.002,)], ["value"])
    df_expected = spark.createDataFrame([(1.0,), (2.0,)], ["value"])
    assert_approx_df_equality(df_actual, df_expected, precision=0.01)
    print("✅ Approximate equality: within threshold of 0.01")

    # Sort columns
    messy = spark.createDataFrame([("a", 1, True)], ["z_col", "a_col", "m_col"])
    sorted_df = sort_columns(messy, "asc")
    print(f"\n=== Sorted columns: {sorted_df.columns} ===")

    # Column matching
    df_a = spark.createDataFrame([(1,)], ["id"])
    df_b = spark.createDataFrame([(2,)], ["id"])
    df_c = spark.createDataFrame([(3,)], ["key"])
    print(f"\nColumns match (id vs id): {columns_match(df_a, df_b)}")
    print(f"Columns match (id vs key): {columns_match(df_a, df_c)}")

    # Row diff
    left = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    right = spark.createDataFrame([(2,), (3,)], ["id"])
    diff = row_diff(left, right)
    print("\n=== Row Diff (left - right) ===")
    diff.show()

    # Union dedup
    set1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    set2 = spark.createDataFrame([(2, "b"), (3, "c")], ["id", "val"])
    combined = union_dedup(set1, set2)
    print("=== Union Dedup ===")
    combined.show()

    # Detect inequality
    try:
        bad1 = spark.createDataFrame([(1,)], ["id"])
        bad2 = spark.createDataFrame([(999,)], ["id"])
        assert_df_equality(bad1, bad2)
    except DataFramesNotEqualError:
        print("✅ Inequality correctly detected by chispa")

    spark.stop()


if __name__ == "__main__":
    main()
