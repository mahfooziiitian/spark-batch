"""Example: Arithmetic column functions — division, percentage, clamping.

Demonstrates null_safe_divide, percentage, clamp, and divide_by_three
column functions with chispa approximate assertions.

Run:
    PYTHONPATH=src uv run python examples/arithmetic_functions.py
"""

import os

from chispa import assert_approx_column_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame.functions.functions import clamp, null_safe_divide, percentage


def main() -> None:
    """Demonstrate arithmetic column functions."""
    spark = (
        SparkSession.builder.appName("example-arithmetic")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Null-safe division
    division_df = spark.createDataFrame(
        [(100.0, 3.0), (50.0, 0.0), (75.0, None), (200.0, 4.0)],
        ["revenue", "headcount"],
    )
    with_per_head = division_df.withColumn(
        "per_head", F.round(null_safe_divide(F.col("revenue"), F.col("headcount")), 2)
    )
    print("=== Null-Safe Division ===")
    with_per_head.show()

    # Percentage calculation
    budget_df = spark.createDataFrame(
        [("Engineering", 500000.0, 1200000.0), ("Marketing", 300000.0, 1200000.0), ("Sales", 400000.0, 1200000.0)],
        ["department", "spend", "total_budget"],
    )
    with_pct = budget_df.withColumn("budget_pct", percentage(F.col("spend"), F.col("total_budget")))
    print("=== Budget Percentages ===")
    with_pct.show()

    # Clamping values
    scores_df = spark.createDataFrame(
        [("Alice", -5.0), ("Bob", 42.0), ("Charlie", 150.0), ("Diana", 88.0)],
        ["student", "raw_score"],
    )
    clamped = scores_df.withColumn("final_score", clamp(F.col("raw_score"), 0.0, 100.0))
    print("=== Clamped Scores [0, 100] ===")
    clamped.show()

    # Verify with chispa approximate assertion
    verify_df = spark.createDataFrame(
        [(100.0, 3.0, 33.33), (50.0, 2.0, 25.0)],
        ["revenue", "headcount", "expected"],
    ).withColumn("actual", null_safe_divide(F.col("revenue"), F.col("headcount")))
    assert_approx_column_equality(verify_df, "actual", "expected", 0.01)
    print("✅ chispa approximate assertion passed")

    spark.stop()


if __name__ == "__main__":
    main()
