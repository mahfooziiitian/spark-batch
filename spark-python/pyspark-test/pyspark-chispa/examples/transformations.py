"""Example: DataFrame transformations — dedup, window functions, renaming.

Demonstrates modify_column_names, with_row_number, with_running_total,
deduplicate, and filter_nulls transformations.

Run:
    PYTHONPATH=src uv run python examples/transformations.py
"""

import os

from pyspark.sql import SparkSession

from data_frame.helper.string_helper import snake_case
from data_frame.transformation.df_transformations import (
    deduplicate,
    filter_nulls,
    modify_column_names,
    with_row_number,
    with_running_total,
)


def main() -> None:
    """Demonstrate DataFrame transformation patterns."""
    spark = (
        SparkSession.builder.appName("example-transformations")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Rename columns with snake_case ---
    messy_df = spark.createDataFrame(
        [("Alice", 95000, "Engineering")],
        ["Employee Name", "Annual Salary", "Department-Name"],
    )
    clean_df = modify_column_names(messy_df, snake_case)
    print("=== Renamed Columns ===")
    print(f"Before: {messy_df.columns}")
    print(f"After:  {clean_df.columns}")
    clean_df.show()

    # --- Row numbers ---
    sales_df = spark.createDataFrame(
        [("Alice", 150), ("Bob", 230), ("Charlie", 90), ("Diana", 310)],
        ["name", "revenue"],
    )
    numbered = with_row_number(sales_df, order_col="revenue", alias="rank")
    print("=== Ranked by Revenue ===")
    numbered.orderBy("rank").show()

    # --- Running total ---
    daily = spark.createDataFrame(
        [("Mon", 100), ("Tue", 200), ("Wed", 150), ("Thu", 300), ("Fri", 250)],
        ["day", "sales"],
    )
    with_total = with_running_total(daily, value_col="sales", order_col="day")
    print("=== Running Total ===")
    with_total.orderBy("day").show()

    # --- Deduplication ---
    events = spark.createDataFrame(
        [
            ("user1", "login", 1),
            ("user1", "login", 2),
            ("user1", "login", 3),
            ("user2", "login", 1),
            ("user2", "login", 2),
        ],
        ["user_id", "event", "timestamp"],
    )
    latest = deduplicate(events, subset=["user_id", "event"], order_col="timestamp", keep="last")
    print("=== Deduplicated (keep last) ===")
    latest.show()

    # --- Filter nulls ---
    dirty = spark.createDataFrame(
        [(1, "Alice", "alice@co.com"), (2, None, "bob@co.com"), (3, "Charlie", None), (4, "Diana", "diana@co.com")],
        ["id", "name", "email"],
    )
    clean = filter_nulls(dirty, columns=["name", "email"])
    print("=== After Filtering Nulls ===")
    clean.show()
    print(f"Removed {dirty.count() - clean.count()} rows with nulls")

    spark.stop()


if __name__ == "__main__":
    main()
