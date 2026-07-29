"""Example: Basic DataFrame operations and transformations.

Demonstrates creating DataFrames, applying transformations, and
writing output using PySpark.

Run:
    uv run python examples/basic_dataframe.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pys.transformation.df_transformation import (
    remove_extra_spaces,
    to_title_case,
    trim_all_columns,
)


def main() -> None:
    """Run basic DataFrame transformation examples."""
    spark = (
        SparkSession.builder.appName("example-basic-dataframe")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Create a sample DataFrame
    data = [
        ("  alice smith  ", 30, "engineering"),
        ("  BOB JONES  ", 25, "marketing"),
        ("  charlie  brown  ", 35, "engineering"),
        ("  diana  prince  ", 28, "sales"),
    ]
    df = spark.createDataFrame(data, ["name", "age", "department"])

    print("=== Original DataFrame ===")
    df.show(truncate=False)

    # Apply transformations
    cleaned = trim_all_columns(df)
    cleaned = remove_extra_spaces(cleaned, "name")
    cleaned = to_title_case(cleaned, "name")

    print("=== After Cleaning ===")
    cleaned.show(truncate=False)

    # Aggregation example
    dept_stats = cleaned.groupBy("department").agg(
        F.count("*").alias("count"),
        F.round(F.avg("age"), 1).alias("avg_age"),
        F.min("age").alias("min_age"),
        F.max("age").alias("max_age"),
    )

    print("=== Department Stats ===")
    dept_stats.show()

    spark.stop()


if __name__ == "__main__":
    main()
