"""Example: Window functions and advanced aggregations.

Demonstrates PySpark window functions including ranking, running totals,
and lag/lead operations commonly used in data engineering.

Run:
    uv run python examples/window_functions.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main() -> None:
    """Demonstrate window function patterns."""
    spark = (
        SparkSession.builder.appName("example-window-functions")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Sales data
    data = [
        ("Engineering", "Alice", 95000),
        ("Engineering", "Bob", 105000),
        ("Engineering", "Charlie", 88000),
        ("Marketing", "Diana", 72000),
        ("Marketing", "Eve", 78000),
        ("Marketing", "Frank", 81000),
        ("Sales", "Grace", 65000),
        ("Sales", "Henry", 71000),
    ]
    df = spark.createDataFrame(data, ["department", "name", "salary"])

    print("=== Employee Data ===")
    df.show()

    # Rank within department
    dept_window = Window.partitionBy("department").orderBy(F.desc("salary"))

    ranked = df.withColumn("rank", F.rank().over(dept_window)).withColumn(
        "dense_rank", F.dense_rank().over(dept_window)
    )

    print("=== Ranked by Salary within Department ===")
    ranked.show()

    # Running total and department statistics
    dept_frame = Window.partitionBy("department")

    stats = df.select(
        "department",
        "name",
        "salary",
        F.sum("salary").over(dept_frame).alias("dept_total"),
        F.avg("salary").over(dept_frame).alias("dept_avg"),
        F.round(F.col("salary") / F.sum("salary").over(dept_frame) * 100, 1).alias("pct_of_dept"),
    )

    print("=== Department Statistics ===")
    stats.show()

    # Lag/Lead: compare with previous/next salary in department
    ordered_window = Window.partitionBy("department").orderBy("salary")

    comparisons = df.select(
        "department",
        "name",
        "salary",
        F.lag("salary", 1).over(ordered_window).alias("prev_salary"),
        F.lead("salary", 1).over(ordered_window).alias("next_salary"),
        (F.col("salary") - F.lag("salary", 1).over(ordered_window)).alias("diff_from_prev"),
    )

    print("=== Salary Comparisons (Lag/Lead) ===")
    comparisons.show()

    spark.stop()


if __name__ == "__main__":
    main()
