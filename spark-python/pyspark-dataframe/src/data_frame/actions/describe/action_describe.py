"""
describe() and summary() compute summary statistics as a new DataFrame.

  describe(*cols)            — count, mean, stddev, min, max  (string + numeric)
  summary(*statistics)       — extended stats including percentiles (numeric only)

Both return a DataFrame — call show() or collect() to materialise results.
Statistics are computed as strings even for numeric columns.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_frame.sample_data import customer_orders, product_revenue
from data_frame.spark_utils import get_spark


def demo_describe_all(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # describe() — all columns: count, mean, stddev, min, max
    # Works on both numeric and string columns
    # ------------------------------------------------------------------
    print("=== describe() — all columns ===")
    df.describe().show(truncate=False)


def demo_describe_subset(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # describe(*cols) — selected columns only
    # ------------------------------------------------------------------
    print("=== describe('quantity', 'unit_price') ===")
    df.describe("quantity", "unit_price").show(truncate=False)

    print("=== describe('product') — string column stats ===")
    df.describe("product").show(truncate=False)
    # min/max on strings are lexicographic; mean/stddev are null for strings


def demo_summary(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # summary() — includes percentiles (25%, 50%, 75%) and inter-quartile
    # Only meaningful for numeric columns
    # ------------------------------------------------------------------
    print("=== summary() — all statistics ===")
    df.summary().show(truncate=False)

    print("=== summary('count', 'mean', '50%', 'max') — selected stats ===")
    df.summary("count", "mean", "50%", "max").show(truncate=False)

    print("=== summary('25%', '50%', '75%') — quartiles only ===")
    df.select("revenue").summary("25%", "50%", "75%").show(truncate=False)


def demo_describe_mixed_schema(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("name", StringType(), nullable=True),
            StructField("age", IntegerType(), nullable=True),
            StructField("salary", DoubleType(), nullable=True),
            StructField("active", StringType(), nullable=True),
        ]
    )
    data = [
        ("Alice", 30, 85000.0, "true"),
        ("Bob", 45, 72000.0, "false"),
        ("Carol", None, 91000.0, "true"),
        ("Dave", 38, None, "true"),
    ]
    df = spark.createDataFrame(data, schema)

    print("=== describe() with NULLs — count reflects non-null values ===")
    df.describe().show(truncate=False)
    # count for 'age'    = 3 (one NULL)
    # count for 'salary' = 3 (one NULL)


def demo_describe_to_dict(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # Collect describe() result to a dict for programmatic access
    # ------------------------------------------------------------------
    stats = {
        row["summary"]: {col: row[col] for col in df.columns if col != "summary"}
        for row in df.describe().collect()
    }

    revenue_mean = float(stats["mean"]["revenue"])
    revenue_max = float(stats["max"]["revenue"])
    print(f"\nRevenue mean : {revenue_mean:.2f}")
    print(f"Revenue max  : {revenue_max:.2f}")


def main(spark: SparkSession) -> None:
    demo_describe_all(spark)
    demo_describe_subset(spark)
    demo_summary(spark)
    demo_describe_mixed_schema(spark)
    demo_describe_to_dict(spark)


if __name__ == "__main__":
    spark = get_spark("action-describe")
    main(spark)
    spark.stop()
