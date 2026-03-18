"""
show() prints rows to stdout — it does NOT return a value.

  show()                        — first 20 rows, truncate at 20 chars
  show(n)                       — first n rows
  show(truncate=False)          — no column truncation
  show(truncate=N)              — truncate at N characters
  show(vertical=True)           — one column per line (wide schemas)
  show(n, truncate, vertical)   — all options combined

show() is an action: it triggers computation and collects n rows to the driver.
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

from data_frame.sample_data import customer_orders, employees, product_revenue
from data_frame.spark_utils import get_spark


def demo_show_defaults(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # Default show() — 20 rows, truncate at 20 characters
    # ------------------------------------------------------------------
    print("=== show() defaults ===")
    df.show()


def demo_show_n_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # show(n) — control number of rows displayed
    # ------------------------------------------------------------------
    print("=== show(3) — first 3 rows ===")
    df.show(3)

    print("=== show(100) on a 7-row DataFrame — shows all ===")
    df.show(100)


def demo_show_truncate(spark: SparkSession) -> None:
    # Wide string values to demonstrate truncation
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("description", StringType(), nullable=True),
            StructField("tags", StringType(), nullable=True),
        ]
    )
    data = [
        (
            1,
            "A very long product description that exceeds the default truncation limit",
            "electronics,mobile,5g,wireless,bluetooth",
        ),
        (2, "Short desc", "books"),
        (
            3,
            "Another lengthy description for demonstration purposes only",
            "apparel,clothing,summer,sale",
        ),
    ]
    df = spark.createDataFrame(data, schema)

    print("=== show() — default truncate at 20 chars ===")
    df.show()

    print("=== show(truncate=False) — full column values ===")
    df.show(truncate=False)

    print("=== show(truncate=40) — truncate at 40 chars ===")
    df.show(truncate=40)


def demo_show_vertical(spark: SparkSession) -> None:
    df = spark.createDataFrame(*product_revenue())

    # ------------------------------------------------------------------
    # vertical=True — one (column: value) pair per line
    # Ideal for wide schemas or deep inspection of individual rows
    # ------------------------------------------------------------------
    print("=== show(3, vertical=True) ===")
    df.show(3, vertical=True)

    print("=== show(2, truncate=False, vertical=True) ===")
    df.show(2, truncate=False, vertical=True)


def demo_show_after_transform(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # show() mid-pipeline for debugging — does not interrupt the chain
    # ------------------------------------------------------------------
    print("=== show() after filter + select ===")
    (
        df.filter(F.col("status") == "active")
        .select(
            "order_id",
            "product",
            F.round(F.col("quantity") * F.col("unit_price"), 2).alias("line_total"),
        )
        .orderBy(F.desc("line_total"))
        .show(truncate=False)
    )


def demo_show_with_nulls(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # NULLs are displayed as "null" in show() output
    # ------------------------------------------------------------------
    print("=== show() with NULL values ===")
    df.filter(F.col("customer_id").isNull()).show(truncate=False)


def main(spark: SparkSession) -> None:
    demo_show_defaults(spark)
    demo_show_n_rows(spark)
    demo_show_truncate(spark)
    demo_show_vertical(spark)
    demo_show_after_transform(spark)
    demo_show_with_nulls(spark)


if __name__ == "__main__":
    spark = get_spark("action-show")
    main(spark)
    spark.stop()
