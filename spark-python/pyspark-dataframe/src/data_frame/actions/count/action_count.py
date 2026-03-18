"""
count() and related actions that return a single integer to the driver.

  count()              → int   — total rows in the DataFrame
  df.distinct().count()→ int   — distinct row count
  isEmpty()            → bool  — True when count() == 0  (Spark 3.3+)
  filter(...).count()  → int   — conditional row count

count() always triggers a full scan — cache the DataFrame first if you need
multiple counts over the same data.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame.sample_data import customer_orders, employees
from data_frame.spark_utils import get_spark


def demo_count(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # count() — total number of rows (includes rows with NULLs)
    # ------------------------------------------------------------------
    total = df.count()
    print(f"Total rows: {total}")

    # count(*) vs count(col) — count(*) includes NULLs, count(col) excludes them
    orders = spark.createDataFrame(*customer_orders())
    count_star = orders.count()
    count_customer = orders.select(F.count("customer_id")).first()[0]
    count_star_fn = orders.select(F.count("*")).first()[0]
    print(f"\ncustomer_orders count(*)         : {count_star}")
    print(f"customer_orders count('*') fn     : {count_star_fn}")
    print(f"customer_orders count(customer_id): {count_customer}  ← excludes NULLs")


def demo_filtered_count(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # Conditional counts — filter before count to avoid full collect
    # ------------------------------------------------------------------
    active_count = df.filter(F.col("status") == "active").count()
    inactive_count = df.filter(F.col("status") == "inactive").count()
    null_customer = df.filter(F.col("customer_id").isNull()).count()

    print(f"\nActive orders   : {active_count}")
    print(f"Inactive orders : {inactive_count}")
    print(f"NULL customer_id: {null_customer}")

    # Count by group without collecting
    print("\nCounts per product:")
    (df.groupBy("product").count().orderBy(F.desc("count")).show(truncate=False))


def demo_distinct_count(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # distinct().count() — number of unique rows
    # countDistinct(col) — number of unique values in one column
    # ------------------------------------------------------------------
    distinct_rows = df.distinct().count()
    unique_products = df.select(F.countDistinct("product")).first()[0]
    unique_customers = (
        df.filter(F.col("customer_id").isNotNull())
        .select(F.countDistinct("customer_id"))
        .first()[0]
    )

    print(f"\nDistinct rows       : {distinct_rows}")
    print(f"Unique products     : {unique_products}")
    print(f"Unique customers    : {unique_customers}")

    # dropDuplicates + count pattern (subset dedup)
    deduped = df.dropDuplicates(["customer_id", "product"]).count()
    print(f"Deduped (cust+prod) : {deduped}")


def demo_is_empty(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # isEmpty() — bool, Spark 3.3+
    # Equivalent to df.count() == 0 but may short-circuit earlier
    # ------------------------------------------------------------------
    print(f"\nemployees.isEmpty()          : {df.isEmpty()}")

    empty_df = df.filter(F.col("id") == -1)
    print(f"filtered(id=-1).isEmpty()    : {empty_df.isEmpty()}")

    # isEmpty() vs take(1) pattern (pre-3.3 alternative)
    is_empty_take = len(df.take(1)) == 0
    print(f"take(1) empty check (pre-3.3): {is_empty_take}")


def demo_row_count_assertions(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # Count-based guards — useful in pipeline validation steps
    # ------------------------------------------------------------------
    row_count = df.count()
    assert row_count > 0, "DataFrame must not be empty"
    assert row_count == 6, f"Expected 6 employees, got {row_count}"

    null_ids = df.filter(F.col("id").isNull()).count()
    assert null_ids == 0, f"Found {null_ids} rows with NULL id"

    print(f"\nAll count assertions passed (rows={row_count})")


def main(spark: SparkSession) -> None:
    demo_count(spark)
    demo_filtered_count(spark)
    demo_distinct_count(spark)
    demo_is_empty(spark)
    demo_row_count_assertions(spark)


if __name__ == "__main__":
    spark = get_spark("action-count")
    main(spark)
    spark.stop()
