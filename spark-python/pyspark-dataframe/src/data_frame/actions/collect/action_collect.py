"""
collect() and its variants pull rows from all executors back to the driver.

  collect()   → List[Row]   — all rows
  first()     → Row         — single first row (never None)
  head(n)     → List[Row]   — first n rows (head() == head(1) returns one Row)
  take(n)     → List[Row]   — first n rows (alias for head when n > 0)
  tail(n)     → List[Row]   — last n rows (Spark 3.0+)

WARNING: collect() on a large DataFrame will OOM the driver.
Always filter, limit, or aggregate before collecting.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_frame.sample_data import customer_orders, employees
from data_frame.spark_utils import get_spark


def demo_collect(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # collect() — all rows as List[Row]
    # ------------------------------------------------------------------
    rows = df.collect()  # (1)
    print(f"collect() returned {len(rows)} rows")
    for row in rows:
        print(f"  id={row['id']}  name={row['employee_name']}")

    # Access by column name or index
    first_row = rows[0]
    print(f"\nColumn by name : {first_row['employee_name']}")
    print(f"Column by index: {first_row[1]}")
    print(f"Row as dict    : {first_row.asDict()}")


def demo_first(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # first() — single Row (never returns None; raises if DataFrame is empty)
    # ------------------------------------------------------------------
    row = df.orderBy("id").first()
    print(f"\nfirst() → {row.asDict()}")

    # Safe pattern when DataFrame may be empty
    result = df.filter(F.col("id") == 999)
    row = result.first()
    if row is None:
        print("first() returned None — DataFrame is empty")


def demo_head(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees())

    # ------------------------------------------------------------------
    # head(n) — first n rows as List[Row]
    # head()  — first Row (same as first())
    # ------------------------------------------------------------------
    top3 = df.head(3)
    print(f"\nhead(3) returned {len(top3)} rows")

    single = df.head()  # (2)
    print(f"head()  returned: {single.asDict()}")


def demo_take(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # take(n) — first n rows as List[Row]; equivalent to head(n)
    # ------------------------------------------------------------------
    sample = df.take(3)
    print(f"\ntake(3) → {[r.asDict() for r in sample]}")

    # Common pattern: quick content check without full collect
    is_populated = len(df.take(1)) > 0
    print(f"DataFrame is populated: {is_populated}")


def demo_tail(spark: SparkSession) -> None:
    df = spark.createDataFrame(*employees()).orderBy("id")

    # ------------------------------------------------------------------
    # tail(n) — last n rows as List[Row] (Spark 3.0+)
    # Requires a full scan — avoid on large DataFrames
    # ------------------------------------------------------------------
    last2 = df.tail(2)
    print(f"\ntail(2) → {[r.asDict() for r in last2]}")


def demo_collect_after_agg(spark: SparkSession) -> None:
    df = spark.createDataFrame(*customer_orders())

    # ------------------------------------------------------------------
    # Best practice: aggregate first, then collect the small result
    # ------------------------------------------------------------------
    summary = (
        df.filter(F.col("status") == "active")
        .groupBy("product")
        .agg(
            F.count("*").alias("orders"),
            F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("revenue"),
        )
        .orderBy(F.desc("revenue"))
        .collect()  # (3)
    )
    print("\nAggregated then collected:")
    for row in summary:
        print(
            f"  {row['product']:10s}  orders={row['orders']}  revenue={row['revenue']}"
        )


def main(spark: SparkSession) -> None:
    demo_collect(spark)
    demo_first(spark)
    demo_head(spark)
    demo_take(spark)
    demo_tail(spark)
    demo_collect_after_agg(spark)


# (1) collect() causes a full shuffle to the driver — never call on production
#     DataFrames without a preceding filter/limit.
# (2) head() with no argument returns a single Row object, not a list.
# (3) Aggregate on executors → collect the small summary to the driver.

if __name__ == "__main__":
    spark = get_spark("action-collect")
    main(spark)
    spark.stop()
