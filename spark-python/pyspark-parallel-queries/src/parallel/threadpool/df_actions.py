"""
Pattern: ThreadPool — parallel DataFrame aggregations
======================================================
Fan out N independent aggregation queries across a ``ThreadPool`` so all
Spark jobs run simultaneously inside a single ``SparkSession``.

This example analyses an e-commerce orders dataset across five dimensions
in parallel (region revenue, top category, daily trend, high-value orders,
return rate) and prints a formatted summary table.

Environment variables
---------------------
DATA_CSV     Path to an orders CSV.  Falls back to built-in sample.
OUTPUT_PATH  Parquet output directory.  Default: /tmp/df_actions
"""

import os
import time
from multiprocessing.pool import ThreadPool
from threading import Lock, current_thread

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/df_actions")
DATA_CSV    = os.environ.get("DATA_CSV", "")

# Realistic e-commerce sample: (order_id, region, category, amount, date, returned)
ORDERS = [
    (1001, "North",   "Electronics",  1_200.00, "2024-01-05", False),
    (1002, "South",   "Clothing",       450.50, "2024-01-05", False),
    (1003, "East",    "Electronics",    980.00, "2024-01-06", False),
    (1004, "West",    "Clothing",       320.75, "2024-01-06", True),
    (1005, "Central", "Electronics",  1_500.00, "2024-01-07", False),
    (1006, "North",   "Clothing",       210.00, "2024-01-07", False),
    (1007, "South",   "Electronics",  1_100.00, "2024-01-08", True),
    (1008, "East",    "Clothing",       870.25, "2024-01-08", False),
    (1009, "West",    "Electronics",    760.00, "2024-01-09", False),
    (1010, "Central", "Clothing",       540.00, "2024-01-09", False),
    (1011, "North",   "Home",           330.00, "2024-01-10", False),
    (1012, "South",   "Home",           290.50, "2024-01-10", True),
    (1013, "East",    "Electronics",    430.00, "2024-01-11", False),
    (1014, "West",    "Home",           680.00, "2024-01-11", False),
    (1015, "Central", "Electronics",    920.00, "2024-01-12", False),
    (1016, "North",   "Clothing",       175.00, "2024-01-12", False),
    (1017, "South",   "Electronics",  1_350.00, "2024-01-13", False),
    (1018, "East",    "Home",           410.00, "2024-01-13", True),
    (1019, "West",    "Clothing",       265.00, "2024-01-14", False),
    (1020, "Central", "Home",           785.00, "2024-01-14", False),
]
SCHEMA = ["order_id", "region", "category", "amount", "date", "returned"]


def load_df(spark: SparkSession) -> DataFrame:
    if DATA_CSV and os.path.exists(DATA_CSV):
        return spark.read.csv(DATA_CSV, header=True, inferSchema=True)
    return spark.createDataFrame(ORDERS, SCHEMA)


# ── Individual query functions ─────────────────────────────────────────────

def revenue_by_region(df: DataFrame) -> dict:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    rows = (
        df.groupBy("region")
          .agg(F.round(F.sum("amount"), 2).alias("revenue"),
               F.count("*").alias("orders"))
          .orderBy(F.desc("revenue"))
          .collect()
    )
    return {"label": "Revenue by Region", "rows": rows}


def top_category(df: DataFrame) -> dict:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    rows = (
        df.groupBy("category")
          .agg(F.round(F.sum("amount"), 2).alias("revenue"),
               F.count("*").alias("orders"),
               F.round(F.avg("amount"), 2).alias("avg_order"))
          .orderBy(F.desc("revenue"))
          .collect()
    )
    return {"label": "Revenue by Category", "rows": rows}


def daily_trend(df: DataFrame) -> dict:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    w = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    rows = (
        df.groupBy("date")
          .agg(F.round(F.sum("amount"), 2).alias("daily_revenue"))
          .withColumn("cumulative", F.round(F.sum("daily_revenue").over(w), 2))
          .orderBy("date")
          .collect()
    )
    return {"label": "Daily Revenue Trend", "rows": rows}


def high_value_orders(df: DataFrame) -> dict:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    threshold = df.agg(F.avg("amount")).first()[0]
    rows = (
        df.filter(F.col("amount") > threshold)
          .orderBy(F.desc("amount"))
          .select("order_id", "region", "category",
                  F.round("amount", 2).alias("amount"))
          .collect()
    )
    return {"label": f"High-Value Orders (> avg ${threshold:,.2f})", "rows": rows}


def return_rate(df: DataFrame) -> dict:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    rows = (
        df.groupBy("region")
          .agg(
              F.count("*").alias("total"),
              F.sum(F.col("returned").cast("int")).alias("returned"),
          )
          .withColumn("return_pct",
                      F.round(F.col("returned") / F.col("total") * 100, 1))
          .orderBy(F.desc("return_pct"))
          .collect()
    )
    return {"label": "Return Rate by Region", "rows": rows}


QUERIES = [revenue_by_region, top_category, daily_trend, high_value_orders, return_rate]


def _run(query_fn, df: DataFrame, results: list, lock: Lock) -> None:
    t = current_thread().name
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setJobDescription(query_fn.__name__)
    result = query_fn(df)
    with lock:
        results.append(result)
    print(f"  [{t}] {query_fn.__name__} → {len(result['rows'])} rows")


def run_parallel(df: DataFrame) -> tuple[list, float]:
    results: list = []
    lock = Lock()
    start = time.perf_counter()
    with ThreadPool(len(QUERIES)) as pool:
        pool.starmap(_run, [(fn, df, results, lock) for fn in QUERIES])
    return results, time.perf_counter() - start


def run_serial(df: DataFrame) -> tuple[list, float]:
    results: list = []
    lock = Lock()
    start = time.perf_counter()
    for fn in QUERIES:
        _run(fn, df, results, lock)
    return results, time.perf_counter() - start


def _print_result(result: dict) -> None:
    print(f"\n  ── {result['label']} ──")
    rows = result["rows"]
    if not rows:
        print("    (no data)")
        return
    cols = rows[0].asDict().keys()
    col_w = 16
    header = "  " + "".join(f"{c:<{col_w}}" for c in cols)
    print(header)
    print("  " + "-" * len(header.strip()))
    for row in rows:
        print("  " + "".join(f"{str(v):<{col_w}}" for v in row))


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
    from utils.spark_session import get_spark

    spark = get_spark("threadpool-df-actions")
    try:
        df = load_df(spark).cache()
        df.count()  # materialise cache

        print(f"\nDataset: {df.count()} orders\n")
        print("── Serial run ──────────────────────────────────")
        _, serial_secs = run_serial(df)

        print("\n── Parallel run ────────────────────────────────")
        results, parallel_secs = run_parallel(df)

        for r in sorted(results, key=lambda x: x["label"]):
            _print_result(r)

        print(f"\nSerial   : {serial_secs:.2f}s")
        print(f"Parallel : {parallel_secs:.2f}s")
        print(f"Speedup  : {serial_secs / parallel_secs:.2f}x")
    finally:
        spark.stop()
