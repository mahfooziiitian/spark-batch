"""
Pattern: ThreadPool — parallel JDBC table ingestion
====================================================
Demonstrates ingesting multiple database tables concurrently via a
``ThreadPool``.  When real JDBC credentials are not set, in-memory
DataFrames simulate the tables so the script runs locally.

Environment variables
---------------------
JDBC_URL   JDBC connection string.  E.g. ``jdbc:postgresql://host/db``
DB_USER    Database username.
DB_PASS    Database password.
TABLES     Comma-separated table names.  Default: orders,customers,products
OUTPUT_PATH  Parquet output root.  Default: /tmp/jdbc_ingestion
"""

import os
import time
from multiprocessing.pool import ThreadPool
from threading import current_thread

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

JDBC_URL    = os.environ.get("JDBC_URL", "")
DB_USER     = os.environ.get("DB_USER", "")
DB_PASS     = os.environ.get("DB_PASS", "")
TABLES      = [t.strip() for t in os.environ.get("TABLES", "orders,customers,products").split(",")]
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/jdbc_ingestion")

# In-memory simulation data
_SAMPLE: dict[str, list] = {
    "orders":    [(1, 101, 250.0, "2024-01-10"),
                  (2, 102, 480.0, "2024-01-11"),
                  (3, 101, 170.5, "2024-01-12"),
                  (4, 103, 920.0, "2024-01-13"),
                  (5, 102, 340.0, "2024-01-14")],
    "customers": [(101, "Alice",   "North"),
                  (102, "Bob",     "South"),
                  (103, "Charlie", "East")],
    "products":  [(201, "Widget",  9.99),
                  (202, "Gadget", 49.99),
                  (203, "Doohickey", 99.99)],
}
_SCHEMAS: dict[str, list] = {
    "orders":    ["order_id", "customer_id", "amount", "date"],
    "customers": ["customer_id", "name", "region"],
    "products":  ["product_id", "name", "price"],
}


def _read_table(spark: SparkSession, table: str) -> DataFrame:
    if JDBC_URL and DB_USER and DB_PASS:
        return (spark.read
                .format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", table)
                .option("user", DB_USER)
                .option("password", DB_PASS)
                .load())
    sample = _SAMPLE.get(table, [(0, "unknown")])
    schema = _SCHEMAS.get(table, ["id", "value"])
    return spark.createDataFrame(sample, schema)


def ingest_table(table: str, stats: list) -> None:
    t = current_thread().name
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    spark.sparkContext.setJobDescription(f"ingest:{table}")

    start = time.perf_counter()
    df = _read_table(spark, table)
    row_count = df.count()
    elapsed = time.perf_counter() - start

    out = f"{OUTPUT_PATH}/{table}"
    df.write.mode("overwrite").parquet(out)
    stats.append({"table": table, "rows": row_count, "secs": round(elapsed, 3)})
    print(f"  [{t}] {table}: {row_count:,} rows → {out} ({elapsed:.3f}s)")


def run_serial() -> tuple[list, float]:
    spark = SparkSession.builder.getOrCreate()
    stats: list = []
    start = time.perf_counter()
    for tbl in TABLES:
        ingest_table(tbl, stats)
    return stats, time.perf_counter() - start


def run_parallel() -> tuple[list, float]:
    stats: list = []
    start = time.perf_counter()
    with ThreadPool(len(TABLES)) as pool:
        pool.starmap(ingest_table, [(tbl, stats) for tbl in TABLES])
    return stats, time.perf_counter() - start


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
    from utils.spark_session import get_spark

    if not JDBC_URL:
        print("JDBC_URL not set — using in-memory simulation data\n")

    spark = get_spark("threadpool-jdbc-ingestion")
    try:
        print("── Tables to ingest ──────────────────────────────")
        for tbl in TABLES:
            print(f"  • {tbl}")

        print("\n── Serial ingestion ──────────────────────────────")
        _, serial_secs = run_serial()

        print("\n── Parallel ingestion ────────────────────────────")
        stats, parallel_secs = run_parallel()

        print("\n── Summary ───────────────────────────────────────")
        print(f"  {'Table':<20} {'Rows':>8} {'Time (s)':>10}")
        print("  " + "-" * 42)
        for s in sorted(stats, key=lambda x: x["table"]):
            print(f"  {s['table']:<20} {s['rows']:>8,} {s['secs']:>10.3f}")
        print(f"\n  Serial   : {serial_secs:.2f}s")
        print(f"  Parallel : {parallel_secs:.2f}s")
        print(f"  Speedup  : {serial_secs / parallel_secs:.2f}x")
    finally:
        spark.stop()
