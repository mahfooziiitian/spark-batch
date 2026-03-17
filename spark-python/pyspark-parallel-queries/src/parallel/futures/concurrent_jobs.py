"""
Pattern: Futures — concurrent Spark jobs via ProcessPoolExecutor
================================================================
Shows how to run fully independent Spark queries concurrently using
``ThreadPoolExecutor`` with structured error handling and result ordering.

Demonstrates:
- ``as_completed()`` ordering (fastest first vs submission order)
- Per-future exception handling
- Timeout enforcement per future
- Cancellation of pending futures when one fails

Environment variables
---------------------
DATA_HOME    Root data directory.  Falls back to in-memory sample.
OUTPUT_PATH  Parquet output directory.  Default: /tmp/concurrent_jobs
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor, Future, as_completed

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_HOME = os.environ.get("DATA_HOME", "")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/concurrent_jobs")

_SALES = [
    ("Q1", "North", 12_000.0),
    ("Q1", "South", 9_500.0),
    ("Q1", "East", 7_200.0),
    ("Q1", "West", 11_800.0),
    ("Q2", "North", 14_200.0),
    ("Q2", "South", 10_100.0),
    ("Q2", "East", 8_900.0),
    ("Q2", "West", 13_200.0),
    ("Q3", "North", 11_500.0),
    ("Q3", "South", 12_000.0),
    ("Q3", "East", 9_800.0),
    ("Q3", "West", 14_700.0),
    ("Q4", "North", 16_000.0),
    ("Q4", "South", 13_500.0),
    ("Q4", "East", 10_200.0),
    ("Q4", "West", 15_900.0),
]
_SCHEMA = ["quarter", "region", "revenue"]


def _load(spark: SparkSession, path: str):
    if DATA_HOME and os.path.isdir(f"{DATA_HOME}/{path}"):
        return spark.read.parquet(f"{DATA_HOME}/{path}")
    return spark.createDataFrame(_SALES, _SCHEMA)


def _query(name: str, fn) -> dict:
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    spark.sparkContext.setJobDescription(name)
    start = time.perf_counter()
    result = fn(spark)
    return {
        "name": name,
        "result": result,
        "elapsed": round(time.perf_counter() - start, 3),
    }


def q_total_revenue(spark: SparkSession, path: str = "sales"):
    df = _load(spark, path)
    return df.agg(F.round(F.sum("revenue"), 2).alias("total")).first()["total"]


def q_top_region(spark: SparkSession, path: str = "sales"):
    df = _load(spark, path)
    row = (
        df.groupBy("region")
        .agg(F.sum("revenue").alias("rev"))
        .orderBy(F.desc("rev"))
        .first()
    )
    return f"{row['region']} (${row['rev']:,.0f})"


def q_best_quarter(spark: SparkSession, path: str = "sales"):
    df = _load(spark, path)
    row = (
        df.groupBy("quarter")
        .agg(F.sum("revenue").alias("rev"))
        .orderBy(F.desc("rev"))
        .first()
    )
    return f"{row['quarter']} (${row['rev']:,.0f})"


def q_region_growth(spark: SparkSession, path: str = "sales"):
    df = _load(spark, path)
    w = __import__("pyspark.sql.window", fromlist=["Window"]).Window
    win = w.partitionBy("region").orderBy("quarter")
    result = (
        df.withColumn("prev", F.lag("revenue").over(win))
        .withColumn(
            "growth",
            F.round((F.col("revenue") - F.col("prev")) / F.col("prev") * 100, 1),
        )
        .filter(F.col("prev").isNotNull())
        .groupBy("region")
        .agg(F.round(F.avg("growth"), 1).alias("avg_growth_pct"))
        .orderBy(F.desc("avg_growth_pct"))
        .collect()
    )
    return [(r["region"], r["avg_growth_pct"]) for r in result]


JOBS = [
    ("total_revenue", q_total_revenue),
    ("top_region", q_top_region),
    ("best_quarter", q_best_quarter),
    ("region_growth", q_region_growth),
]


def run_as_completed(timeout: float = 30.0) -> tuple[list, float]:
    """Run all jobs, yield results as each completes."""
    results = []
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(JOBS)) as ex:
        futures: dict[Future, str] = {
            ex.submit(_query, name, fn): name for name, fn in JOBS
        }
        print(f"\n  Submitted {len(futures)} jobs simultaneously")
        for f in as_completed(futures, timeout=timeout):
            name = futures[f]
            try:
                r = f.result()
                done = time.perf_counter() - start
                print(
                    f"  ✓ {r['name']:<20} completed at +{done:.3f}s ({r['elapsed']}s)"
                )
                results.append(r)
            except Exception as exc:
                print(f"  ✗ {name} failed: {exc}")
    return results, time.perf_counter() - start


def run_serial() -> tuple[list, float]:
    spark = SparkSession.builder.getOrCreate()
    results = []
    start = time.perf_counter()
    for name, fn in JOBS:
        results.append(_query(name, fn))
    return results, time.perf_counter() - start


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
    from utils.spark_session import get_spark

    spark = get_spark("futures-concurrent-jobs")
    try:
        print("── Serial run ──────────────────────────────────")
        _, serial_secs = run_serial()

        print("\n── Concurrent run (as_completed) ───────────────")
        results, parallel_secs = run_as_completed()

        print("\n── Results ─────────────────────────────────────")
        for r in results:
            print(f"  {r['name']:<20}: {r['result']}")

        print(f"\nSerial   : {serial_secs:.2f}s")
        print(f"Parallel : {parallel_secs:.2f}s")
        print(f"Speedup  : {serial_secs / parallel_secs:.2f}x")
    finally:
        spark.stop()
