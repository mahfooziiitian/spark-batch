"""
Pattern: Threading + FAIR scheduler — named-thread analytics
=============================================================
Uses plain ``threading.Thread`` with explicit ``setLocalProperty`` inside
each thread (not inherited).  Each thread sets its own pool assignment and
job description before submitting Spark work.

Demonstrates:
- Thread naming for Spark UI visibility
- Per-thread pool assignment
- Timing comparison: serial vs threaded

Environment variables
---------------------
SPARK_MASTER   Spark master URL.  Default: local[*]
NUM_THREADS    Parallel threads.  Default: 4
"""

import os
import time
import threading
from threading import current_thread, Lock

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

NUM_THREADS = int(os.environ.get("NUM_THREADS", "4"))

_SALES = [
    (i, f"region_{i % 5}", f"cat_{i % 3}", round(50 + i * 2.5, 2))
    for i in range(500)
]
_SCHEMA = ["id", "region", "category", "amount"]

_results: list[dict] = []
_lock = Lock()


def _query_job(spark: SparkSession, region: str, pool: str) -> None:
    t = current_thread().name
    sc = spark.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", pool)
    sc.setJobDescription(f"region-analytics:{region}")

    start = time.perf_counter()
    df = spark.createDataFrame(_SALES, _SCHEMA).filter(F.col("region") == region)
    agg = df.agg(
        F.count("*").alias("orders"),
        F.round(F.sum("amount"), 2).alias("revenue"),
        F.round(F.avg("amount"), 2).alias("avg_order"),
    ).first()
    elapsed = time.perf_counter() - start

    with _lock:
        _results.append({
            "thread":    t,
            "region":    region,
            "pool":      pool,
            "orders":    agg["orders"],
            "revenue":   agg["revenue"],
            "avg_order": agg["avg_order"],
            "elapsed":   round(elapsed, 3),
        })
    print(f"  [{t:>16}] {region:<12} orders={agg['orders']:>4} revenue={agg['revenue']:>8.2f} ({elapsed:.3f}s)")


def run_threaded(spark: SparkSession) -> tuple[list, float]:
    _results.clear()
    regions   = [f"region_{i}" for i in range(5)]
    pools     = ["production", "production", "production", "test", "test"]
    threads = [
        threading.Thread(
            target=_query_job,
            args=(spark, region, pool),
            name=f"analytics-thread-{i+1}",
        )
        for i, (region, pool) in enumerate(zip(regions, pools))
    ]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return list(_results), time.perf_counter() - start


def run_serial(spark: SparkSession) -> tuple[list, float]:
    _results.clear()
    regions = [f"region_{i}" for i in range(5)]
    start   = time.perf_counter()
    for region in regions:
        _query_job(spark, region, "production")
    return list(_results), time.perf_counter() - start


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
    from utils.spark_session import get_spark

    spark = get_spark("scheduling-threading-fair")
    try:
        print(f"Threads: {NUM_THREADS}\n")

        print("── Serial run ──────────────────────────────────")
        _, serial_secs = run_serial(spark)

        print("\n── Threaded run ────────────────────────────────")
        results, parallel_secs = run_threaded(spark)

        print("\n── Summary ─────────────────────────────────────")
        print(f"  {'Region':<12} {'Pool':<12} {'Orders':>6} {'Revenue':>10} {'Time':>8}")
        print("  " + "-" * 52)
        for r in sorted(results, key=lambda x: x["region"]):
            print(f"  {r['region']:<12} {r['pool']:<12} {r['orders']:>6} {r['revenue']:>10.2f} {r['elapsed']:>8.3f}s")

        print(f"\nSerial   : {serial_secs:.2f}s")
        print(f"Parallel : {parallel_secs:.2f}s")
        print(f"Speedup  : {serial_secs / parallel_secs:.2f}x")
    finally:
        spark.stop()
