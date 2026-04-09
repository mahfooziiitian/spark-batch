"""
Pattern: Futures — async job submission with concurrent.futures
===============================================================
Demonstrates how ``ThreadPoolExecutor.submit()`` + ``Future`` objects
replicate the semantics of Scala's ``RDD.countAsync()`` in Python.

Key concepts:
- ``submit()`` returns immediately — Spark job starts in background thread
- ``as_completed()`` yields results in completion order (fastest first)
- Futures support ``cancel()``, timeouts, and ``add_done_callback()``

Note: ``RDD.countAsync()`` / ``reduceAsync()`` are Scala-only APIs.
Python equivalent is ``executor.submit(lambda: rdd.count())``.

Environment variables
---------------------
SPARK_MASTER   Spark master URL.  Default: local[*]
"""

import time
from concurrent.futures import ThreadPoolExecutor, Future, as_completed

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _make_df(spark: SparkSession, region: str, size: int):
    """Create a labelled in-memory DataFrame for one region."""
    rows = [(i, region, round(100.0 + i * 0.5, 2)) for i in range(size)]
    return spark.createDataFrame(rows, ["id", "region", "amount"])


def demo_basic_submit(spark: SparkSession) -> None:
    """Submit three count jobs, collect in completion order."""
    print("\n── Basic submit + as_completed ─────────────────")

    datasets = [
        ("North",   500),
        ("South",   200),
        ("Central", 800),
    ]
    dfs = {region: _make_df(spark, region, n) for region, n in datasets}

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures: dict[Future, str] = {}
        for region, df in dfs.items():
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
            submit_at = time.perf_counter() - start
            f = ex.submit(df.count)
            futures[f] = region
            print(f"  submitted {region} at +{submit_at:.3f}s")

        for f in as_completed(futures):
            region = futures[f]
            count  = f.result()
            done_at = time.perf_counter() - start
            print(f"  {region} completed at +{done_at:.3f}s  → {count:,} rows")

    print(f"  Total: {time.perf_counter() - start:.2f}s")


def demo_complex_futures(spark: SparkSession) -> None:
    """Multiple aggregations submitted concurrently, results used for ranking."""
    print("\n── Concurrent aggregations (parallel analytics) ─")

    regions = ["North", "South", "East", "West", "Central"]
    dfs = {r: _make_df(spark, r, 100 + i * 50) for i, r in enumerate(regions)}

    def aggregate(region: str):
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
        spark.sparkContext.setJobDescription(f"agg:{region}")
        df = dfs[region]
        row = df.agg(F.count("*").alias("rows"),
                     F.round(F.sum("amount"), 2).alias("revenue"),
                     F.round(F.avg("amount"), 2).alias("avg")).first()
        return {"region": region, "rows": row["rows"],
                "revenue": row["revenue"], "avg": row["avg"]}

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(regions)) as ex:
        futures = {ex.submit(aggregate, r): r for r in regions}
        results = [f.result() for f in as_completed(futures)]

    results.sort(key=lambda x: x["revenue"], reverse=True)
    print(f"\n  {'Region':<10} {'Rows':>8} {'Revenue':>12} {'Avg Order':>12}")
    print("  " + "-" * 46)
    for r in results:
        print(f"  {r['region']:<10} {r['rows']:>8,} {r['revenue']:>12,.2f} {r['avg']:>12,.2f}")
    print(f"\n  All 5 regions in {time.perf_counter() - start:.2f}s")


def demo_callbacks(spark: SparkSession) -> None:
    """Add done callbacks to futures for fire-and-forget side-effects."""
    print("\n── Done callbacks ───────────────────────────────")

    completed_log: list[str] = []

    def on_done(future: Future, region: str) -> None:
        count = future.result()
        completed_log.append(f"{region}:{count}")

    with ThreadPoolExecutor(max_workers=3) as ex:
        for region in ["Alpha", "Beta", "Gamma"]:
            df = _make_df(spark, region, 50)
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
            f = ex.submit(df.count)
            f.add_done_callback(lambda fut, r=region: on_done(fut, r))

    print(f"  Callbacks fired: {sorted(completed_log)}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
    from utils.spark_session import get_spark

    spark = get_spark("futures-async")
    try:
        demo_basic_submit(spark)
        demo_complex_futures(spark)
        demo_callbacks(spark)
    finally:
        spark.stop()
