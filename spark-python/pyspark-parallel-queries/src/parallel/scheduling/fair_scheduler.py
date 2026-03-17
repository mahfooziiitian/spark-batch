"""
Pattern: FAIR scheduler — pool-based job prioritisation
========================================================
Demonstrates Spark's FAIR scheduler using two pools defined in
``fairscheduler.xml``:
- ``production`` — FAIR mode, weight=2, minShare=3  (higher priority)
- ``test``        — FIFO mode, weight=1, minShare=1  (lower priority)

By assigning different pools, production jobs receive more executor
resources when contending with background test/analysis jobs.

Key points:
- ``SPARK_MASTER`` env var controls where jobs run
- Pool assignment is per-thread via ``setLocalProperty``
- ``InheritableThread`` is required for child threads to inherit pool

Environment variables
---------------------
SPARK_MASTER   Spark master URL.  Default: local[*]
"""

import time
import threading
from threading import current_thread

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from pyspark.util import InheritableThread
    _Thread = InheritableThread
except ImportError:
    _Thread = threading.Thread

_DATASET = [(i, float(i * 2), f"region_{i % 5}") for i in range(5_000)]
_SCHEMA  = ["id", "value", "region"]


def _agg_job(spark: SparkSession, pool: str, label: str, results: list) -> None:
    t = current_thread().name
    sc = spark.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", pool)
    sc.setJobDescription(label)
    start = time.perf_counter()
    df = spark.createDataFrame(_DATASET, _SCHEMA)
    val = df.agg(F.round(F.sum("value"), 2)).first()[0]
    elapsed = time.perf_counter() - start
    results.append({"label": label, "pool": pool, "result": val, "elapsed": round(elapsed, 3)})
    print(f"  [{t:>15}] pool={pool:<12} {label}: {val:,.0f}  ({elapsed:.3f}s)")


def demo_single_pool(spark: SparkSession) -> None:
    print("\n── Single pool (FIFO default) ────────────────────")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "")
    results = []
    start = time.perf_counter()
    threads = [
        _Thread(target=_agg_job, args=(spark, "", f"no-pool-{i}", results),
                name=f"no-pool-{i}")
        for i in range(3)
    ]
    for t in threads: t.start()
    for t in threads: t.join()
    print(f"  Done in {time.perf_counter() - start:.2f}s")


def demo_mixed_pools(spark: SparkSession) -> None:
    print("\n── Mixed pools (production vs test) ─────────────")
    results = []
    start = time.perf_counter()
    threads = [
        _Thread(target=_agg_job, args=(spark, "production", f"prod-{i}", results),
                name=f"prod-{i}")
        for i in range(3)
    ] + [
        _Thread(target=_agg_job, args=(spark, "test", f"test-{i}", results),
                name=f"test-{i}")
        for i in range(2)
    ]
    for t in threads: t.start()
    for t in threads: t.join()

    elapsed = time.perf_counter() - start
    print(f"\n  Total: {elapsed:.2f}s  ({len(threads)} concurrent jobs)")

    prod_avg  = sum(r["elapsed"] for r in results if r["pool"] == "production") / 3
    test_avg  = sum(r["elapsed"] for r in results if r["pool"] == "test") / 2
    print(f"  Avg production job: {prod_avg:.3f}s")
    print(f"  Avg test job      : {test_avg:.3f}s")


def demo_job_groups(spark: SparkSession) -> None:
    print("\n── Job groups with FAIR pools ────────────────────")
    sc = spark.sparkContext
    sc.setLocalProperty("spark.jobGroup.id", "batch-run-42")
    sc.setLocalProperty("spark.job.interruptOnCancel", "true")

    results = []
    threads = [
        _Thread(target=_agg_job, args=(spark, "production", f"grouped-{i}", results),
                name=f"grouped-{i}")
        for i in range(4)
    ]
    for t in threads: t.start()
    for t in threads: t.join()
    print(f"  Group 'batch-run-42': {len(results)} jobs completed")
    for r in sorted(results, key=lambda x: x["elapsed"]):
        print(f"    {r['label']}: {r['elapsed']}s")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
    from utils.spark_session import get_spark

    spark = get_spark("scheduling-fair")
    try:
        demo_single_pool(spark)
        demo_mixed_pools(spark)
        demo_job_groups(spark)
    finally:
        spark.stop()
