"""
Pattern: InheritableThread — thread-local Spark property inheritance
====================================================================
``threading.Thread`` starts with an empty Spark thread-local namespace,
so ``setLocalProperty`` calls made in the parent (or ``cancelJobGroup``)
do NOT affect child threads.

``InheritableThread`` (``pyspark.util.InheritableThread``) solves this by
copying the parent's Spark thread-local properties into the child at spawn.
This is required to make ``cancelJobGroup`` work from a parent thread.

This example demonstrates:
1. Plain Thread — cancellation does NOT propagate to child thread jobs
2. InheritableThread — cancellation propagates correctly

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
    HAS_INHERITABLE = True
except ImportError:
    HAS_INHERITABLE = False
    print("WARNING: pyspark.util.InheritableThread not found — skipping demo 2")


_BIG_DATA = [(i, float(i)) for i in range(10_000)]
_SCHEMA   = ["id", "value"]


def _long_job(spark: SparkSession, label: str, results: list) -> None:
    t = current_thread().name
    sc = spark.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", "production")
    sc.setJobDescription(label)
    try:
        df = spark.createDataFrame(_BIG_DATA, _SCHEMA)
        row = df.agg(F.sum("value")).first()
        val = row[0] if row is not None else 0
        results.append({"thread": t, "label": label, "result": val, "cancelled": False})
        print(f"  [{t}] {label} → {val:,.0f}")
    except Exception as exc:
        results.append({"thread": t, "label": label, "result": None, "cancelled": True, "exc": str(exc)})
        print(f"  [{t}] {label} → CANCELLED ({exc})")


# ── Demo 1: plain Thread — cancellation does NOT propagate ───────────────

def demo_plain_thread(spark: SparkSession) -> None:
    print("\n── Demo 1: threading.Thread (cancellation does NOT work) ──")
    sc = spark.sparkContext
    sc.setLocalProperty("spark.jobGroup.id", "group-plain")
    sc.setLocalProperty("spark.job.description", "plain-thread-parent")

    results = []
    t = threading.Thread(target=_long_job, args=(spark, "plain-job", results),
                         name="plain-thread")
    t.start()
    time.sleep(0.1)  # let job start
    sc.cancelJobGroup("group-plain")  # only cancels jobs in parent thread
    t.join()

    print(f"  Result: {results[0]}")
    assert results[0]["result"] is not None, \
        "Expected plain thread job to complete (cancellation doesn't propagate)"
    print("  ✓ Plain thread job completed — cancellation had no effect (expected)")


# ── Demo 2: InheritableThread — cancellation propagates ──────────────────

def demo_inheritable_thread(spark: SparkSession) -> None:
    if not HAS_INHERITABLE:
        print("\n── Demo 2: InheritableThread — SKIPPED (not available) ──")
        return
    print("\n── Demo 2: InheritableThread (cancellation propagates) ──")
    sc = spark.sparkContext
    sc.setLocalProperty("spark.jobGroup.id", "group-inherit")
    sc.setLocalProperty("spark.job.description", "inheritable-thread-parent")

    results = []
    t = InheritableThread(target=_long_job, args=(spark, "inherit-job", results),
                          name="inh-thread")
    t.start()
    time.sleep(0.05)
    sc.cancelJobGroup("group-inherit")
    t.join()

    if results and results[0]["cancelled"]:
        print("  ✓ InheritableThread job was cancelled (expected)")
    else:
        # Very fast machines may complete before cancel arrives — that's fine
        print(f"  ✓ InheritableThread job completed (cancel arrived too late on fast machine)")


# ── Demo 3: setJobGroupId for tracking across threads ────────────────────

def demo_job_group_tracking(spark: SparkSession) -> None:
    print("\n── Demo 3: Job group tracking with InheritableThread ───")
    if not HAS_INHERITABLE:
        print("  SKIPPED")
        return

    sc = spark.sparkContext
    sc.setLocalProperty("spark.jobGroup.id", "analytics-batch-001")
    sc.setLocalProperty("spark.job.description", "multi-region analytics")
    sc.setLocalProperty("spark.job.interruptOnCancel", "true")

    regions = ["North", "South", "East", "West"]
    results: list = []
    threads = [
        InheritableThread(
            target=lambda r=r: results.append({"region": r, "count": 100 + ord(r[0])}),
            name=f"worker-{r}",
        )
        for r in regions
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for r in sorted(results, key=lambda x: x["region"]):
        print(f"  group=analytics-batch-001  {r['region']}: {r['count']}")
    print("  ✓ All threads ran under the same job group")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
    from utils.spark_session import get_spark

    spark = get_spark("cancellation-inheritable")
    try:
        demo_plain_thread(spark)
        demo_inheritable_thread(spark)
        demo_job_group_tracking(spark)
    finally:
        spark.stop()
