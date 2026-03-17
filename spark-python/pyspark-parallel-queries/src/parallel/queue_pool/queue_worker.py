"""
Pattern: Queue worker pool — controlled concurrency
====================================================
A fixed-size pool of ``threading.Thread`` workers drain a shared
``queue.Queue``.  Unlike ``ThreadPool``, this lets you:
- Control concurrency precisely (N workers regardless of queue depth)
- Track which worker processed which item
- Implement back-pressure and graceful shutdown
- Add a poison-pill ``None`` sentinel to stop workers

Queue safety: workers use ``q.get(block=False)`` + ``except queue.Empty``
to avoid the TOCTOU race of ``while not q.empty(): q.get()``.

Environment variables
---------------------
SPARK_MASTER   Spark master URL.  Default: local[*]
NUM_WORKERS    Worker thread count.  Default: 4
"""

import os
import queue
import time
import threading
from threading import Lock, current_thread

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "4"))

# Queries to process: (name, SQL-like description)
WORK_ITEMS = [
    ("orders_north",   "SELECT region='North'"),
    ("orders_south",   "SELECT region='South'"),
    ("orders_east",    "SELECT region='East'"),
    ("orders_west",    "SELECT region='West'"),
    ("orders_central", "SELECT region='Central'"),
    ("high_value",     "SELECT amount > 700"),
    ("category_agg",   "GROUP BY category"),
    ("date_range",     "SELECT date BETWEEN Jan-05 and Jan-08"),
]

_ORDERS = [
    (1001, "North",   "Electronics", 1_200.0, "2024-01-05"),
    (1002, "South",   "Clothing",      450.5, "2024-01-05"),
    (1003, "East",    "Electronics",   980.0, "2024-01-06"),
    (1004, "West",    "Clothing",      320.8, "2024-01-06"),
    (1005, "Central", "Electronics", 1_500.0, "2024-01-07"),
    (1006, "North",   "Clothing",      210.0, "2024-01-07"),
    (1007, "South",   "Electronics", 1_100.0, "2024-01-08"),
    (1008, "East",    "Clothing",      870.3, "2024-01-08"),
    (1009, "West",    "Electronics",   760.0, "2024-01-09"),
    (1010, "Central", "Clothing",      540.0, "2024-01-09"),
]
_SCHEMA = ["order_id", "region", "category", "amount", "date"]


def _execute_item(name: str, description: str, spark: SparkSession) -> dict:
    df = spark.createDataFrame(_ORDERS, _SCHEMA)
    # Simulate the logical work implied by the description
    if "region=" in description:
        region = description.split("=")[1].strip("'")
        result_df = df.filter(F.col("region") == region)
    elif "amount >" in description:
        threshold = float(description.split(">")[1].strip())
        result_df = df.filter(F.col("amount") > threshold)
    elif "GROUP BY" in description:
        col = description.split("GROUP BY ")[1].strip()
        result_df = df.groupBy(col).agg(F.count("*").alias("cnt"))
    else:
        result_df = df
    return {"name": name, "rows": result_df.count()}


def worker(q: queue.Queue, results: list, lock: Lock, progress: list) -> None:
    spark = SparkSession.builder.getOrCreate()
    t = current_thread().name
    processed = 0
    while True:
        try:
            item = q.get(block=False)
        except queue.Empty:
            break
        if item is None:  # poison pill
            q.task_done()
            break
        name, description = item
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
        spark.sparkContext.setJobDescription(name)
        start = time.perf_counter()
        r = _execute_item(name, description, spark)
        elapsed = time.perf_counter() - start
        r["worker"] = t
        r["elapsed"] = round(elapsed, 3)
        with lock:
            results.append(r)
            progress.append(1)
            done = len(progress)
            total = q.maxsize + done
            print(f"  [{t}] {name}: {r['rows']} rows ({elapsed:.3f}s)  [{done}/{total}]")
        processed += 1
        q.task_done()
    print(f"  [{t}] exiting after {processed} items")


def run_queue_pool(items: list[tuple[str, str]]) -> tuple[list, float]:
    q: queue.Queue = queue.Queue(maxsize=len(items))
    for item in items:
        q.put(item)

    results: list = []
    progress: list = []
    lock = Lock()

    start = time.perf_counter()
    threads = [
        threading.Thread(target=worker, args=(q, results, lock, progress),
                         name=f"worker-{i+1}", daemon=True)
        for i in range(NUM_WORKERS)
    ]
    for t in threads:
        t.start()
    q.join()
    for t in threads:
        t.join()
    return results, time.perf_counter() - start


def run_serial(items: list[tuple[str, str]]) -> tuple[list, float]:
    spark = SparkSession.builder.getOrCreate()
    results = []
    start = time.perf_counter()
    for name, description in items:
        r = _execute_item(name, description, spark)
        r["worker"] = "main"
        r["elapsed"] = 0.0
        results.append(r)
    return results, time.perf_counter() - start


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))
    from utils.spark_session import get_spark

    spark = get_spark("queue-worker-pool")
    try:
        print(f"Workers: {NUM_WORKERS}, Items: {len(WORK_ITEMS)}\n")

        print("── Serial run ──────────────────────────────────")
        _, serial_secs = run_serial(WORK_ITEMS)

        print("\n── Queue pool run ───────────────────────────────")
        results, parallel_secs = run_queue_pool(WORK_ITEMS)

        print("\n── Worker assignment ────────────────────────────")
        worker_map: dict[str, list] = {}
        for r in results:
            worker_map.setdefault(r["worker"], []).append(r["name"])
        for w, items_done in sorted(worker_map.items()):
            print(f"  {w}: {', '.join(items_done)}")

        print(f"\nSerial   : {serial_secs:.2f}s")
        print(f"Parallel : {parallel_secs:.2f}s")
        print(f"Speedup  : {serial_secs / parallel_secs:.2f}x")
    finally:
        spark.stop()
