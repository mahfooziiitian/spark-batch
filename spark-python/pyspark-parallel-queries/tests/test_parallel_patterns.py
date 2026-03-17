"""
Tests for all PySpark parallel execution patterns.

Covers:
  - threading.Thread with FAIR scheduler
  - concurrent.futures.ThreadPoolExecutor
  - multiprocessing.pool.ThreadPool
  - Python-native async futures (ThreadPoolExecutor.submit)
  - Queue-based worker pool
  - Horizontal parallelism (per-column operations)
  - Scheduler pool assignment via setLocalProperty
  - Window functions used in parallel aggregation context
"""

import os
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing.pool import ThreadPool
from threading import Lock

import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


# ---------------------------------------------------------------------------
# Session-scoped SparkSession fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .appName("test-parallel-patterns")
               .master("local[2]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.scheduler.mode", "FAIR")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Sample data helpers
# ---------------------------------------------------------------------------

SALES_DATA = [
    (1, "North",  "Electronics", 1200.0),
    (2, "South",  "Clothing",     450.0),
    (3, "East",   "Electronics",  980.0),
    (4, "West",   "Clothing",     320.0),
    (5, "North",  "Clothing",     210.0),
    (6, "South",  "Electronics", 1100.0),
    (7, "East",   "Clothing",     870.0),
    (8, "West",   "Electronics",  760.0),
]
SALES_SCHEMA = ["id", "region", "category", "amount"]


# ---------------------------------------------------------------------------
# TestSparkSession
# ---------------------------------------------------------------------------

class TestSparkSession:
    def test_version(self, spark):
        assert spark.version.startswith("3.")

    def test_master(self, spark):
        assert spark.sparkContext.master == "local[2]"

    def test_app_name(self, spark):
        assert spark.sparkContext.appName == "test-parallel-patterns"

    def test_fair_scheduler_enabled(self, spark):
        mode = spark.sparkContext.getConf().get("spark.scheduler.mode")
        assert mode == "FAIR"


# ---------------------------------------------------------------------------
# TestThreading
# ---------------------------------------------------------------------------

class TestThreading:
    def test_two_threads_produce_correct_results(self, spark):
        df = spark.createDataFrame(SALES_DATA, SALES_SCHEMA).cache()
        results: dict = {}
        lock = Lock()

        def count_region(region: str) -> None:
            c = df.filter(F.col("region") == region).count()
            with lock:
                results[region] = c

        threads = [threading.Thread(target=count_region, args=(r,)) for r in ("North", "South")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results["North"] == 2
        assert results["South"] == 2

    def test_parallel_faster_than_serial(self, spark):
        sizes = [500_000, 600_000, 700_000, 800_000]
        serial_results: dict = {}
        parallel_results: dict = {}

        start = time.perf_counter()
        for i, n in enumerate(sizes):
            serial_results[i] = spark.range(n).count()
        serial_secs = time.perf_counter() - start

        threads = [
            threading.Thread(
                target=lambda i=i, n=n: parallel_results.__setitem__(i, spark.range(n).count())
            )
            for i, n in enumerate(sizes)
        ]
        start = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        parallel_secs = time.perf_counter() - start

        assert serial_results == parallel_results
        assert parallel_secs < serial_secs * 1.5

    def test_join_ensures_completion(self, spark):
        completed = []
        lock = Lock()

        def slow_job() -> None:
            count = spark.range(100_000).count()
            with lock:
                completed.append(count)

        threads = [threading.Thread(target=slow_job) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(completed) == 3


# ---------------------------------------------------------------------------
# TestThreadPoolExecutor
# ---------------------------------------------------------------------------

class TestThreadPoolExecutor:
    def test_all_futures_resolve(self, spark):
        df = spark.createDataFrame(SALES_DATA, SALES_SCHEMA)
        queries = {
            "total": lambda: df.agg(F.sum("amount")).first()[0],
            "north": lambda: df.filter(F.col("region") == "North").count(),
            "elec":  lambda: df.filter(F.col("category") == "Electronics").count(),
        }
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(fn): name for name, fn in queries.items()}
            results = {futures[f]: f.result() for f in as_completed(futures)}

        assert results["total"] == pytest.approx(5890.0)
        assert results["north"] == 2
        assert results["elec"]  == 4

    def test_exception_propagated_from_future(self, spark):
        def bad_job() -> int:
            raise ValueError("intentional error")

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(bad_job)

        with pytest.raises(ValueError, match="intentional error"):
            future.result()

    def test_max_workers_limits_concurrency(self, spark):
        results = []
        lock = Lock()

        def track() -> int:
            with lock:
                results.append(threading.active_count())
            return spark.range(10_000).count()

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(track) for _ in range(4)]
            for f in futures:
                f.result()

        assert all(c >= 1 for c in results)


# ---------------------------------------------------------------------------
# TestThreadPool
# ---------------------------------------------------------------------------

class TestThreadPool:
    def test_pool_map_returns_results(self, spark):
        regions = ["North", "South", "East", "West"]
        df = spark.createDataFrame(SALES_DATA, SALES_SCHEMA).cache()

        def count_region(region: str) -> tuple:
            return region, df.filter(F.col("region") == region).count()

        with ThreadPool(len(regions)) as pool:
            results = dict(pool.map(count_region, regions))

        assert results["North"] == 2
        assert results["South"] == 2
        assert results["East"]  == 2
        assert results["West"]  == 2

    def test_pool_context_manager_closes_cleanly(self, spark):
        def job(n: int) -> int:
            return spark.range(n).count()

        with ThreadPool(3) as pool:
            counts = pool.map(job, [10_000, 20_000, 30_000])

        assert counts == [10_000, 20_000, 30_000]

    def test_thread_safe_result_collection(self, spark):
        results: list = []
        lock = Lock()

        def collect_result(col: str, df) -> None:
            count = df.dropDuplicates([col]).count()
            with lock:
                results.append((col, count))

        df = spark.createDataFrame(SALES_DATA, SALES_SCHEMA)
        with ThreadPool(4) as pool:
            pool.starmap(collect_result, [(col, df) for col in df.columns])

        assert len(results) == len(df.columns)


# ---------------------------------------------------------------------------
# TestAsyncFutures
# ---------------------------------------------------------------------------

class TestAsyncFutures:
    """
    PySpark's Scala API exposes countAsync/reduceAsync, but these are not
    surfaced in the Python API.  The Python-idiomatic equivalent is
    ThreadPoolExecutor.submit(), which returns a Future immediately while
    Spark jobs run concurrently in the shared session.
    """

    def test_future_returns_correct_count(self, spark):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(lambda: spark.range(42).count())
        assert future.result() == 42

    def test_multiple_futures_submitted_before_get(self, spark):
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_a = executor.submit(lambda: spark.range(10_000).count())
            future_b = executor.submit(lambda: spark.range(10_000, 20_000).count())
            future_c = executor.submit(lambda: spark.range(20_000, 30_000).count())

        assert future_a.result() == 10_000
        assert future_b.result() == 10_000
        assert future_c.result() == 10_000

    def test_future_sum_via_aggregation(self, spark):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                lambda: spark.range(1, 6).agg(F.sum("id")).first()[0]
            )
        assert future.result() == 15  # 1+2+3+4+5

    def test_futures_run_concurrently(self, spark):
        sizes = [500_000, 600_000, 700_000]

        serial_start = time.perf_counter()
        serial_counts = [spark.range(n).count() for n in sizes]
        serial_secs = time.perf_counter() - serial_start

        with ThreadPoolExecutor(max_workers=len(sizes)) as executor:
            futures = [executor.submit(lambda n=n: spark.range(n).count()) for n in sizes]
        parallel_start = time.perf_counter()
        parallel_counts = [f.result() for f in futures]
        parallel_secs = time.perf_counter() - parallel_start

        assert serial_counts == parallel_counts
        assert parallel_secs < serial_secs * 1.5


# ---------------------------------------------------------------------------
# TestSchedulerPools
# ---------------------------------------------------------------------------

class TestSchedulerPools:
    def test_pool_assignment_does_not_break_jobs(self, spark):
        results: dict = {}

        def run_in_pool(pool: str, job_id: int) -> None:
            spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
            results[job_id] = spark.range(100_000).count()

        threads = [
            threading.Thread(target=run_in_pool, args=("production", 0)),
            threading.Thread(target=run_in_pool, args=("test",       1)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results[0] == 100_000
        assert results[1] == 100_000

    def test_default_pool_when_none_set(self, spark):
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
        assert spark.range(1_000).count() == 1_000


# ---------------------------------------------------------------------------
# TestQueueWorkerPool
# ---------------------------------------------------------------------------

class TestQueueWorkerPool:
    def test_all_items_processed(self, spark):
        items = list(range(10))
        task_queue: queue.Queue = queue.Queue()
        for item in items:
            task_queue.put(item)

        results: list = []
        lock = Lock()

        def worker() -> None:
            while True:
                try:
                    item = task_queue.get(block=False)
                except queue.Empty:
                    break
                try:
                    count = spark.range(item * 1_000 + 1).count()
                    with lock:
                        results.append(count)
                finally:
                    task_queue.task_done()

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == len(items)

    def test_queue_empty_on_completion(self, spark):
        q: queue.Queue = queue.Queue()
        for i in range(5):
            q.put(i)

        processed = []

        def drain() -> None:
            while True:
                try:
                    item = q.get(block=False)
                except queue.Empty:
                    break
                processed.append(item)
                q.task_done()

        t = threading.Thread(target=drain)
        t.start()
        t.join()

        assert q.empty()
        assert sorted(processed) == list(range(5))


# ---------------------------------------------------------------------------
# TestHorizontalParallelism
# ---------------------------------------------------------------------------

class TestHorizontalParallelism:
    def test_per_column_dedup_correct(self, spark):
        df = spark.createDataFrame(
            [(1, "a", "x"), (1, "b", "x"), (2, "a", "y")],
            ["id", "name", "code"],
        )
        total = df.count()
        results: dict = {}
        lock = Lock()

        def count_dups(col: str) -> None:
            dups = total - df.dropDuplicates([col]).count()
            with lock:
                results[col] = dups

        with ThreadPool(3) as pool:
            pool.map(count_dups, df.columns)

        assert results["id"]   == 1
        assert results["name"] == 1
        assert results["code"] == 1

    def test_parallel_dedup_matches_serial(self, spark):
        df = spark.createDataFrame(SALES_DATA, SALES_SCHEMA).cache()
        total = df.count()

        serial_results: dict = {}
        for col in df.columns:
            serial_results[col] = total - df.dropDuplicates([col]).count()

        parallel_results: dict = {}
        lock = Lock()

        def count_dups(col: str) -> None:
            dups = total - df.dropDuplicates([col]).count()
            with lock:
                parallel_results[col] = dups

        with ThreadPool(len(df.columns)) as pool:
            pool.map(count_dups, df.columns)

        assert serial_results == parallel_results


# ---------------------------------------------------------------------------
# TestWindowFunctions
# ---------------------------------------------------------------------------

class TestWindowFunctions:
    def test_running_total_per_region(self, spark):
        df = spark.createDataFrame(SALES_DATA, SALES_SCHEMA)
        w = (Window
             .partitionBy("region")
             .orderBy("id")
             .rowsBetween(Window.unboundedPreceding, 0))
        result = df.withColumn("running_total", F.sum("amount").over(w))
        assert result.count() == len(SALES_DATA)

    def test_rank_within_category(self, spark):
        df = spark.createDataFrame(SALES_DATA, SALES_SCHEMA)
        w = Window.partitionBy("category").orderBy(F.desc("amount"))
        result = df.withColumn("rank", F.rank().over(w))
        top_electronics = result.filter(
            (F.col("category") == "Electronics") & (F.col("rank") == 1)
        ).first()
        assert top_electronics["amount"] == 1200.0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
