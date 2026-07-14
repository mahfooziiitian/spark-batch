---
applyTo: "{**/test_*.py,**/*_test.py}"
---

# PySpark Parallel Testing Instructions

## SparkSession Fixture

One session-scoped fixture shared across all tests. FAIR mode must be on:

```python
import os
import sys
import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

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
```

- `local[2]` — two threads; deterministic and fast.
- `shuffle.partitions=2` — avoids the 200-partition default on tiny test datasets.
- `FAIR` mode — required for tests that verify parallel job behaviour.
- `ui.enabled=false` — speeds up fixture creation.

## Test Class Groups

Organise tests by pattern, one class per pattern:

```python
class TestSparkSession:       ...  # version, master, scheduler mode
class TestThreading:          ...  # threading.Thread + join correctness
class TestThreadPoolExecutor: ...  # futures, as_completed, exception propagation
class TestThreadPool:         ...  # pool.map, context manager, thread safety
class TestAsyncFutures:       ...  # submit-before-get pattern
class TestSchedulerPools:     ...  # setLocalProperty, pool assignment
class TestQueueWorkerPool:    ...  # Queue drain, completion, task_done
class TestHorizontalParallelism: ... # per-column ops, Lock-protected results
class TestWindowFunctions:    ...  # rank, running total (used in parallel agg)
```

## Parallel-Safe Assertions

Use `df.count()`, never `len(df.collect())`:
```python
assert df.count() == 8
```

For shared-state tests, verify the collected results after all threads join:
```python
results: dict = {}
lock = Lock()

def job(key: str) -> None:
    with lock:
        results[key] = spark.range(100).count()

threads = [threading.Thread(target=job, args=(k,)) for k in keys]
for t in threads: t.start()
for t in threads: t.join()

assert len(results) == len(keys)
assert all(v == 100 for v in results.values())
```

## Testing Thread Safety

Test that parallel results match serial results — this catches incorrect locking:

```python
def test_parallel_matches_serial(self, spark):
    df = spark.createDataFrame(DATA, SCHEMA).cache()
    total = df.count()

    serial = {col: total - df.dropDuplicates([col]).count() for col in df.columns}

    parallel: dict = {}
    lock = Lock()
    def dedup(col): 
        with lock: parallel[col] = total - df.dropDuplicates([col]).count()
    with ThreadPool(len(df.columns)) as pool:
        pool.map(dedup, df.columns)

    assert serial == parallel
```

## Testing Queue Workers

```python
def test_all_items_processed(self, spark):
    q: queue.Queue = queue.Queue()
    items = list(range(10))
    for i in items: q.put(i)

    processed = []
    lock = Lock()

    def worker():
        while True:
            try: item = q.get(block=False)
            except queue.Empty: break
            try:
                with lock: processed.append(spark.range(item + 1).count())
            finally: q.task_done()

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(processed) == len(items)
    assert q.empty()
```

## Testing Parallel Speedup

Use a relative timing assertion — not a fixed time bound (CI environments vary):

```python
def test_parallel_not_slower_than_serial(self, spark):
    sizes = [500_000, 600_000, 700_000]

    start = time.perf_counter()
    serial = [spark.range(n).count() for n in sizes]
    serial_secs = time.perf_counter() - start

    results = {}
    threads = [threading.Thread(
        target=lambda i=i, n=n: results.__setitem__(i, spark.range(n).count())
    ) for i, n in enumerate(sizes)]
    start = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join()
    parallel_secs = time.perf_counter() - start

    assert list(results[i] for i in range(len(sizes))) == serial
    assert parallel_secs < serial_secs * 1.5  # must not be significantly slower
```

## File I/O Tests

```python
def test_write_and_read_parquet(self, spark, tmp_path):
    path = str(tmp_path / "output.parquet")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    df.write.mode("overwrite").parquet(path)
    assert spark.read.parquet(path).count() == 2
```

## Entry Point

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```

## CI Environment Variables

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```
