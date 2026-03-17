# Testing Guide

The test suite covers all seven parallel patterns with a single session-scoped
`SparkSession` fixture. All 25 tests run in under 20 seconds on a laptop.

## Running the Tests

```bash
# All tests
pytest src/parallel/tests/ -v

# Single class
pytest src/parallel/tests/test_parallel_patterns.py::TestThreading -v

# With short tracebacks
pytest src/parallel/tests/ -v --tb=short
```

## CI Environment Variables

Set these before running tests locally or in a CI pipeline:

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```

## SparkSession Fixture

A single `session`-scoped fixture is shared across the entire test run.
Starting and stopping the JVM for every test would make the suite 10–50× slower.

```python
@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .appName("test-parallel-patterns")
               .master("local[2]")                           # (1)!
               .config("spark.sql.shuffle.partitions", "2")  # (2)!
               .config("spark.scheduler.mode", "FAIR")       # (3)!
               .config("spark.ui.enabled", "false")          # (4)!
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

1. Two threads — deterministic and fast for small test datasets.
2. Default 200 is wasteful; 2 is enough for test data.
3. Required for tests that verify pool assignment and parallel correctness.
4. Skip the Spark Web UI to speed up fixture creation.

## Test Class Groups

```python
class TestSparkSession:          # version, master, scheduler mode
class TestThreading:             # Thread correctness, join, serial vs parallel
class TestThreadPoolExecutor:    # futures, as_completed, exception propagation
class TestThreadPool:            # pool.map, context manager, thread safety
class TestAsyncFutures:          # submit-before-get, concurrent execution
class TestSchedulerPools:        # setLocalProperty, pool assignment
class TestQueueWorkerPool:       # Queue drain, completion, task_done
class TestHorizontalParallelism: # per-column dedup, serial == parallel
class TestWindowFunctions:       # rank, running totals (used in parallel agg)
```

## Full Test Suite

```python title="src/parallel/tests/test_parallel_patterns.py"
--8<-- "src/parallel/tests/test_parallel_patterns.py"
```

## Testing Thread Safety

The key parallel test is verifying that results collected from multiple threads
match the serial baseline — this catches incorrect locking:

```python
def test_parallel_matches_serial(self, spark):
    df = spark.createDataFrame(SALES_DATA, SALES_SCHEMA).cache()
    total = df.count()

    serial = {col: total - df.dropDuplicates([col]).count()
              for col in df.columns}

    parallel: dict = {}
    lock = Lock()

    def dedup(col: str) -> None:
        with lock:
            parallel[col] = total - df.dropDuplicates([col]).count()

    with ThreadPool(len(df.columns)) as pool:
        pool.map(dedup, df.columns)

    assert serial == parallel    # (1)!
```

1. If locking is incorrect, workers race and overwrite each other — this assertion fails.

## Testing Parallel Speedup

Use a relative timing bound — not a fixed time limit, which would be flaky on
slow CI machines:

```python
def test_parallel_not_slower_than_serial(self, spark):
    sizes = [500_000, 600_000, 700_000]

    start = time.perf_counter()
    serial_counts = [spark.range(n).count() for n in sizes]
    serial_secs = time.perf_counter() - start

    results: dict = {}
    threads = [threading.Thread(
        target=lambda i=i, n=n: results.__setitem__(i, spark.range(n).count())
    ) for i, n in enumerate(sizes)]

    start = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join()
    parallel_secs = time.perf_counter() - start

    assert list(results[i] for i in range(len(sizes))) == serial_counts
    assert parallel_secs < serial_secs * 1.5   # (1)!
```

1. Parallel must not be significantly slower. On a loaded CI machine the
   speedup may be minimal, so `1.5×` is a safe upper bound.

## Testing Queue Workers

```python
def test_all_items_processed(self, spark):
    items = list(range(10))
    task_queue: queue.Queue = queue.Queue()
    for i in items: task_queue.put(i)

    results: list = []
    lock = Lock()

    def worker() -> None:
        while True:
            try: item = task_queue.get(block=False)
            except queue.Empty: break
            try:
                with lock: results.append(spark.range(item + 1).count())
            finally: task_queue.task_done()

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(results) == len(items)
    assert task_queue.empty()
```

## File I/O Tests

```python
def test_write_and_read_parquet(self, spark, tmp_path):
    path = str(tmp_path / "output.parquet")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    df.write.mode("overwrite").parquet(path)
    assert spark.read.parquet(path).count() == 2
```

Use `tmp_path` (pytest built-in) — each test gets a unique temp directory that
is cleaned up automatically after the session.
