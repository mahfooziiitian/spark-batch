---
applyTo: "src/parallel/**/*.py"
---

# Parallel Execution Instructions

## Core Rule

Spark's `SparkSession` and `SparkContext` are **thread-safe**. Share one session
across all threads — never create a new session per thread. The FAIR scheduler
distributes resources across concurrent in-flight jobs.

## Pattern 1 — `threading.Thread`

Use for two or more independent Spark actions that should overlap.
Always `join()` every thread before reading results.

```python
import threading
from threading import Lock

results: dict = {}
lock = Lock()

def run_job(label: str) -> None:
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    count = df.filter(F.col("region") == label).count()
    with lock:
        results[label] = count

threads = [threading.Thread(target=run_job, args=(r,)) for r in regions]
for t in threads:
    t.start()
for t in threads:
    t.join()
```

## Pattern 2 — `multiprocessing.pool.ThreadPool`

Use for homogeneous fan-out: the same function applied to a list of inputs.
Always use the context manager form so the pool is terminated on exception.

```python
from multiprocessing.pool import ThreadPool

def process(item: str) -> tuple:
    spark = SparkSession.builder.getOrCreate()  # returns shared session
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    return item, df.filter(F.col("name") == item).count()

with ThreadPool(len(items)) as pool:
    results = dict(pool.map(process, items))
```

**Thread safety:** never mutate a shared list or dict inside pool workers without a `Lock`.
Use `pool.map` return values instead of side-effect appends.

## Pattern 3 — `concurrent.futures.ThreadPoolExecutor`

Use when you need fine-grained control: submit heterogeneous jobs and consume
results as they complete via `as_completed`.

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

queries = {
    "count_a": lambda: df.filter(F.col("x") > 0).count(),
    "count_b": lambda: df.groupBy("y").count().count(),
}

with ThreadPoolExecutor(max_workers=len(queries)) as executor:
    futures = {executor.submit(fn): name for name, fn in queries.items()}
    results = {futures[f]: f.result() for f in as_completed(futures)}
```

Submit **all** futures before blocking on `.result()` — this is what achieves
concurrent execution.

## Pattern 4 — Python Async Futures

PySpark's Python API does not expose `countAsync`/`reduceAsync` (those are
Scala-only). Use `ThreadPoolExecutor.submit()` for the equivalent:

```python
with ThreadPoolExecutor(max_workers=3) as executor:
    future_a = executor.submit(lambda: spark.range(1_000_000).count())
    future_b = executor.submit(lambda: spark.range(2_000_000).count())
    future_c = executor.submit(lambda: df.agg(F.sum("amount")).first()[0])

# All three jobs are now in-flight simultaneously in the Spark scheduler
count_a = future_a.result()
count_b = future_b.result()
total   = future_c.result()
```

## Pattern 5 — Queue Worker Pool

Use when you have a fixed-size pool draining a variable-length work queue.
Use `queue.get(block=False)` + `except queue.Empty` — **not** `while not q.empty()`,
which is a TOCTOU race when multiple workers drain simultaneously.

```python
import queue
from threading import Thread

task_queue: queue.Queue = queue.Queue()
for item in items:
    task_queue.put(item)

def worker() -> None:
    while True:
        try:
            item = task_queue.get(block=False)
        except queue.Empty:
            break
        try:
            process(item)
        finally:
            task_queue.task_done()

threads = [Thread(target=worker, daemon=False) for _ in range(WORKER_COUNT)]
for t in threads:
    t.start()
for t in threads:
    t.join()
```

## Pattern 6 — `InheritableThread` + Job Cancellation

Use `pyspark.InheritableThread` (not `threading.Thread`) when the thread must
inherit Spark thread-local properties (e.g. `jobGroup`). This is required for
`cancelJobGroup` to work correctly.

```python
from pyspark import InheritableThread

def long_job() -> None:
    sc.setJobGroup("my_group", "description")
    result = sc.parallelize(range(10)).map(slow_fn).collect()

def cancel_after(secs: float) -> None:
    time.sleep(secs)
    sc.cancelJobGroup("my_group")

worker    = InheritableThread(target=long_job)
canceller = InheritableThread(target=cancel_after, args=(5.0,))
worker.start()
canceller.start()
worker.join()
canceller.join()
```

## Pattern 7 — Horizontal Parallelism (Per-Column)

For column-independent operations (dedup counts, null checks, stats), distribute
across a `ThreadPool`. Collect results via a `Lock`-protected shared structure.

```python
from threading import Lock

results: dict = {}
lock = Lock()

def analyse_column(col: str) -> None:
    stat = df.agg(F.countDistinct(col)).first()[0]
    with lock:
        results[col] = stat

with ThreadPool(len(df.columns)) as pool:
    pool.map(analyse_column, df.columns)
```

## FAIR Scheduler Pools

Always use pool assignment per thread to prevent one large job starving others:

```python
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
```

Define pools in `scheduling/fairscheduler.xml`:
- `production`: FAIR mode, weight=2, minShare=3 — for live workloads
- `test`: FIFO mode, weight=1, minShare=1 — for batch/test jobs

Point the session at the XML file:
```python
.config("spark.scheduler.allocation.file", "scheduling/fairscheduler.xml")
```

## Thread Safety Rules

| Shared resource | Safe approach |
| --------------- | ------------- |
| `dict` / `list` result collector | Wrap mutations in `threading.Lock()` |
| `SparkSession` / `SparkContext` | Thread-safe; no lock needed |
| Thread-local properties (`scheduler.pool`) | Use `setLocalProperty` inside each thread |
| `queue.Queue` drain | Use `get(block=False)` + `except queue.Empty` |

## Timing Comparisons

Every script that demonstrates parallel speedup must print a comparison:

```python
serial_secs   = run_serial(...)
parallel_secs = run_parallel(...)

print(f"Serial   : {serial_secs:.2f}s")
print(f"Parallel : {parallel_secs:.2f}s")
print(f"Speedup  : {serial_secs / parallel_secs:.2f}x")
```
