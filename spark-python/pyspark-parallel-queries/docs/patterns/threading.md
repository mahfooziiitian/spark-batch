# threading.Thread

The simplest parallel pattern: wrap each Spark action in a `threading.Thread`,
start both threads, then `join()` to wait for completion. The FAIR scheduler
interleaves tasks from all in-flight jobs across executor cores.

## How It Works

```mermaid
graph TD
    subgraph Driver["Driver"]
        MT[Main Thread]
        T1[Thread 1\nword_count]
        T2[Thread 2\nchar_count]
        SS[SparkSession\nFAIR mode]
    end

    subgraph Executors
        E1[Executor 1]
        E2[Executor 2]
    end

    MT -->|"Thread.start()"| T1
    MT -->|"Thread.start()"| T2
    T1 -->|"submit Job A"| SS
    T2 -->|"submit Job B"| SS
    SS -->|tasks| E1
    SS -->|tasks| E2
    T1 -->|"Thread.join()"| MT
    T2 -->|"Thread.join()"| MT
```

Both threads share the same `SparkSession` — no synchronisation is needed for
the Spark calls themselves. A `threading.Lock` is only required if the threads
write to a shared Python data structure.

## When to Use

!!! success "Good fit"
    - Two or three fixed, named parallel actions
    - One cached DataFrame feeding multiple independent aggregations
    - Word count + char count; regional rollups submitted simultaneously

!!! failure "Not suitable"
    - More than ~8 jobs (use [ThreadPool](threadpool.md) or [Futures](futures.md) instead)
    - Jobs whose inputs depend on each other's outputs

## Code

```python title="src/parallel/thread_jobs/word_char_count.py"
--8<-- "src/parallel/thread_jobs/word_char_count.py"
```

## Run

```bash
SPARK_MASTER=local[*] python src/parallel/thread_jobs/word_char_count.py
```

Expected output (times will vary):

```
Serial   : 3.42s
Parallel : 1.87s
Speedup  : 1.83x
```

## Key Rules

- Always call `t.join()` on every thread before reading results or exiting.
- Assign each thread to a FAIR pool with `spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")` inside the target function.
- For shared Python state (dicts, lists), protect writes with a `threading.Lock`.

## Configuration Reference

| Config key | Value | Description |
| ---------- | ----- | ----------- |
| `spark.scheduler.mode` | `FAIR` | Required — otherwise threads queue behind each other |
| `spark.scheduler.pool` | `production` | Per-thread pool name (set inside each thread) |
| `SPARK_MASTER` env var | `local[*]` | Use all CPU cores for local testing |
