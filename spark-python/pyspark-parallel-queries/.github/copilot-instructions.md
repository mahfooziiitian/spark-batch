# GitHub Copilot Instructions — PySpark Parallel Queries

> **Global instruction file.** Topic-specific conventions live in
> `.github/instructions/` and are auto-applied based on the file you are
> editing. See the table below for the full list.

## Modular Instruction Files

| File | Scope (`applyTo`) | What It Covers |
| ---- | ------------------ | -------------- |
| [`python.instructions.md`](instructions/python.instructions.md) | `**/*.py` | PEP 8, type hints, imports, docstrings |
| [`pyspark.instructions.md`](instructions/pyspark.instructions.md) | `src/**/*.py` | SparkSession builder, AQE, FAIR scheduler, env vars, output |
| [`parallel.instructions.md`](instructions/parallel.instructions.md) | `src/parallel/**/*.py` | Threading, ThreadPool, ThreadPoolExecutor, Queue, InheritableThread, horizontal parallelism |
| [`testing.instructions.md`](instructions/testing.instructions.md) | `{**/test_*.py,**/*_test.py}` | Session fixture, test class groups, parallel-safe assertions |
| [`mkdocs.instructions.md`](instructions/mkdocs.instructions.md) | `{docs/**/*.md,mkdocs.yml}` | Material theme, parallel-pattern page template, Mermaid diagrams |
| [`docker.instructions.md`](instructions/docker.instructions.md) | `{**/Dockerfile,**/docker-compose*.yml}` | Base image, ENV vars, healthchecks, volumes |

When editing a source file under `src/parallel/`, all three of **python**,
**pyspark**, and **parallel** instructions apply simultaneously.

---

## Project Overview

This project is a **PySpark parallel execution reference** that demonstrates
every idiomatic way to run multiple Spark jobs concurrently inside a single
`SparkSession`. All patterns are self-contained, runnable with `local[*]`,
and benchmarked against a serial baseline so the speedup is visible.

| Pattern | Module | Key API |
| ------- | ------ | ------- |
| Thread-per-job | `pyspark_parallel_queries.py` | `threading.Thread` |
| Thread pool — JDBC ingestion | `pyspark_run_parallel.py` | `ThreadPool` |
| Futures | `pyspark_future.py` | `ThreadPoolExecutor.submit()` |
| DataFrame thread pool | `run_parallel_df.py` | `ThreadPool.starmap()` |
| Job cancellation | `inheritable_thread.py` | `InheritableThread` + `cancelJobGroup` |
| Concurrent futures | `concurrent/run_parallel_jobs.py` | `ThreadPoolExecutor` + `as_completed` |
| Horizontal parallelism | `multiprocess/spark-horizontal parallelism.py` | `ThreadPool` + per-column ops |
| FAIR scheduler | `scheduling/scheduling_within_application.py` | `setLocalProperty` |
| Queue worker pool | `threadings/queue/parallel_queries_using_queue.py` | `queue.Queue` + `Thread` |
| Threading + FAIR | `threadings/running_parallel_queries_threading.py` | `Thread` + scheduler pools |

---

## Project Structure

```
pyspark-parallel-queries/
├── .github/
│   ├── copilot-instructions.md          # ← you are here (global)
│   └── instructions/
│       ├── python.instructions.md
│       ├── pyspark.instructions.md
│       ├── parallel.instructions.md
│       ├── testing.instructions.md
│       ├── mkdocs.instructions.md
│       └── docker.instructions.md
├── src/
│   └── parallel/
│       ├── concurrent/                  # ThreadPoolExecutor pattern
│       ├── multiprocess/                # Horizontal parallelism (per-column)
│       ├── scheduling/                  # FAIR scheduler + fairscheduler.xml
│       ├── threadings/
│       │   ├── queue/                   # Queue-based worker pool
│       │   └── running_parallel_queries_threading.py
│       ├── tests/                       # pytest test suite
│       ├── inheritable_thread.py        # InheritableThread + job cancellation
│       ├── pyspark_future.py            # Python futures (ThreadPoolExecutor.submit)
│       ├── pyspark_parallel_queries.py  # Basic threading.Thread
│       ├── pyspark_run_parallel.py      # ThreadPool + JDBC ingestion
│       ├── pyspark_util_version.py      # Spark diagnostics
│       └── run_parallel_df.py           # ThreadPool + DataFrame actions
├── docker-compose.yaml                  # MySQL service for JDBC examples
└── spark-warehouse/
```

---

## Tech Stack

| Component | Version |
| --------- | ------- |
| PySpark | 3.5.x |
| Python | ≥ 3.11 |
| Java | 11 (LTS) |
| Testing | pytest |
| Documentation | MkDocs Material ≥ 9.5 |
| Docker | MySQL 8.0.32 (JDBC examples) |

---

## Key Conventions

- **One SparkSession, many threads.** `SparkSession`/`SparkContext` are thread-safe; never create a new session per thread.
- **Always `FAIR` scheduler** when submitting parallel jobs — otherwise later jobs queue behind the first.
- **Always `t.join()`** every thread before reading its results or exiting the process.
- **Use `Lock`** for any mutable state shared across threads (lists, dicts).
- **Use `with ThreadPool(...) as pool:`** — the context manager calls `terminate()` on exception and `join()` on exit.
- **`SPARK_MASTER` env var** with `local[*]` fallback — every script must run locally without changes.
- **Credentials via env vars** (`JDBC_URL`, `JDBC_USER`, `JDBC_PASS`) — never hard-code.
- **In-memory sample data fallback** — every script works without external files or databases.
- **Timing comparison** — every script that demonstrates parallelism should print serial and parallel wall-clock times.

---

## SparkSession Pattern (parallel-optimised)

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-job-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.scheduler.mode", "FAIR")           # required for parallel jobs
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

Per-thread pool assignment (set inside each thread, not at session level):
```python
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
```

---

## Common Commands

```bash
# Run the test suite
pytest src/parallel/tests/ -v

# Run a specific pattern
SPARK_MASTER=local[*] python src/parallel/pyspark_parallel_queries.py
SPARK_MASTER=local[*] python src/parallel/concurrent/run_parallel_jobs.py

# Start MySQL for JDBC examples
docker compose up -d

# Preview docs
mkdocs serve

# Build docs (strict)
mkdocs build --strict
```

---

## Things to Avoid

- **Do not** create a new `SparkSession` inside each thread — use `SparkSession.builder.getOrCreate()` to retrieve the shared session.
- **Do not** use bare `while not q.empty(): q.get()` in queue workers — this is a TOCTOU race; use `q.get(block=False)` + `except queue.Empty` instead.
- **Do not** use `threading.Thread` instead of `pyspark.InheritableThread` when the thread needs to cancel Spark job groups — `InheritableThread` propagates Spark thread-local properties.
- **Do not** omit `t.join()` — the main process may exit before threads complete.
- **Do not** mutate a shared list or dict from multiple threads without a `threading.Lock`.
- **Do not** hard-code `JAVA_HOME`, Python paths, or Windows-style paths (`C:\\`, `E:\\`).
- **Do not** hard-code JDBC credentials — use `JDBC_URL`, `JDBC_USER`, `JDBC_PASS` env vars.
- **Do not** use `from pyspark.sql.functions import *` — always `import functions as F`.
