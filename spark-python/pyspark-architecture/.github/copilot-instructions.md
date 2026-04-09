# GitHub Copilot Instructions — PySpark Architecture

> **Global instruction file.** Topic-specific conventions live in
> `.github/instructions/` and are auto-applied by file pattern. See the table
> below for the full list.

## Modular Instruction Files

| File | Scope (`applyTo`) | What It Covers |
| ---- | ----------------- | -------------- |
| [`mkdocs.instructions.md`](instructions/mkdocs.instructions.md) | `docs/**/*.md`, `mkdocs.yml` | Architecture diagrams, page structure, admonitions, Mermaid diagrams |
| [`pyspark-architecture.instructions.md`](instructions/pyspark-architecture.instructions.md) | `src/**/*.py` | Driver/Executor patterns, SparkContext lifecycle, cluster-manager configs |
| [`testing.instructions.md`](instructions/testing.instructions.md) | `tests/**/*.py` | SparkSession/SparkContext fixtures, cluster-manager mocking, assertions |

---

## Project Overview

This module is a **PySpark architecture reference** that demonstrates the internal
components of Apache Spark:

| Component | Responsibility |
| --------- | -------------- |
| **SparkSession** | Unified entry point — wraps SparkContext, SQL, and streaming contexts |
| **SparkContext** | Connects the Driver to the cluster; manages RDD creation and task scheduling |
| **Driver** | Hosts the `main()` function; builds the DAG; coordinates execution |
| **Executor** | JVM process on each worker node; runs tasks and stores partition data |
| **Cluster Manager** | Resource broker — Local, Standalone, YARN, Kubernetes, or Mesos |

---

## Project Structure

```
pyspark-architecture/
├── .github/
│   ├── copilot-instructions.md             # ← you are here (global)
│   └── instructions/
│       ├── mkdocs.instructions.md          # documentation conventions
│       ├── pyspark-architecture.instructions.md  # source-code conventions
│       └── testing.instructions.md         # test conventions
├── src/
│   ├── src/architecture/spark_session.py    # SparkSession creation & configuration patterns
│   ├── src/architecture/spark_driver.py     # Driver-side logic — DAG, plan, actions
│   └── src/architecture/spark_executor.py   # Executor-side patterns — tasks, partitions, cache
├── tests/
│   ├── tests/test_sparksession.py          # SparkSession lifecycle tests
│   ├── tests/test_spark_context.py         # SparkContext & RDD tests
│   └── tests/test_spark_cluster_manager.py # YARN & Kubernetes cluster-manager tests
├── docs/                   # MkDocs documentation source
├── mkdocs.yml              # MkDocs Material config
├── pyproject.toml
└── README.md
```

---

## Tech Stack

| Component | Version |
| --------- | ------- |
| PySpark | 3.3.x (upgrade to 3.5.x before adding new features) |
| Python | ≥ 3.11 |
| Java | 11 (LTS) |
| Testing | pytest |
| Documentation | MkDocs Material ≥ 9.5 |
| Package management | pip / uv |

---

## Key Conventions

- **SparkSession** is always the entry point — never instantiate `SparkContext`
  directly in new production code (use `spark.sparkContext` to access it).
- Use `SparkContext.getOrCreate()` when showing singleton behaviour to prevent
  multiple active contexts in the same JVM.
- Log level: `"WARN"` in example scripts, `"ERROR"` in pytest fixtures.
- Keep `SPARK_MASTER` env-var driven with `"local[*]"` fallback for portability.
- Tests for YARN / Kubernetes cluster managers must be clearly marked
  `@pytest.mark.skip` (or use mocks) so they don't run in CI without a real cluster.
- All source files live under `src/`; all tests under `tests/`.
- Never call `spark.stop()` inside a shared session-scoped pytest fixture — use
  `yield` + `stop()` in the fixture teardown only.

---

## SparkSession Pattern

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("pyspark-architecture-demo")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

---

## Common Commands

```bash
# Run all tests
pytest tests/ -v

# Run a specific source file
SPARK_MASTER=local[*] python src/spark_session.py

# Preview docs locally
mkdocs serve

# Build docs (strict mode — used in CI)
mkdocs build --strict
```

---

## Things to Avoid

- **Do not** create a new `SparkContext` directly when a `SparkSession` already
  exists — use `SparkSession.builder.getOrCreate()` instead.
- **Do not** use `scope="function"` for the Spark fixture in tests — JVM startup
  cost makes per-test session creation prohibitively slow.
- **Do not** hard-code cluster master URLs (e.g. `yarn`, `k8s://...`) in shared
  source files — drive them from environment variables.
- **Do not** use `from pyspark.sql.functions import *` — always `import functions as F`.
- **Do not** leave `spark.ui.enabled` as `true` in tests — it slows fixture setup
  and opens ports unnecessarily.
