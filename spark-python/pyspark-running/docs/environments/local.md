# Local Mode

Local mode runs Spark entirely on your machine — no cluster, no HDFS, no setup beyond
installing PySpark. It is the fastest way to develop and test jobs.

## How it works

Spark spawns all executor threads inside the **same JVM process** as the driver.

```
┌──────────────────────────────────┐
│  Your machine                    │
│  ┌────────────────────────────┐  │
│  │  Driver  +  Executors      │  │
│  │  (single JVM process)      │  │
│  └────────────────────────────┘  │
└──────────────────────────────────┘
```

| Master string | Behaviour |
|---------------|-----------|
| `local`       | Single thread — serial execution |
| `local[2]`    | Exactly 2 executor threads |
| `local[*]`    | One thread per CPU core (recommended) |

## Prerequisites

=== "pip"
    ```bash
    pip install pyspark
    ```

=== "uv"
    ```bash
    uv add pyspark
    ```

!!! warning "Java required"
    Java 8, 11, or 17 must be available on your `PATH`:
    ```bash
    java -version
    ```

## Run the example

```bash
python local/local_example.py
```

## SparkSession for local mode

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("my-local-job")
         .master("local[*]")                          # (1)!
         .config("spark.sql.shuffle.partitions", "4") # (2)!
         .config("spark.ui.enabled", "false")         # (3)!
         .getOrCreate())
```

1. Use all available CPU cores.
2. Default is 200 — too high for small local data.
3. Skip the Spark Web UI to speed up startup.

## Key configuration reference

| Option | Recommended value | Purpose |
|--------|-------------------|---------|
| `spark.master` | `local[*]` | All CPU cores |
| `spark.sql.shuffle.partitions` | `4` – `8` | Right-size for local data volume |
| `spark.ui.enabled` | `false` | Skip Web UI in scripts/tests |
| `spark.driver.memory` | `2g` – `4g` | Increase for large local datasets |

## Example — `local_example.py`

Demonstrates: in-memory data creation → DataFrame transforms → SQL → Parquet round-trip.

```python title="local/local_example.py"
--8<-- "local/local_example.py"
```

## When to use local mode

!!! success "Good fit"
    - Writing and debugging new jobs
    - Unit / integration tests in CI pipelines
    - Learning PySpark without a cluster

!!! failure "Not a good fit"
    - Processing large datasets (> available RAM)
    - Performance benchmarking against a real cluster
    - Jobs that read from HDFS or cloud storage
