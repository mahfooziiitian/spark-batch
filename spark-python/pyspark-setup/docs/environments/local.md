# Local / venv

Run PySpark on your laptop inside an isolated Python virtual environment.
No cluster required.

## How it works

Spark spawns all executor threads inside the same JVM process as the driver.

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

!!! warning "Java required"
    Java 8, 11, or 17 must be on your `PATH`:
    ```bash
    java -version
    ```

## Setup

=== "One command"
    ```bash
    bash local/setup-venv.sh
    ```
    Creates `.venv`, installs `local/requirements.txt`, and runs a smoke test.

=== "Manual"
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate          # Windows: .venv\Scripts\activate
    pip install -r local/requirements.txt
    export PYSPARK_PYTHON=$(which python)
    export PYSPARK_DRIVER_PYTHON=$(which python)
    ```

## SparkSession

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
2. Default 200 is too high for small local datasets.
3. Skip the Spark Web UI to speed up startup.

## Key Configuration

| Config | Value | Purpose |
|--------|-------|---------|
| `spark.master` | `local[*]` | All CPU cores |
| `spark.sql.shuffle.partitions` | `4` – `8` | Right-size for local data |
| `spark.ui.enabled` | `false` | Skip Web UI |
| `spark.driver.memory` | `2g` – `4g` | Increase for large datasets |

## Run the Example

```bash
python local/local_example.py
```

## Full Example

```python title="local/local_example.py"
--8<-- "local/local_example.py"
```

!!! success "Good fit"
    - Writing and debugging new jobs
    - Unit / integration tests in CI
    - Learning PySpark without a cluster

!!! failure "Not a good fit"
    - Processing data larger than available RAM
    - Performance benchmarking against a real cluster
