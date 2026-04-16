# Installation

## Prerequisites

!!! warning "Java required"
    PySpark requires Java 11 or 17 on your `PATH`.

    ```bash
    java -version
    ```

## Install uv

All child projects use [uv](https://docs.astral.sh/uv/) for dependency management.

=== "macOS / Linux"
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

=== "Windows"
    ```bash
    powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

=== "pip"
    ```bash
    pip install uv
    ```

## Set Up a Child Project

Each project is independent. Navigate into the one you want and install:

```bash
cd pyspark-chispa   # or pyspark-deepu or pyspark-pytest
uv sync --group dev
```

## Verify Installation

```bash
uv run python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').config('spark.ui.enabled','false').getOrCreate()
print('Spark', spark.version, 'OK')
spark.stop()
"
```

!!! success "Expected output"
    ```
    Spark 3.5.x OK
    ```

## Environment Variables

Set these for consistent behavior across local and CI environments:

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```

## Common Tasks

All child projects use [taskipy](https://github.com/taskipy/taskipy) for task running:

| Command | Description |
| --- | --- |
| `uv run task test` | Run tests, stop on first failure |
| `uv run task test_verbose` | Verbose output with full tracebacks |
| `uv run task lint` | Lint with ruff |
| `uv run task format` | Format code with ruff |
| `uv run task typecheck` | Type check with mypy |
| `uv run task check` | Full CI pipeline (lint + format + test) |
| `uv run task docs` | Build MkDocs documentation |
| `uv run task docs_serve` | Serve docs locally |
| `uv run task clean` | Remove caches and build artifacts |
