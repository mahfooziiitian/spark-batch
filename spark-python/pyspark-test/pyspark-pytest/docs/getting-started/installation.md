# Installation

## Prerequisites

!!! warning "Java required"
    PySpark requires Java 11 or 17 on your `PATH`.

    ```bash
    java -version
    ```

## Install Dependencies

=== "uv (Recommended)"
    ```bash
    uv sync --group dev
    ```

=== "pip"
    ```bash
    pip install pyspark faker pandas pyarrow pytest
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

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```

## Available Tasks

| Command | Description |
| --- | --- |
| `uv run task test` | Run tests, stop on first failure |
| `uv run task test_verbose` | Verbose output with full tracebacks |
| `uv run task lint` | Lint with ruff |
| `uv run task format` | Format code with ruff |
| `uv run task typecheck` | Type check with mypy |
| `uv run task check` | Full CI pipeline |
| `uv run task docs` | Build documentation |
| `uv run task docs_serve` | Serve docs locally |
| `uv run task clean` | Remove caches and artifacts |
