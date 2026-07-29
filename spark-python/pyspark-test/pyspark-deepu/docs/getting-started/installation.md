# Installation

## Prerequisites

!!! warning "Java 11+ required"
    PySpark requires Java 11 or 17 on your `PATH` with `JAVA_HOME` set.

    ```bash
    java -version
    # Expected: openjdk version "11.x.x" or "17.x.x"
    ```

!!! info "Internet access required (first run)"
    PyDeequ downloads the Deequ JAR from Maven Central on first use.
    Subsequent runs use the cached artifact in `~/.ivy2/`.

## Install Dependencies

=== "uv (Recommended)"
    ```bash
    uv sync --group dev
    ```

=== "pip"
    ```bash
    pip install pyspark>=3.5 pydeequ>=1.0.1 pytest>=8.0
    ```

=== "conda"
    ```bash
    conda install -c conda-forge pyspark=3.5
    pip install pydeequ  # Not available on conda-forge
    ```

## Verify Installation

```bash
uv run python -c "
import os
os.environ['SPARK_VERSION'] = '3.5'
import pydeequ
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .master('local[*]')
    .config('spark.jars.packages', pydeequ.deequ_maven_coord)
    .config('spark.jars.excludes', pydeequ.f2j_maven_coord)
    .config('spark.ui.enabled', 'false')
    .getOrCreate())

print(f'Spark {spark.version} + PyDeequ OK')
print(f'Deequ JAR: {pydeequ.deequ_maven_coord}')
spark.stop()
"
```

!!! success "Expected output"
    ```
    Spark 3.5.x + PyDeequ OK
    Deequ JAR: com.amazon.deequ:deequ:2.0.7-spark-3.5
    ```

## Environment Variables

```bash
export SPARK_VERSION=3.5              # Required for PyDeequ JAR selection
export PYSPARK_PYTHON=python3         # Python interpreter for workers
export PYSPARK_DRIVER_PYTHON=python3  # Python interpreter for driver
export SPARK_LOCAL_IP=127.0.0.1       # Avoid DNS issues in local mode
export JAVA_HOME=/usr/lib/jvm/java-11 # Point to your Java installation
```

!!! tip "Shell configuration"
    Add these to your `~/.bashrc` or `~/.zshrc` for persistence.

## Available Tasks

| Command | Description |
| --- | --- |
| `uv run task test` | Run tests, stop on first failure |
| `uv run task test_verbose` | Verbose output with full tracebacks |
| `uv run task lint` | Lint with ruff |
| `uv run task format` | Format code with ruff |
| `uv run task typecheck` | Type check with mypy |
| `uv run task security` | Security scan with bandit |
| `uv run task check` | Full CI pipeline |
| `uv run task docs` | Build documentation |
| `uv run task docs_serve` | Serve docs locally |
| `uv run task clean` | Remove caches and artifacts |

## Makefile Shortcuts

```bash
make install      # Install all dependencies
make test         # Run tests
make quality      # Lint + format + typecheck + security
make check        # Full CI pipeline (quality + test)
make docs         # Build docs
```

## Troubleshooting

??? question "Error: `SPARK_VERSION` not set"
    Ensure `os.environ["SPARK_VERSION"] = "3.5"` is set **before** `import pydeequ`.
    PyDeequ reads this at import time, not at SparkSession creation.

??? question "Error: `ClassNotFoundException` for Deequ"
    The Deequ JAR failed to download. Check internet connectivity and try:
    ```bash
    rm -rf ~/.ivy2/cache/com.amazon.deequ
    ```
    Then re-run to force a fresh download.

??? question "Error: `f2j` classpath conflict"
    Ensure you have the exclusion configured:
    ```python
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    ```

??? question "Tests hang after completion"
    This is a known JVM thread issue. The `conftest.py` includes
    `os._exit()` in `pytest_sessionfinish` to handle this.
