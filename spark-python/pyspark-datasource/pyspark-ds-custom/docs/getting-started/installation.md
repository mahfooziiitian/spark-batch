# Installation

## Prerequisites

!!! warning "Java 17 required"
    PySpark 4.x requires **Java 17** (not 11). Verify with:
    ```bash
    java -version
    # openjdk version "17.0.x" ...
    ```

    === "macOS"
        ```bash
        brew install openjdk@17
        ```

    === "Ubuntu/Debian"
        ```bash
        sudo apt install openjdk-17-jdk
        ```

    === "SDKMAN"
        ```bash
        sdk install java 17.0.12-tem
        ```

## Install

=== "uv (recommended)"
    ```bash
    uv sync
    ```

=== "pip"
    ```bash
    pip install -e ".[dev]"
    ```

## Dependencies

### Runtime

| Package | Version | Purpose |
|---------|---------|---------|
| [`pyspark`](https://spark.apache.org/docs/4.0.0/api/python/) | ≥ 4.0.0 | Spark runtime + Python Data Source API |
| [`pyarrow`](https://arrow.apache.org/docs/python/) | ≥ 14.0.0 | Arrow batch serialization (required by API) |
| [`requests`](https://docs.python-requests.org/) | ≥ 2.32.0 | HTTP client for REST API connectors |

### Development

| Package | Purpose |
|---------|---------|
| `pytest` + `pytest-cov` | Test runner + coverage |
| `ruff` | Linting + formatting |
| `bandit` | Security scanning |
| `pip-audit` | Dependency vulnerability audit |
| `faker` + `fastapi` + `uvicorn` | Mock API server |
| `pyspark-data-sources` | Community data source connectors |
| `mkdocs-material` | Documentation |

## Verify Installation

```bash
uv run python -c "from custom_ds import create_spark_session; print('✅ OK')"
```

## Development Tools

The project includes a `Makefile` and `justfile` for common tasks:

```bash
# Show all available commands
make help
just --list

# Run full quality pipeline
just ci          # install → lint → security → test → docs
make ci          # same with make
```

See [pyproject.toml](https://github.com/mahfooziiitian/spark-batch/blob/main/spark-python/pyspark-datasource/pyspark-ds-custom/pyproject.toml) for the full configuration.

## Project Structure

```
pyspark-ds-custom/
├── src/custom_ds/        # Installable library
│   ├── batch/            # In-memory batch reader
│   ├── writer/           # JSON-lines batch sink
│   ├── streaming/        # Counter streaming source
│   ├── restapi/          # REST API connectors (batch, stream, Arrow)
│   ├── uc_auth/          # Unity Catalog HTTP auth data source
│   ├── util/             # Registration helper
│   └── session.py        # SparkSession factory
├── examples/             # Runnable demo scripts (01–11)
├── tests/                # pytest suite (16 tests)
├── docs/                 # This documentation (MkDocs Material)
├── Makefile              # GNU Make automation
├── justfile              # Just runner automation
└── pyproject.toml        # Project config (hatchling + ruff + bandit + coverage)
```
