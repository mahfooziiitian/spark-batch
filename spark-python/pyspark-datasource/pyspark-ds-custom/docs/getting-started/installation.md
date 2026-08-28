# Installation

## Prerequisites

!!! warning "Java 17 required"
    PySpark 4.x requires **Java 17** on your `PATH`. Verify with:
    ```bash
    java -version
    ```

## Install

=== "uv (recommended)"
    ```bash
    uv sync
    ```

=== "pip"
    ```bash
    pip install -e .
    ```

## Dependencies

| Package | Purpose |
|---------|---------|
| `pyspark>=4.0.0` | Spark runtime + Python Data Source API |
| `pyarrow>=14.0.0` | Arrow batch serialization (required by API) |
| `requests>=2.32.0` | HTTP client for REST API connectors |

### Dev Dependencies

| Package | Purpose |
|---------|---------|
| `pytest>=8.0` | Test runner |
| `faker>=37.0.0` | Test data generation |
| `fastapi>=0.115.0` | Mock API servers |
| `uvicorn>=0.34.0` | ASGI server for FastAPI |

## Verify Installation

```bash
uv run python -c "from custom_ds import create_spark_session; print('OK')"
```

## Project Structure

```
pyspark-ds-custom/
├── src/custom_ds/        # Installable library
│   ├── batch/            # In-memory batch reader
│   ├── writer/           # JSON-lines batch sink
│   ├── streaming/        # Counter streaming source
│   ├── restapi/          # REST API connectors (batch, stream, Arrow)
│   ├── util/             # Registration helper
│   └── session.py        # SparkSession factory
├── examples/             # Runnable demo scripts
├── tests/                # pytest suite
├── docs/                 # This documentation
└── pyproject.toml        # Project config
```
