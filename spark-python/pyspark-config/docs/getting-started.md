# Getting Started

## Prerequisites

!!! warning "Java required"
    Java 11 or 17 must be on your `PATH`. PySpark will not start without a JVM.

    ```bash
    java -version
    ```

## Installation

=== "uv (Recommended)"
    ```bash
    uv sync
    ```

=== "pip"
    ```bash
    pip install pyspark==3.5.0 jproperties~=2.1.1 findspark~=2.0.1
    ```

=== "conda"
    ```bash
    conda install -c conda-forge pyspark=3.5.0
    pip install jproperties findspark
    ```

## Project Structure

```
pyspark-config/
├── cfg/                          # Sample config files
│   ├── config.cfg                #   INI with BasicInterpolation
│   ├── config.conf               #   INI with ExtendedInterpolation
│   └── config.properties         #   Java-style properties
├── src/cfg/
│   ├── option/                   # Config file readers
│   │   ├── config_jproperties/   # Reusable PropertiesHandler
│   │   └── config_parser/        # ConfigParser & jproperties examples
│   ├── validation/               # SparkConf creation & inspection
│   ├── dynamic/                  # Runtime config changes
│   └── library/                  # findspark usage
├── notebooks/                    # Jupyter notebooks
├── tests/                        # pytest test suite
├── pyproject.toml                # uv / PEP 621 project metadata
└── mkdocs.yml                    # This documentation
```

## Run an Example

```bash
uv run python src/cfg/option/config_parser/config_option.py
```

This prints every Spark configuration key-value pair for a local session.

## Run the Tests

```bash
uv run pytest -v
```

## Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `SPARK_MASTER` | Spark master URL | `local[*]` |
| `PYSPARK_PYTHON` | Python binary for executors | `python3` |
| `PYSPARK_DRIVER_PYTHON` | Python binary for driver | `python3` |
| `SPARK_LOCAL_IP` | Bind address for local mode | `127.0.0.1` |
