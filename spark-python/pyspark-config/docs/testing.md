# Testing

The project uses [pytest](https://docs.pytest.org/) to test the configuration
utilities. Tests live in `tests/` and are run with `uv run pytest`.

## Run the Tests

```bash
uv run pytest -v
```

## Test Structure

```
tests/
└── option/
    └── config_jproperties/
        └── test_config_jproperties.py   # PropertiesHandler read/write tests
```

## Example: PropertiesHandler

The test creates a temporary `.properties` file, reads it, writes a new key, and
verifies the result:

```python title="tests/option/config_jproperties/test_config_jproperties.py"
--8<-- "tests/option/config_jproperties/test_config_jproperties.py"
```

!!! note
    The `tmp_path` fixture provides a unique temporary directory per test — no manual
    cleanup required.

## CI Environment Variables

Set these in your CI workflow to ensure PySpark starts correctly:

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```
