# Testing

This section covers the test setup, patterns, and how to run the test suite.

## Test Suite Overview

| Test File | Tests | What it covers |
|-----------|-------|---------------|
| `test_spark_app.py` | 10 | SparkSession, DataFrame ops, SQL, window functions, Parquet I/O |
| `test_pyarrow_conversion.py` | 7 | Arrow conversions, mapInPandas, applyInPandas, Parquet I/O |
| `test_pandas_udf.py` | 3 | Scalar, grouped aggregate, grouped map UDFs |
| `test_udtf.py` | 3 | SquareNumbers, FibonacciNumbers, Arrow UDTF |

## Run Tests

```bash
cd spark-python/pyspark-pyarrow
poetry run pytest tests/ -v --tb=short
```

!!! tip "Quick run"

    ```bash
    poetry run pytest tests/ -x -q    # stop on first failure, quiet output
    ```

## Topics

- [Test Setup](test-setup.md) — conftest.py, fixtures, Java config
