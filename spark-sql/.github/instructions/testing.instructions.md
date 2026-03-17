---
applyTo: "tests/**/*.py"
---

# Testing Instructions — pytest + PySpark

## Stack

| Package | Purpose |
|---------|---------|
| `pytest==8.4.1` | Test runner |
| `chispa>=0.11.1` | PySpark DataFrame equality assertions |
| `pytest-cov` | Coverage |
| `pytest-mock` | Mocking |
| `pytest-sugar` | Prettier output |

Run tests:

```bash
uv run task test          # all tests with verbose output
uv run pytest tests/      # same, direct pytest
uv run pytest -k "scd"    # filter by name
uv run pytest -m unit     # filter by marker
```

## Test File Layout

```
tests/
  <domain>/
    test_<module>.py
```

- Test files **must** be named `test_*.py`
- Test functions **must** start with `test_`
- One test file per module/domain

## SparkSession in Tests

- Create a **single session per module** using a module-scoped fixture — never create a session in every test function:

```python
# tests/conftest.py  (shared across all test modules)
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-session")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
```

- Set `spark.sql.shuffle.partitions = 2` in tests to avoid creating 200 shuffle files.

## Writing PySpark Tests

### Small inline DataFrames

Use `spark.createDataFrame` with explicit column names:

```python
def test_compute_totals(spark):
    input_df = spark.createDataFrame(
        [("US", 100.0), ("US", 200.0), ("CA", 50.0)],
        ["region", "amount"],
    )
    result = compute_totals(input_df, "region")
    expected = spark.createDataFrame(
        [("US", 300.0), ("CA", 50.0)],
        ["region", "total"],
    )
    assert_df_equality(result, expected, ignore_row_order=True)
```

### DataFrame Equality with chispa

```python
from chispa.dataframe_comparer import assert_df_equality

# Ignore row order (default in most tests)
assert_df_equality(actual, expected, ignore_row_order=True)

# Ignore nullable flags
assert_df_equality(actual, expected, ignore_nullable=True)

# Ignore both
assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
```

### Schema Tests

```python
from pyspark.sql.types import DoubleType, StringType

def test_output_schema(spark):
    result = my_transform(spark, input_df)
    assert result.schema["total"].dataType == DoubleType()
    assert "region" in result.columns
```

## Pytest Markers

Declare markers in `pyproject.toml` under `[tool.pytest.ini_options]`. Tag tests:

```python
import pytest

@pytest.mark.unit
def test_fast_logic():
    ...

@pytest.mark.integration
def test_reads_delta_table(spark):
    ...

@pytest.mark.slow
def test_large_dataset(spark):
    ...
```

Run by marker:

```bash
uv run pytest -m unit
uv run pytest -m "not slow"
```

## Coverage

```bash
uv run task report_cov_html    # HTML report in htmlcov/
uv run task report_cov_xml     # XML for CI
```

Coverage config in `pyproject.toml` `[tool.coverage.*]` — do not create a separate `.coveragerc`.

## What to Test

| Category | Should be tested |
|----------|-----------------|
| DataFrame transformations | Shape, schema, values |
| Aggregations | Totals, counts, nulls |
| SCD merge logic | Insert, update, no-change cases |
| NULL edge cases | NULLs in keys, values, group columns |
| Schema enforcement | Column types, nullable flags |

## What NOT to do

- Do not call `df.show()` or `df.printSchema()` in test functions
- Do not write to disk (`df.write.*`) in unit tests — return DataFrames instead
- Do not hardcode absolute paths — use `tmp_path` fixture or `pytest-datadir` for files
- Do not use `assert df.count() == n` as the only assertion — also check values
