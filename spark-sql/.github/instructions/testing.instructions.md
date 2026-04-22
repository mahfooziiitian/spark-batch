---
applyTo: "{**/test_*.py,**/*_test.py}"
---

# Testing Instructions — pytest + PySpark

## Stack

| Package | Version | Purpose |
|---------|---------|---------|
| `pytest` | `>=8.4.1` | Test runner |
| `chispa` | `>=0.11.1` | PySpark DataFrame equality assertions |
| `pytest-cov` | `>=6.2.1` | Coverage reporting |
| `pytest-mock` | `>=3.14.1` | Mocking / patching |
| `pytest-sugar` | `>=1.0.0` | Prettier terminal output |
| `pytest-xdist` | `>=3.8.0` | Parallel test execution (`-n auto`) |
| `pytest-html` | `>=4.1.1` | HTML test reports |
| `pytest-datadir` | `>=1.8.0` | Per-test data directory fixtures |

Run tests:

```bash
uv run task test              # all tests, verbose
uv run pytest -k "scd"        # filter by name
uv run pytest -m unit         # filter by marker
uv run pytest -n auto         # parallel execution (pytest-xdist)
```

## Test File Layout

```
tests/
  conftest.py              # shared SparkSession fixture
  <domain>/
    test_<module>.py       # one file per src module / topic
```

- Test files **must** be named `test_*.py`.
- Test functions **must** start with `test_`.

## SparkSession Fixture

Define a **session-scoped** fixture in `tests/conftest.py` — never create a SparkSession
inside an individual test function:

```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-session")
        .config("spark.sql.shuffle.partitions", "2")   # avoid 200 shuffle files
        .config("spark.ui.enabled", "false")            # skip Spark Web UI
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

## Writing PySpark Tests

### Class organisation

Group related tests into classes:

```python
class TestSCDType1:
    def test_insert_new_record(self, spark): ...
    def test_update_changed_record(self, spark): ...
    def test_no_update_unchanged_record(self, spark): ...
    def test_duplicate_key_rejected(self, spark): ...
```

### Inline DataFrames

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

### DataFrame equality with chispa

```python
from chispa.dataframe_comparer import assert_df_equality

assert_df_equality(actual, expected, ignore_row_order=True)
assert_df_equality(actual, expected, ignore_nullable=True)
assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
```

### Count and value assertions

Prefer `df.count()` over `len(df.collect())`:

```python
assert df.count() == 3
assert set(df.columns) == {"customer_id", "name", "city"}
assert df.filter(F.col("is_current") == True).count() == 2
```

For single-row assertions, collect minimally:

```python
row = df.filter(F.col("customer_id") == "cust2").first()
assert row["city"] == "TX"
assert row["prev_city"] == "CA"
```

### Schema assertions

```python
from pyspark.sql.types import BooleanType, StringType, TimestampType

def test_output_schema(spark):
    result = my_transform(spark, input_df)
    assert result.schema["is_current"].dataType == BooleanType()
    assert result.schema["city"].dataType == StringType()
    assert "start_date" in result.columns
```

### File I/O tests

Use `tmp_path` (pytest built-in) — unique per test, cleaned up automatically:

```python
def test_write_parquet(spark, tmp_path):
    path = str(tmp_path / "output.parquet")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    df.write.mode("overwrite").parquet(path)
    read_back = spark.read.parquet(path)
    assert read_back.count() == 2
```

### SCD test scenarios

Every SCD type implementation must test these three cases:

```python
class TestSCDType2:
    def test_new_record_inserted(self, spark): ...          # WHEN NOT MATCHED
    def test_changed_record_versioned(self, spark): ...     # old row expired, new row active
    def test_unchanged_record_skipped(self, spark): ...     # no new row, updated_at unchanged
    def test_idempotent_on_rerun(self, spark): ...          # same result on second run
    def test_no_duplicate_active_rows(self, spark): ...     # is_current = TRUE count == distinct keys
```

## Pytest Markers

Tags are declared in `pyproject.toml` under `[tool.pytest.ini_options]`:

```python
import pytest

@pytest.mark.unit
def test_hash_computation(): ...

@pytest.mark.integration
def test_delta_merge(spark): ...

@pytest.mark.slow
def test_large_dataset(spark): ...
```

Run by marker:

```bash
uv run pytest -m unit
uv run pytest -m "not slow"
uv run pytest -m "integration and not network"
```

## Coverage

```bash
uv run task report_cov_html   # HTML → htmlcov/index.html
uv run task report_cov_xml    # XML for CI upload
```

Coverage is configured in `pyproject.toml` `[tool.coverage.*]` — do not create `.coveragerc`.
Minimum threshold: **60%** (`fail_under = 60`).

## What to Test

| Category | Should be tested |
|----------|-----------------|
| DataFrame transformations | Shape, schema, specific values |
| Aggregations | Totals, counts, NULL handling |
| SCD merge logic | Insert, update, no-change, idempotency |
| NULL edge cases | NULLs in keys, join columns, group-by columns |
| Schema enforcement | Column types, nullable flags |
| Row hash computation | Same input → same hash; different input → different hash |
| Window functions | Deterministic ordering, partition boundaries |

## What NOT to Do

- Do **not** call `df.show()` or `df.printSchema()` in test functions.
- Do **not** write to disk (`df.write.*`) in unit tests — return DataFrames and assert in memory.
- Do **not** hardcode absolute paths — use `tmp_path` or `pytest-datadir`.
- Do **not** use `assert df.count() == n` as the **only** assertion — also verify column values.
- Do **not** create a new `SparkSession` per test function — use the session-scoped fixture.

## Entry Point

Every test file should run standalone:

```python
if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v", "--tb=short"])
```

## Environment Variables

Set these in CI and in local `.env` files:

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```
