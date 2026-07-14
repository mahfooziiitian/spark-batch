---
applyTo: "{**/test_*.py,**/*_test.py}"
---

# Testing — pytest + PySpark

## Running Tests

```bash
uv run task test           # all tests
uv run pytest -k "scd"     # filter by name
uv run pytest -m unit      # filter by marker
```

## SparkSession Fixture

Session-scoped in `tests/conftest.py` — never create SparkSession per test:

```python
@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-session")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

## Test Structure

- Files: `test_*.py`. Functions: `test_*`. Group related tests in classes.
- One test file per `src/` module or topic.

## Assertions

```python
from chispa.dataframe_comparer import assert_df_equality

assert_df_equality(actual, expected, ignore_row_order=True)
```

- Use `df.count()` for row counts.
- Use `df.filter(...).first()` for single-row value checks.
- Check schema types: `result.schema["col"].dataType == StringType()`.

## SCD Test Cases (mandatory)

Every SCD implementation must test:
1. New record inserted
2. Changed record versioned/updated
3. Unchanged record skipped
4. Idempotent on rerun
5. No duplicate active rows

## Rules

- No `df.show()` or `df.printSchema()` in tests.
- No disk writes in unit tests — assert in memory.
- Use `tmp_path` for I/O tests.
- Coverage minimum: 60%.

## Markers

Declared in `pyproject.toml`: `unit`, `integration`, `slow`.

```bash
uv run pytest -m "not slow"
```
