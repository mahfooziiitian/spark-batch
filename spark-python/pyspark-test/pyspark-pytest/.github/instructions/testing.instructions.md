---
applyTo: "{**/test_*.py,**/*_test.py,**/conftest.py}"
---

# PySpark Testing Instructions — pyspark-pytest

## SparkSession Fixture

Use a single session-scoped fixture in `tests/conftest.py`:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-suite")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

**Key settings:**
- `local[2]` — two threads; deterministic and fast.
- `shuffle.partitions=2` — default 200 is wasteful for test data.
- `ui.enabled=false` — skip Spark Web UI.
- `setLogLevel("ERROR")` — suppress all output except actual errors.
- Do **not** hardcode `JAVA_HOME` in fixtures — set it via environment.

## Shared Fixture

Place the SparkSession fixture in `tests/conftest.py`. Do not create inline
fixtures in individual test files — reuse the shared fixture.

## Assertion Patterns

### PySpark Built-in (preferred)

```python
from pyspark.testing.utils import assertDataFrameEqual

assertDataFrameEqual(actual_df, expected_df)
```

### Row count

```python
assert df.count() == 5
```

### Column checks

```python
assert set(df.columns) == {"id", "name", "amount"}
```

### Single-row assertions

```python
row = df.filter(F.col("id") == 1).first()
assert row["name"] == "expected_value"
```

### SQL assertions

```python
df.createOrReplaceTempView("my_table")
result = spark.sql("SELECT COUNT(*) AS cnt FROM my_table").first()
assert result["cnt"] == expected
```

## Mock-Based Testing

For testing reader functions without actual files, use `unittest.mock`:

```python
from unittest.mock import Mock

def test_load_csv():
    mock_spark = Mock()
    mock_df = Mock()
    mock_spark.read.csv.return_value = mock_df
    result = load_csv(mock_spark, "test.csv")
    mock_spark.read.csv.assert_called_once()
```

## Test Organisation

- Mirror `src/` structure in `tests/`.
- Group tests into classes by feature:

```python
class TestDataProcessing:
    """Tests for the data processing pipeline."""

    def test_normalise_transactions(self, spark):
        ...

    def test_classify_transactions(self, spark):
        ...
```

## Edge Cases

- Empty DataFrames
- Null values and null propagation
- Single-row DataFrames
- Invalid/missing columns

## Entry Point

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```

## Running Tests

```bash
poetry run pytest                    # all tests
poetry run pytest tests/ -v          # verbose
poetry run pytest tests/dataframe/   # specific directory
```
