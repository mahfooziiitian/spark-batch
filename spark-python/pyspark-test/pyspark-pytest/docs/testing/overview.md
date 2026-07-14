# Testing Overview

## SparkSession Fixture

All tests share a single session-scoped SparkSession:

```python title="tests/conftest.py"
--8<-- "tests/conftest.py"
```

Key settings:

- `local[2]` — two threads for deterministic tests
- `shuffle.partitions=2` — avoids the wasteful default of 200
- `ui.enabled=false` — skips the Spark Web UI
- `setLogLevel("ERROR")` — suppresses all output except errors

## Test Organisation

Tests mirror the source directory:

```
src/data_processing.py     → tests/test_data_processing.py
src/reader/                → tests/reader/
src/transformation/        → tests/transformation/
```

Tests are grouped into classes:

```python
class TestDataProcessing:
    """Tests for the data processing pipeline."""

    def test_classify_transactions(self, spark):
        ...

    def test_normalise_transactions(self, spark):
        ...
```

## Three Testing Approaches

### 1. PySpark Native Assertions

```python
from pyspark.testing.utils import assertDataFrameEqual

assertDataFrameEqual(actual_df, expected_df)
```

### 2. Collect and Assert

```python
output = my_function(input_df)
assert output.count() == 2
assert [row.name for row in output.collect()] == ["Alice", "Bob"]
```

### 3. Mock-Based Testing

```python
from unittest.mock import Mock

mock_spark = Mock()
result = load_csv(mock_spark, "test.csv")
mock_spark.read.format.assert_called_with("csv")
```

## Running Tests

```bash
uv run task test              # stop on first failure
uv run task test_verbose      # verbose with full tracebacks
uv run pytest tests/reader/   # specific directory
```
