# Mock Testing

Use `unittest.mock` to test PySpark code without creating real SparkSessions
or reading actual files.

## When to Use Mocks

!!! success "Good fit"
    - Testing reader/writer functions without real files
    - Verifying that Spark API methods are called correctly
    - Fast unit tests that don't need a JVM

!!! failure "Not a good fit"
    - Testing DataFrame transformations (use real Spark)
    - Testing SQL queries (use real Spark)
    - Testing data correctness (use real data)

## Pattern: Mock SparkSession

```python
from unittest.mock import Mock

@pytest.fixture
def spark():
    mock = Mock()
    mock.read.format.return_value = mock
    mock.option.return_value = mock
    mock.load.return_value = Mock()  # returns a mock DataFrame
    return mock
```

## Example: Testing a CSV Reader

```python
from unittest.mock import Mock
from reader.spark_reader import load_csv

def test_load_csv():
    mock_spark = Mock()
    mock_spark.read.format.return_value = mock_spark
    mock_spark.option.return_value = mock_spark
    mock_spark.load.return_value = Mock()

    result = load_csv(mock_spark, "test.csv")

    mock_spark.read.format.assert_called_with("csv")
    mock_spark.option.assert_any_call("sep", ",")
    mock_spark.option.assert_any_call("inferSchema", "true")
    mock_spark.option.assert_any_call("header", "true")
    mock_spark.load.assert_called_with("test.csv")
```

## Key Mock Assertions

| Method | Checks |
| --- | --- |
| `.assert_called()` | Was called at least once |
| `.assert_called_with(...)` | Was called with exact args |
| `.assert_any_call(...)` | Was called with args at any point |
| `.assert_called_once()` | Was called exactly once |
| `.called` | Boolean — was it called? |
| `.call_count` | Number of times called |

## Run Tests

```bash
uv run pytest tests/reader/ -v
```
