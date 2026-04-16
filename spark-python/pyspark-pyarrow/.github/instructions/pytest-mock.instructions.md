---
applyTo: "{**/test_*.py,**/*_test.py,**/conftest.py}"
---

# pytest-mock Instructions

## When to Use Mocks

Use `pytest-mock` (the `mocker` fixture) to isolate units under test from:

- **External services** — APIs, databases, cloud storage.
- **Slow operations** — Spark I/O, network calls.
- **Non-deterministic results** — timestamps, random values.

**Do NOT mock** Spark DataFrame operations in integration-style tests — use a real
local SparkSession instead.

## mocker Fixture

`pytest-mock` provides the `mocker` fixture automatically. Use it instead of
`unittest.mock.patch` decorators:

```python
def test_read_config(mocker):
    mocker.patch("mymodule.open", mocker.mock_open(read_data='{"key": "val"}'))
    result = load_config("dummy.json")
    assert result["key"] == "val"
```

## Patching Patterns

### Patch a function return value

```python
def test_get_table_path(mocker):
    mocker.patch("mymodule.resolve_path", return_value="/data/table.parquet")
    path = get_table_path("orders")
    assert path == "/data/table.parquet"
```

### Patch an environment variable

```python
def test_custom_master(mocker):
    mocker.patch.dict("os.environ", {"SPARK_MASTER": "local[4]"})
    session = create_session()
    assert session.conf.get("spark.master") == "local[4]"
```

### Spy — verify a call without changing behaviour

```python
def test_logs_warning(mocker):
    spy = mocker.patch("mymodule.logger.warning")
    process_empty_dataframe(spark.createDataFrame([], schema))
    spy.assert_called_once()
```

### Mock Spark I/O in unit tests

```python
def test_transform_without_io(mocker, spark):
    sample = spark.createDataFrame([(1, "a")], ["id", "val"])
    mocker.patch.object(spark.read, "parquet", return_value=sample)
    result = my_etl_job(spark, "/fake/path")
    assert result.count() == 1
```

## Assertion Helpers

```python
mock_fn.assert_called_once()
mock_fn.assert_called_with("expected_arg")
mock_fn.assert_not_called()
assert mock_fn.call_count == 3
```

## Organising Mock Tests

Group mock-heavy tests in a dedicated class to separate them from integration tests:

```python
class TestTransformLogic:
    """Integration tests — real SparkSession, no mocks."""
    ...

class TestIOLayer:
    """Unit tests — mock external I/O."""
    ...
```

## Anti-Patterns to Avoid

- **Over-mocking**: don't mock Spark internals (`_jdf`, `_jvm`). Test at the DataFrame API level.
- **Mocking what you own**: prefer testing real implementations; mock only boundaries.
- **Ignoring call assertions**: if you patch something, assert it was called correctly.
- **Leaking mocks**: `mocker` auto-cleans up — avoid manual `patch.start()` / `patch.stop()`.
