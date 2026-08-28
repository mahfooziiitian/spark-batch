---
applyTo: "tests/**/*.py"
---

# Testing Conventions

## Framework

- pytest with pytest-sugar (dev dependency).
- Run with `uv run pytest` or `uv run pytest tests/ -v --tb=short`.

## Directory Layout

```
tests/
├── conftest.py                    # Shared session-scoped SparkSession fixture
├── test_rest_api.py               # Unit tests for REST API utilities
├── test_incremental_state.py      # Watermark parsing + IncrementalStateStore tests
└── test_incremental_runner.py     # Incremental runner integration tests
```

For DB-backed state store tests, use a `tmp_path`-based SQLite file (never
a shared/production DB URL) so each test gets an isolated database:

```python
@pytest.fixture
def state_store(tmp_path):
    return IncrementalStateStore(f"sqlite:///{tmp_path / 'test.db'}")
```

To exercise the incremental runner without a real HTTP call, patch
`fetch_records` at the point it's imported into `incremental_runner`:

```python
with patch("incremental.incremental_runner.fetch_records", return_value=(records, {})):
    df = run_incremental_ingestion(spark, config, "source_name", state_store=state_store)
```


## SparkSession Fixture

Define once in `conftest.py`, shared across all test files:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("test-pyspark-ds-api")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

Key settings:
- `local[2]` — two threads; deterministic and fast.
- `shuffle.partitions=2` — default 200 is wasteful for test data.
- `ui.enabled=false` — skip Spark Web UI to speed up fixture creation.
- `setLogLevel("ERROR")` — suppress all output except actual errors.

## Test Organisation

Group tests into classes by capability area:

```python
class TestReadKeyValue:
    """Tests for dot-path JSON key traversal."""

class TestAuthentication:
    """Tests for auth header/object building."""

class TestPagination:
    """Tests for pagination strategies."""

class TestRequestBuilder:
    """Tests for request component building."""

class TestAPIClient:
    """Tests for the core APIClient class."""

class TestIngestion:
    """Tests for parallel and partitioned API ingestion."""
```

## Test Data

Use `tmp_path` for file output tests:

```python
def test_write_json_response(self, tmp_path):
    path = str(tmp_path / "output.json")
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    FileWriter.write_json_response_to_file(path, data)

    with open(path) as f:
        lines = f.readlines()
    assert len(lines) == 2
```

## Mocking API Responses

Use `unittest.mock` to mock HTTP calls:

```python
from unittest.mock import patch, MagicMock

def test_make_request_success(self):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": [{"id": 1}]}
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response):
        response = make_request(url="http://api.example.com/data", method="GET")
        assert response.json()["data"][0]["id"] == 1
```

For testing pagination, mock sequential responses:

```python
def test_cursor_pagination(self):
    responses = [
        MagicMock(json=lambda: {"data": [{"id": 1}], "next_cursor": "abc"}),
        MagicMock(json=lambda: {"data": [{"id": 2}], "next_cursor": None}),
    ]
    with patch("requests.get", side_effect=responses):
        result = fetch_paginated_data(url="http://api.example.com", strategy="cursor", ...)
        assert len(result) == 2
```

## Testing JSON Path Traversal

Test `read_key_value()` with nested structures:

```python
def test_read_key_value_nested(self):
    data = {"meta": {"pagination": {"total_pages": 5, "current_page": 1}}}
    assert read_key_value(data, "meta.pagination.total_pages") == 5

def test_read_key_value_top_level(self):
    data = {"name": "Alice", "age": 30}
    assert read_key_value(data, "name") == "Alice"
```

## Assertions

Prefer `df.count()` over `len(df.collect())`:

```python
assert df.count() == 10
assert set(df.columns) == {"id", "name", "email"}
```

For single-row assertions, collect minimally:

```python
row = df.filter(F.col("id") == 1).first()
assert row["name"] == "Alice"
```

For set-based comparisons where order doesn't matter:

```python
values = {r["name"] for r in df.collect()}
assert values == {"Alice", "Bob", "Charlie"}
```

## Schema Assertions

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

expected_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])
assert df.schema == expected_schema
```

## Naming Conventions

- Test files: `test_<topic>.py`
- Test classes: `Test<CapabilityArea>`
- Test methods: `test_<what_it_verifies>`

## Entry Point

Every test file includes a direct-run entry point:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```
