---
applyTo: "tests/**/*.py"
---

# Testing Conventions

## Framework

- Use **pytest** as the test runner.
- Tests live under `tests/` mirroring the `src/` structure where applicable.
- Test file names follow the pattern `*_test.py` (e.g., `json_df_test.py`).

## SparkSession Fixture

Provide a session-scoped SparkSession fixture to avoid repeated JVM startup overhead:

```python
import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName("json-tests")
        .getOrCreate()
    )
    yield session
    session.stop()
```

## Writing Tests

### Test JSON Read / Write Round-Trip

Use pytest's `tmp_path` fixture to create isolated temporary directories:

```python
import json
from pathlib import Path
from pyspark.sql import SparkSession


def test_read_json_basic(spark: SparkSession, tmp_path: Path) -> None:
    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    json_file = tmp_path / "test.json"
    with json_file.open("w") as f:
        for record in data:
            f.write(json.dumps(record) + "\n")

    df = spark.read.json(str(json_file))

    assert df.count() == 2
    assert set(df.columns) == {"name", "age"}
```

### Schema Assertions

Validate that the DataFrame schema matches expectations:

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType


def test_schema_inference(spark: SparkSession, tmp_path: Path) -> None:
    # ... write JSON and read ...

    expected_schema = StructType([
        StructField("age", LongType(), nullable=True),
        StructField("name", StringType(), nullable=True),
    ])
    assert df.schema == expected_schema
```

### Data Content Assertions

Collect results and compare against expected values:

```python
def test_data_content(spark: SparkSession, tmp_path: Path) -> None:
    # ... write JSON and read ...

    rows = sorted(df.collect(), key=lambda r: r["name"])
    assert rows[0]["name"] == "Alice"
    assert rows[0]["age"] == 30
```

## Guidelines

- Keep tests focused — one behavior per test function.
- Use `tmp_path` (function-scoped) for file I/O; never write to fixed paths.
- Prefer `df.collect()` for small result sets; use `df.count()` or `df.first()` for large data.
- Name test functions descriptively: `test_<feature>_<scenario>` (e.g., `test_gzip_compression_round_trip`).
- Always assert on both schema structure and data content when testing read/write.
- Use `spark.read.schema(expected_schema).json(path)` in tests to avoid schema inference variability.

## Running Tests

```bash
# From project root
pytest

# Verbose output
pytest -v

# Run a specific test file
pytest tests/df/json_df_test.py
```
