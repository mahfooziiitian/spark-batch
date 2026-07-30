---
applyTo: "{**/test_*.py,**/*_test.py}"
---

# Testing Conventions

## Framework

- Use **pytest** as the test runner.
- Tests live under `tests/` mirroring the `src/` and `examples/` structure where applicable.
- Test file names follow the pattern `test_*.py` or `*_test.py`.
- Target PySpark 4.x APIs in tests.

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
        .appName("pys-json-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

Place this fixture in `tests/conftest.py` for automatic discovery.

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

### Testing Library Code (src/pys_json)

```python
from pys_json import JsonReader, create_spark_session
from pys_json.parsing import parse_json_column
from pys_json.schema import with_corrupt_record


def test_json_reader_multiline(spark: SparkSession, tmp_path: Path) -> None:
    json_file = tmp_path / "multi.json"
    json_file.write_text('[{"name": "Alice"}, {"name": "Bob"}]')

    reader = JsonReader(spark).multiline()
    df = reader.read(str(json_file))

    assert df.count() == 2
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

### Testing VARIANT Type (Spark 4.0+)

```python
from pyspark.sql import functions as F


def test_variant_parsing(spark: SparkSession) -> None:
    df = spark.createDataFrame([('{"name": "Alice", "age": 30}',)], ["raw"])
    result = df.select(F.parse_json("raw").alias("v"))
    assert result.schema[0].dataType.typeName() == "variant"
```

## Databricks-Specific Tests

For code targeting Databricks, use conditional skips:

```python
import pytest


def is_databricks() -> bool:
    try:
        import dbutils  # noqa: F401
        return True
    except ImportError:
        return False


@pytest.mark.skipunless(is_databricks(), reason="Requires Databricks Runtime")
def test_unity_catalog_json(spark: SparkSession) -> None:
    df = spark.read.json("/Volumes/catalog/schema/volume/test.json")
    assert df.count() > 0
```

## Guidelines

- Keep tests focused — one behavior per test function.
- Use `tmp_path` (function-scoped) for file I/O; never write to fixed paths.
- Prefer `df.collect()` for small result sets; use `df.count()` or `df.first()` for large data.
- Name test functions descriptively: `test_<feature>_<scenario>` (e.g., `test_gzip_compression_round_trip`).
- Always assert on both schema structure and data content when testing read/write.
- Use `spark.read.schema(expected_schema).json(path)` in tests to avoid schema inference variability.
- Test both library code (`src/pys_json`) and examples where applicable.

## Running Tests

```bash
# From project root
uv run pytest

# Verbose output
uv run pytest -v

# Run a specific test file
uv run pytest tests/df/json_df_test.py

# Run tests matching a pattern
uv run pytest -k "compression"
```
