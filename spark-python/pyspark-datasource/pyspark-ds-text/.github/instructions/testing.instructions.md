---
applyTo: "tests/**/*.py"
---

# Testing Conventions

## Framework

- pytest ≥ 8.0 (dev dependency).
- Run with `uv run pytest` or `uv run pytest tests/ -v --tb=short`.

## Directory Layout

```
tests/
├── __init__.py
├── conftest.py                   # session-scoped SparkSession fixture
└── test_text_datasource.py       # all text datasource tests
```

## SparkSession Fixture

Defined once in `conftest.py`, shared across all test files:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("test-pyspark-ds-text")
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
class TestReadTextBasic:
    """Tests for basic text file reading."""

class TestReadTextOptions:
    """Tests for wholetext, lineSep, encoding, pathGlobFilter, recursiveFileLookup."""

class TestParseText:
    """Tests for delimiter split, fixed-width, regex extraction."""

class TestWriteText:
    """Tests for text writer, compression, single-column requirement."""

class TestTextSQL:
    """Tests for temp views and SQL queries over text data."""

class TestWordCount:
    """Tests for word count and text analytics."""
```

## Test Data

Create temporary files using pytest's `tmp_path` fixture:

```python
def test_read_basic(self, spark, tmp_path):
    path = str(tmp_path / "sample.txt")
    with open(path, "w") as f:
        f.write("Hello World\nApache Spark\n")

    df = spark.read.text(path)
    assert df.count() == 2
    assert df.columns == ["value"]
```

For compressed test data, use Python's `gzip` and `bz2` modules:

```python
import gzip

def test_read_gzip(self, spark, tmp_path):
    path = str(tmp_path / "data.txt.gz")
    with gzip.open(path, "wt") as f:
        f.write("compressed line 1\ncompressed line 2\n")

    df = spark.read.text(path)
    assert df.count() == 2
```

## Assertions

Prefer `df.count()` over `len(df.collect())`:

```python
assert df.count() == 5
assert df.columns == ["value"]
assert set(df.columns) == {"id", "name", "price"}
```

For single-row assertions, collect minimally:

```python
row = df.filter(F.col("value").contains("ERROR")).first()
assert "NullPointerException" in row["value"]
```

For ordered multi-row assertions:

```python
rows = df.orderBy("value").collect()
assert rows[0]["value"] == "alpha"
assert rows[1]["value"] == "beta"
```

For set-based comparisons where order doesn't matter:

```python
values = {r["value"] for r in df.collect()}
assert values == {"line one", "line two", "line three"}
```

## Schema Assertions

The text datasource always returns a single `value` column:

```python
from pyspark.sql.types import StructType, StructField, StringType

expected_schema = StructType([StructField("value", StringType(), True)])
assert df.schema == expected_schema
```

## Write + Read Round-Trip Tests

Verify written text files are readable:

```python
def test_write_and_read_text(self, spark, tmp_path):
    data = [("hello",), ("world",)]
    df = spark.createDataFrame(data, ["value"])

    path = str(tmp_path / "output")
    df.write.mode("overwrite").text(path)

    read_back = spark.read.text(path)
    assert read_back.count() == 2
    values = {r["value"] for r in read_back.collect()}
    assert values == {"hello", "world"}
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
