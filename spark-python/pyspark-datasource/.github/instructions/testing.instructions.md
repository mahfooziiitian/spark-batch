---
applyTo: "**/tests/**/*.py"
---

# Testing Conventions

## Framework

- pytest ≥ 8.0 (dev dependency in each child project).
- Run with `uv run pytest` or `uv run pytest tests/ -v --tb=short`.

## Directory Layout

```
tests/
├── __init__.py
├── conftest.py              # session-scoped SparkSession fixture
├── test_read.py             # read tests
├── test_write.py            # write tests
└── test_<topic>.py          # additional topic tests
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
        .appName("test-pyspark-datasource")
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
class TestReadBasic:
    """Tests for basic datasource reading."""

class TestReadOptions:
    """Tests for reader options and configuration."""

class TestWriteBasic:
    """Tests for datasource write operations."""

class TestSchemaHandling:
    """Tests for schema inference and explicit schemas."""

class TestRoundTrip:
    """Tests for write-then-read round-trip integrity."""
```

## Test Data

Create temporary files using pytest's `tmp_path` fixture:

```python
def test_read_parquet(self, spark, tmp_path):
    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, ["name", "age"])

    path = str(tmp_path / "test.parquet")
    df.write.parquet(path)

    result = spark.read.parquet(path)
    assert result.count() == 2
```

For text-based formats, write sample files directly:

```python
def test_read_csv(self, spark, tmp_path):
    path = str(tmp_path / "data.csv")
    with open(path, "w") as f:
        f.write("name,age\nAlice,30\nBob,25\n")

    df = spark.read.csv(path, header=True, inferSchema=True)
    assert df.count() == 2
    assert set(df.columns) == {"name", "age"}
```

## Assertions

Prefer `df.count()` over `len(df.collect())`:

```python
assert df.count() == 5
assert df.columns == ["id", "name", "value"]
assert set(df.columns) == {"id", "name", "price"}
```

For single-row assertions, collect minimally:

```python
row = df.filter(F.col("name") == "Alice").first()
assert row["age"] == 30
```

For ordered multi-row assertions:

```python
rows = df.orderBy("name").collect()
assert rows[0]["name"] == "Alice"
assert rows[1]["name"] == "Bob"
```

For set-based comparisons where order doesn't matter:

```python
values = {r["name"] for r in df.collect()}
assert values == {"Alice", "Bob", "Carol"}
```

## Schema Assertions

Verify DataFrame schemas match expectations:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

expected_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
])
assert df.schema == expected_schema
```

For partial schema checks:

```python
assert "name" in df.columns
assert df.schema["age"].dataType == IntegerType()
```

## Write + Read Round-Trip Tests

Verify written data is readable and correct:

```python
def test_write_and_read_parquet(self, spark, tmp_path):
    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, ["name", "age"])

    path = str(tmp_path / "output")
    df.write.mode("overwrite").parquet(path)

    read_back = spark.read.parquet(path)
    assert read_back.count() == 2
    names = {r["name"] for r in read_back.collect()}
    assert names == {"Alice", "Bob"}
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
