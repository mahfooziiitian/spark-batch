---
applyTo: "{**/test_*.py,**/*_test.py}"
---

# Testing Conventions

## Framework

- Use **pytest** as the test runner.
- Tests live under `tests/`, named `test_*.py`.
- Target PySpark 4.x APIs; every test that touches Spark uses the shared `spark` fixture.

## SparkSession Fixture

Defined once in `tests/conftest.py`, session-scoped to avoid repeated JVM startup:

```python
import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName("custom-ds-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

## Testing a Batch DataSource

```python
from custom_ds import SimpleDataSource


def test_simple_source_respects_num_rows_option(spark) -> None:
    spark.dataSource.register(SimpleDataSource)

    df = spark.read.format("simple").option("numRows", 25).option("numPartitions", 3).load()

    assert df.count() == 25
    assert df.rdd.getNumPartitions() == 3
```

- Assert both partition count (planning correctness) and row count / content (`read()`
  correctness) — a bug in either method can pass the other check.
- Re-registering the same `DataSource` class multiple times across tests in the same session is
  safe; `spark.dataSource.register()` overwrites the prior registration.

## Testing a Batch Sink (Writer)

Use pytest's `tmp_path` fixture — never write to a fixed path:

```python
def test_simple_sink_writes_all_rows(spark, tmp_path) -> None:
    spark.dataSource.register(SimpleSinkDataSource)

    df = spark.range(10).selectExpr("id", "concat('row-', id) as value")
    df.write.format("simple_sink").option("path", str(tmp_path)).mode("append").save()

    written_files = list(tmp_path.glob("part-*.jsonl"))
    assert written_files
```

## Testing a Streaming Source

Prefer unit-testing the `SimpleDataSourceStreamReader`/`DataSourceStreamReader` subclass directly
(no Spark session needed) over running a full streaming query in tests:

```python
def test_counter_reader_advances_offset() -> None:
    reader = SimpleCounterStreamReader({"rowsPerBatch": 3})

    rows, next_offset = reader.read(reader.initialOffset())

    assert list(rows) == [(0,), (1,), (2,)]
    assert next_offset == {"offset": 3}
```

If a full streaming query test is required, bound it with `query.awaitTermination(timeout=...)`
inside the test and always call `query.stop()` in a `finally` block.

## Guidelines

- Keep tests focused — one behavior per test function.
- Name tests descriptively: `test_<component>_<scenario>`.
- Assert on schema (`df.columns`, `df.schema`) and data content, not just row counts.
- Test library code in `src/custom_ds/` directly; do not re-test example scripts.

## Running Tests

```bash
uv run pytest              # From project root
uv run pytest -v            # Verbose output
uv run pytest tests/test_simple_source.py   # Single file
uv run pytest -k "sink"     # Pattern match
```
