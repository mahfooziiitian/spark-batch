---
applyTo: "{**/test_*.py,**/*_test.py,**/conftest.py}"
---

# PySpark Testing Instructions (Root-Level Defaults)

These are baseline testing conventions for all child projects. Each child project
may override these via its own `.github/instructions/testing.instructions.md`.

## SparkSession Fixture Patterns

Child projects use one of two fixture patterns. Follow the pattern established
in the specific child project.

### Pattern 1: Shared conftest.py (preferred for new projects)

A single session-scoped fixture in `tests/conftest.py` shared by all test files:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-suite")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

Used by: **pyspark-chispa**, **pyspark-deepu**, **pyspark-pytest**

### Pattern 2: Per-file inline fixtures

Each test file defines its own session-scoped SparkSession fixture:

```python
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("test-name").getOrCreate()
    yield spark
    spark.stop()
```

When working in a project that uses per-file fixtures, add the fixture to new test
files rather than creating a `conftest.py`.

### Key settings:
- `local[*]` — use all available cores for parallelism.
- `shuffle.partitions=2` — default 200 is wasteful for test data (when used).
- `ui.enabled=false` — skip Spark Web UI to speed up fixture creation (when used).
- `setLogLevel("ERROR")` — suppress all output except actual errors.

## Test Organisation

- Mirror source directory structure in `tests/`.
- One test file per source module.
- Group related tests into classes or keep as standalone functions — follow the
  pattern established in the specific child project.

## Assertions

Prefer `df.count()` over `len(df.collect())`:

```python
assert df.count() == 5
assert set(df.columns) == {"id", "name", "score"}
assert df.filter(F.col("id") > 3).count() == 2
```

For single-row checks, collect minimally:

```python
row = df.filter(F.col("region") == "North").first()
assert row["total_revenue"] == 1999.98
```

## Test Data Creation

Create small, focused DataFrames for each test:

```python
data = [("alice", 100), ("bob", 200), (None, None)]
df = spark.createDataFrame(data, ["name", "amount"])
```

When all values might be null, provide an explicit schema:

```python
from pyspark.sql.types import DoubleType, StructField, StructType

schema = StructType([
    StructField("num", DoubleType()),
    StructField("expected", DoubleType()),
])
df = spark.createDataFrame([(None, None)], schema)
```

## Edge Cases to Always Cover

- **Null values** — verify null propagation
- **Empty strings** — distinct from null
- **Empty DataFrames** — zero rows with correct schema
- **Single-column / single-row** — boundary conditions
- **Error paths** — invalid input wrapped in `pytest.raises`

## Entry Point

Always include a direct-run entry point:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```

## CI Environment Variables

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```
