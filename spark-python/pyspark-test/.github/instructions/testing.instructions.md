---
applyTo: "{**/test_*.py,**/*_test.py,**/conftest.py}"
---

# PySpark Testing Instructions (Root-Level Defaults)

These are baseline testing conventions for all child projects. Each child project
may override these via its own `.github/instructions/testing.instructions.md`.

## SparkSession Fixture

Use a single session-scoped fixture in `tests/conftest.py`:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-suite")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

**Key settings:**
- `local[2]` — two threads; deterministic and fast.
- `shuffle.partitions=2` — default 200 is wasteful for test data.
- `ui.enabled=false` — skip Spark Web UI.
- `setLogLevel("ERROR")` — suppress all output except actual errors.

## Test Organisation

- Group tests into classes by function or capability.
- Mirror `src/` structure in `tests/` — one test file per source module.
- Shared fixtures go in `tests/conftest.py` only — never in individual test files.

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
