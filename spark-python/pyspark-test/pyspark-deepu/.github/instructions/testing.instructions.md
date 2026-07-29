---
applyTo: "{**/test_*.py,**/*_test.py,**/conftest.py}"
---

# PyDeequ Testing Instructions

## SparkSession Fixture

This project uses a **shared conftest.py** (`tests/conftest.py`) with a
session-scoped SparkSession configured with the Deequ JAR.

**Important:** `SPARK_VERSION` must be set before importing pydeequ:

```python
import os
os.environ["SPARK_VERSION"] = "3.5"

import pydeequ  # noqa: E402
import pytest  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local[2]")
               .appName("pyspark-deepu-tests")
               .config("spark.jars.packages", pydeequ.deequ_maven_coord)
               .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

**Key settings:**
- `spark.jars.packages` — downloads the Deequ JAR at session startup.
- `spark.jars.excludes` — excludes conflicting f2j dependency.
- `local[2]` — two threads for deterministic tests.
- `shuffle.partitions=2` — avoids wasteful default of 200.
- `ui.enabled=false` — skips Spark Web UI.

## Test Organisation

- Mirror `src/` structure in `tests/`.
- One test file per source module.
- Group related tests into classes:

```python
class TestAnalyzers:
    """Tests for PyDeequ analyzer computations."""

    def test_size_analyzer(self, spark):
        df = spark.createDataFrame([Row(a="foo", b=1), Row(a="bar", b=2)])
        result = AnalysisRunner(spark).onData(df).addAnalyzer(Size()).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(spark, result)

        row = result_df.filter(F.col("name") == "Size").first()
        assert row["value"] == 2.0
```

## Assertion Patterns

PyDeequ returns analysis results as DataFrames. Assert on collected rows:

```python
result_df = AnalyzerContext.successMetricsAsDataFrame(spark, result)
row = result_df.filter(F.col("name") == "Size").first()
assert row["value"] == expected_count
```

For verification results, check the status column:

```python
result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
row = result_df.first()
assert row["constraint_status"] == "Success"
```

## Edge Cases

- Test with empty DataFrames
- Test with all-null columns (completeness = 0.0)
- Test with single-row DataFrames
- Test constraint violations (expected failures)

## Entry Point

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```

## Running Tests

```bash
uv run task test                           # all tests, stop on first failure
uv run task test_verbose                   # verbose output
uv run pytest tests/constraints/ -v        # specific domain
```
