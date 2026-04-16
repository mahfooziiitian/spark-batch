---
applyTo: "{**/test_*.py,**/*_test.py,**/conftest.py}"
---

# PyDeequ Testing Instructions

## SparkSession Fixture

PyDeequ tests require the Deequ Maven JAR. Use a session-scoped fixture:

```python
import os
import pytest
import pydeequ
from pyspark.sql import SparkSession

os.environ["SPARK_VERSION"] = "3.0.2"

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-suite")
        .master("local[2]")
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

**Key settings:**
- `spark.jars.packages` — downloads the Deequ JAR at session startup.
- `spark.jars.excludes` — excludes conflicting f2j dependency.
- `SPARK_VERSION` env var — must match the installed PySpark version.

## Shared Fixture

Place the SparkSession fixture in `tests/conftest.py` so all test files share one
session. Do not create inline fixtures in individual test files.

## Test Organisation

- Mirror `src/` structure in `tests/`.
- One test file per source module.
- Group tests into classes by feature:

```python
class TestAnalyzers:
    """Tests for PyDeequ analyzer computations."""

    def test_size_analyzer(self, spark):
        ...

    def test_completeness_analyzer(self, spark):
        ...
```

## Assertion Patterns

PyDeequ returns analysis results as DataFrames. Assert on collected rows:

```python
result_df = AnalysisRunner(spark).onData(df).addAnalyzer(Size()).run()
result = AnalysisRunner.successMetricsAsDataFrame(spark, result_df)
row = result.filter(F.col("name") == "Size").first()
assert row["value"] == expected_count
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
pytest tests/
```
