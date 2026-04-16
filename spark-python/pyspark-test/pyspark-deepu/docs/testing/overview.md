# Testing Overview

## SparkSession Fixture

All tests share a single session-scoped SparkSession configured with the Deequ JAR:

```python title="tests/conftest.py"
--8<-- "tests/conftest.py"
```

Key settings:

- `spark.jars.packages` — downloads the Deequ JAR at session startup
- `spark.jars.excludes` — excludes conflicting f2j dependency
- `SPARK_VERSION` env var — must match the installed PySpark version
- `local[2]` — two threads for deterministic tests
- `shuffle.partitions=2` — avoids the wasteful default of 200
- `ui.enabled=false` — skips the Spark Web UI

## Test Organisation

Tests mirror the source directory structure:

```
src/mertics/computations/analyzers/analyzers.py
    → tests/mertics/computations/test_analyzers.py
```

Tests are grouped into classes by feature:

```python
class TestAnalyzers:
    """Tests for PyDeequ analyzer computations."""

    def test_analyzer(self, spark):
        ...
```

## Assertion Patterns

PyDeequ returns analysis results as DataFrames. Assert on collected rows:

```python
result_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
row = result_df.filter(F.col("name") == "Size").first()
assert row["value"] == expected_count
```

For constraint verification, check the result status:

```python
result_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
assert result_df.filter(F.col("constraint_status") == "Failure").count() == 0
```

## Running Tests

```bash
uv run task test              # stop on first failure
uv run task test_verbose      # verbose with full tracebacks
```

## Edge Cases to Cover

- Empty DataFrames
- All-null columns (completeness = 0.0)
- Single-row DataFrames
- Constraint violations (expected failures)
