---
applyTo: "{**/test_*.py,**/*_test.py,**/conftest.py}"
---

# PySpark + Chispa Testing Instructions

## SparkSession Fixture

A single session-scoped fixture lives in `tests/conftest.py`. Never create a
SparkSession inside individual test files — always inject via the `spark` parameter:

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
- `ui.enabled=false` — skip Spark Web UI to speed up fixture creation.
- `setLogLevel("ERROR")` — suppress all output except actual errors.

## Test Organisation

Group tests into classes by capability. Mirror `src/` structure in `tests/`:

```
src/data_frame/columns/column_equality.py
    → tests/columns/test_column_equality.py  →  class TestRemoveNonWordCharacters
src/data_frame/equality/df_equality.py
    → tests/equality/test_df_equality.py     →  class TestSortColumns, class TestApproxDfEquality
```

Class naming: `Test` + function or concept being tested.

## Chispa Assertion Patterns

### Exact column equality
Compare a computed column against an expected column in the same DataFrame:

```python
from chispa.column_comparer import assert_column_equality

def test_clean_name(self, spark):
    data = [("jo&&se", "jose"), (None, None)]
    df = spark.createDataFrame(data, ["name", "expected"]).withColumn(
        "result", my_function(F.col("name"))
    )
    assert_column_equality(df, "result", "expected")
```

### Approximate column equality
For floating-point results, use a precision threshold:

```python
from chispa import assert_approx_column_equality

assert_approx_column_equality(df, "result", "expected", 0.01)
```

### Full DataFrame equality
Compare two entire DataFrames (schema + data):

```python
from chispa import assert_df_equality

assert_df_equality(actual_df, expected_df)
```

### Approximate DataFrame equality
```python
from chispa.dataframe_comparer import assert_approx_df_equality

assert_approx_df_equality(df1, df2, precision=0.1)
```

### Schema equality
Always compare `.schema` (StructType), not the DataFrame itself:

```python
from chispa.schema_comparer import assert_schema_equality

assert_schema_equality(df1.schema, df2.schema)
```

### Testing expected failures
When a chispa assertion is *expected* to raise, wrap with `pytest.raises`:

```python
with pytest.raises(Exception):
    assert_df_equality(df1, df2)

with pytest.raises(ValueError, match="valid sort orders"):
    sort_columns(df, "invalid")
```

## Standard Assertions

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

## Null Handling

When creating DataFrames with all-null rows, PySpark cannot infer the schema.
Provide an explicit schema:

```python
from pyspark.sql.types import DoubleType, StructField, StructType

schema = StructType([
    StructField("num", DoubleType()),
    StructField("expected", DoubleType()),
])
df = spark.createDataFrame([(None, None)], schema)
```

## Edge Cases to Always Cover

Every test class should include cases for:
- **Null values** — verify null propagation
- **Empty strings** — distinct from null
- **Empty DataFrames** — zero rows with correct schema
- **Single-column / single-row** — boundary conditions
- **Error paths** — invalid input wrapped in `pytest.raises`

## Pure Python Unit Tests

Helper functions with no Spark dependency (e.g. `string_helper`) should have
standalone tests that don't need the `spark` fixture:

```python
class TestDotsToUnderscores:
    def test_single_dot(self):
        assert dots_to_underscores("a.b") == "a_b"
```

## Docstrings in Tests

Use Google-style docstrings on test classes to describe scope. Individual test
methods do not need docstrings — the method name should be descriptive enough:

```python
class TestSortColumns:
    """Tests for the sort_columns DataFrame utility."""

    def test_ascending(self, spark):
        ...

    def test_invalid_sort_order_raises(self, spark):
        ...
```

Add a docstring to a test method only when the scenario is non-obvious:

```python
def test_null_propagation_with_explicit_schema(self, spark):
    """Verify null handling when schema is provided explicitly.

    PySpark cannot infer types from all-null rows, so an explicit
    StructType is required to avoid CANNOT_DETERMINE_TYPE errors.
    """
```

## Entry Point

Always include a direct-run entry point:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```

## Running Tests

```bash
uv run task test            # sequential, stop on first failure
uv run task test_parallel   # parallel via pytest-xdist
uv run task test_verbose    # verbose output with full tracebacks
```
