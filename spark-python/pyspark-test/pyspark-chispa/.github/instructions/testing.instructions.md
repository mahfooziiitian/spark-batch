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
        SparkSession.builder.appName("chispa-test-suite")
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

Group tests into classes by function. Mirror `src/data_frame/` structure in `tests/`:

```
src/data_frame/columns/column_equality.py
    → tests/columns/test_column_equality.py  →  class TestRemoveNonWordCharacters
src/data_frame/equality/df_equality.py
    → tests/equality/test_df_equality.py     →  class TestSortColumns, class TestColumnsMatch
```

Class naming: `Test` + function or concept being tested.

## Imports

Import source modules from the `data_frame` package:

```python
from data_frame.columns.column_equality import remove_non_word_characters
from data_frame.equality.df_equality import sort_columns
from data_frame.helper.string_helper import dots_to_underscores
from data_frame.schema.schema_utils import schema_to_dict
```

Import chispa assertions explicitly:

```python
from chispa.column_comparer import assert_column_equality
from chispa import assert_approx_column_equality, assert_df_equality
from chispa.dataframe_comparer import assert_approx_df_equality
from chispa.schema_comparer import assert_schema_equality
```

## Chispa Assertion Patterns

### Exact column equality

```python
def test_clean_name(self, spark):
    data = [("jo&&se", "jose"), (None, None)]
    df = spark.createDataFrame(data, ["name", "expected"]).withColumn("actual", my_function(F.col("name")))
    assert_column_equality(df, "actual", "expected")
```

### Approximate column equality

```python
assert_approx_column_equality(df, "actual", "expected", 0.01)
```

### Full DataFrame equality

```python
assert_df_equality(actual_df, expected_df)
```

### Schema equality — pass `.schema`, not the DataFrame

```python
assert_schema_equality(df1.schema, df2.schema)
```

### Testing expected failures

```python
with pytest.raises(Exception):
    assert_df_equality(df1, df2)
```

## Null Handling

When creating DataFrames with all-null rows, provide an explicit schema:

```python
from pyspark.sql.types import StringType, StructField, StructType

schema = StructType([StructField("text", StringType()), StructField("expected", StringType())])
df = spark.createDataFrame([(None, None)], schema)
```

## Edge Cases to Always Cover

- **Null values** — verify null propagation (use explicit schemas)
- **Empty strings** — distinct from null
- **Empty DataFrames** — zero rows with correct schema
- **Error paths** — invalid input wrapped in `pytest.raises`

## Pure Python Unit Tests

Helper functions with no Spark dependency should have standalone tests
that don't need the `spark` fixture:

```python
class TestDotsToUnderscores:
    def test_single_dot(self):
        assert dots_to_underscores("a.b") == "a_b"
```

## Entry Point

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
