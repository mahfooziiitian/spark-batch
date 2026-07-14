# Testing

The test suite validates every extraction function both as a **pure Python
function** and through a **PySpark UDF** running in a DataFrame.

## Run Tests

```bash
uv run pytest                      # all tests, default output
uv run pytest tests/ -v            # verbose
uv run pytest tests/ -v --tb=short # verbose with short tracebacks
uv run pytest -k "namespace"       # run only namespace-related tests
```

## Test Structure

```
tests/
├── __init__.py
├── conftest.py                    # session-scoped SparkSession fixture
├── test_data_processing.py        # single-field + multi-field extraction (11 tests)
├── test_attributes_explode.py     # attribute extraction + explode (8 tests)
├── test_namespace_handling.py     # XML namespaces (9 tests)
├── test_nested_flattening.py      # order → line items flattening (8 tests)
├── test_error_handling.py         # malformed / missing XML (11 tests)
└── test_build_from_dataframe.py   # XML generation + round-trip (10 tests)
```

**Total: 57 tests**

## SparkSession Fixture

Defined once in `conftest.py` and shared across all test files:

```python title="tests/conftest.py"
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("test-spark-xml-etree")
        .master("local[2]")                           # (1)!
        .config("spark.sql.shuffle.partitions", "2")  # (2)!
        .config("spark.ui.enabled", "false")          # (3)!
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")          # (4)!
    yield session
    session.stop()
```

1. Two threads — deterministic and fast.
2. Default 200 is wasteful for test data.
3. Skip the Spark Web UI to speed up fixture creation.
4. Suppress all output except actual errors.

## Test Organisation

Tests are grouped by class, with two kinds per module:

### Pure function tests (no Spark)

Test the Python function directly — fast and isolated:

```python
class TestExtractTitle:
    def test_extracts_title_from_valid_cd(self):
        xml = "<CD><TITLE>Empire Burlesque</TITLE></CD>"
        assert extract_title(xml) == "Empire Burlesque"

    def test_returns_none_when_title_missing(self):
        xml = "<CD><ARTIST>Bob Dylan</ARTIST></CD>"
        assert extract_title(xml) is None
```

### Spark UDF integration tests

Test the full flow through a DataFrame — verifies the UDF registration,
serialization, and Spark execution:

```python
class TestExtractTitle:
    def test_extract_title_udf_across_dataframe(self, spark):
        rows = [Row(index=0, cd="<CD><TITLE>Test</TITLE></CD>")]
        df = spark.createDataFrame(rows)
        extract_title_udf = udf(extract_title, StringType())
        result = (
            df.select(extract_title_udf(F.col("cd")).alias("title"))
            .collect()
        )
        assert result[0]["title"] == "Test"
```

## Assertion Patterns

| Pattern | Example |
|---------|---------|
| Row count | `assert df.count() == 5` |
| Column set | `assert set(df.columns) == {"id", "name"}` |
| Single row | `row = df.filter(F.col("id") == 1).first(); assert row["name"] == "X"` |
| Ordered list | `result = df.orderBy("id").collect(); assert result[0]["x"] == "y"` |
| Set comparison | `names = {r["name"] for r in df.collect()}; assert names == {"A", "B"}` |
| Null check | `assert df.filter(F.col("x").isNull()).count() == 0` |

## Test Coverage by Module

| Module | Test File | Pure | Spark | Total |
|--------|-----------|------|-------|-------|
| `xmls_data_processing` | `test_data_processing.py` | 7 | 4 | 11 |
| `xmls_data_processing_multiple_column` | `test_data_processing.py` | (included above) | — | — |
| `xmls_data_processing_multiple_column2` | `test_attributes_explode.py` | 3 | 5 | 8 |
| `xmls_namespace_handling` | `test_namespace_handling.py` | 6 | 3 | 9 |
| `xmls_nested_flattening` | `test_nested_flattening.py` | 4 | 4 | 8 |
| `xmls_error_handling` | `test_error_handling.py` | 7 | 4 | 11 |
| `xmls_build_from_dataframe` | `test_build_from_dataframe.py` | 4 | 6 | 10 |

## pytest Configuration

Defined in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
pythonpath = ["src"]               # makes `spark_etree` importable
testpaths = ["tests"]              # only collect from tests/
```

## Adding a New Test File

1. Create `tests/test_<topic>.py`.
2. Import the functions under test from `spark_etree.<module>`.
3. Use the `spark` fixture parameter for Spark integration tests.
4. Add the `if __name__` entry point:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```
