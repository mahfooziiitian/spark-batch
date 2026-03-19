---
applyTo: "tests/**/*.py"
---

# Testing Conventions

## Framework

- pytest ≥ 8.0 (dev dependency).
- Run with `uv run pytest` or `uv run pytest tests/ -v --tb=short`.

## Directory Layout

```
tests/
├── __init__.py
├── conftest.py                   # session-scoped SparkSession fixture
├── test_data_processing.py       # single-field + multi-field extraction
├── test_attributes_explode.py    # attributes + array explode
├── test_namespace_handling.py    # XML namespaces
├── test_nested_flattening.py     # order → line items
├── test_error_handling.py        # malformed / missing XML
└── test_build_from_dataframe.py  # XML generation + round-trip
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
        .appName("test-spark-xml-etree")
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

Group tests into classes by capability area. Each class tests one module:

```python
class TestExtractTitle:
    """Tests for xmls_data_processing.extract_title."""

    def test_extracts_title_from_valid_cd(self):
        ...

    def test_returns_none_when_title_missing(self):
        ...

    def test_extract_title_udf_across_dataframe(self, spark):
        ...
```

## Two Kinds of Tests per Module

1. **Pure function tests** — test the Python function directly, no Spark needed:

```python
def test_extracts_title_from_valid_cd(self):
    xml = "<CD><TITLE>Empire Burlesque</TITLE></CD>"
    assert extract_title(xml) == "Empire Burlesque"
```

2. **Spark UDF integration tests** — run through the UDF in a DataFrame:

```python
def test_extract_title_udf_across_dataframe(self, spark):
    rows = [Row(index=0, cd="<CD><TITLE>Test</TITLE></CD>")]
    df = spark.createDataFrame(rows)
    extract_title_udf = udf(extract_title, StringType())
    result = df.select(extract_title_udf(F.col("cd")).alias("title")).collect()
    assert result[0]["title"] == "Test"
```

## Assertions

Prefer `df.count()` over `len(df.collect())`:

```python
assert df.count() == 5
assert set(df.columns) == {"id", "name", "score"}
```

For single-row assertions, collect minimally:

```python
row = df.filter(F.col("id") == 1).first()
assert row["name"] == "Widget"
```

For ordered multi-row assertions:

```python
result = df.orderBy("index").collect()
assert result[0]["title"] == "Empire Burlesque"
assert result[4]["title"] == "Eros"
```

For set-based comparisons where order doesn't matter:

```python
names = {r["name"] for r in df.select("name").collect()}
assert names == {"Alice", "Bob", "Charlie"}
```

## Imports from Source

Import functions, constants, and schemas from the source modules:

```python
from spark_etree.xmls_data_processing import SAMPLE_XML, extract_title
from spark_etree.xmls_error_handling import PRODUCT_SCHEMA, safe_parse_product
```

The `pythonpath = ["src"]` setting in `pyproject.toml` makes `spark_etree`
importable without installation.

## Test Data

Use inline XML strings in tests — small, deterministic, and self-contained:

```python
xml = '<product sku="P1"><name>Widget</name><price>19.99</price></product>'
```

For integration tests, reuse the module's `SAMPLE_XML` or `SAMPLE_DATA` constants.

## Naming Conventions

- Test files: `test_<module_topic>.py`
- Test classes: `Test<CapabilityArea>`
- Test methods: `test_<what_it_verifies>`

## Entry Point

Every test file includes a direct-run entry point:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```
