---
applyTo: "tests/**/*.py"
---

# Pytest Conventions

## Directory Layout

```
tests/
├── __init__.py
└── xml/
    ├── __init__.py
    └── test_<feature>.py
```

- All test files live under `tests/xml/`.
- File names must start with `test_`.
- Every directory has an `__init__.py`.

## SparkSession Fixture

Use a **session-scoped** fixture shared across all tests in a file. Starting
Spark is expensive — never create a new session per test:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("XML Processing Test")
        .getOrCreate()
    )
    yield spark
    spark.stop()
```

## Writing Tests

### Naming

- Name test functions `test_<what_it_verifies>`:
  ```python
  def test_xpath_string_extracts_header_fields(spark): ...
  def test_xpath_boolean_evaluates_condition(spark): ...
  ```
- Add a one-line **Google-style docstring** describing the behaviour under test:
  ```python
  def test_xpath_boolean(spark):
      """xpath_boolean returns True when the score condition is met."""
  ```

### Structure (Arrange → Act → Assert)

```python
def test_extract_title(spark):
    """xpath_string extracts the book title."""
    # Arrange — build inline XML DataFrame
    data = ["<book><title>PySpark</title></book>"]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_books")

    # Act — run XPath query
    result = spark.sql(
        "SELECT xpath_string(data, 'book/title') AS t FROM test_books"
    )

    # Assert — check collected values
    assert result.collect()[0].t == "PySpark"
```

### Temp View Naming

Use a **unique temp view name per test** to avoid cross-test interference:

```python
# ✅ Good — prefixed with "test_"
df.createOrReplaceTempView("test_xml_header")
df.createOrReplaceTempView("test_xml_bool")

# ❌ Bad — generic name, may collide
df.createOrReplaceTempView("xml_data")
```

### Data

- Use **inline XML strings** — avoid external file dependencies in tests.
- Keep XML minimal — include only the elements needed for the assertion.
- Test **edge cases**: empty XML, missing elements, multiple rows, namespaces.

## Assertions

- Prefer direct value comparison over count-based checks:
  ```python
  # ✅ Good
  assert rows[0].tag1 == "expected_value"

  # ❌ Weak — passes even if values are wrong
  assert result.count() > 0
  ```
- For array results, compare as Python lists:
  ```python
  assert rows[0].items == ["b1", "b2", "b3"]
  ```
- For multi-row results, sort before comparing:
  ```python
  tags = sorted([row.tag1 for row in result.collect()])
  assert tags == ["row1", "row2", "row3"]
  ```

## Running Tests

```bash
uv run pytest tests/ -v              # full suite, verbose
uv run pytest tests/ -v --tb=short   # short tracebacks
uv run pytest tests/xml/test_xml_array_xpath.py -v   # single file
uv run pytest tests/ -k "boolean"    # filter by keyword
```

## Dependencies

Defined in `pyproject.toml` under `[dependency-groups] dev`:

| Package | Purpose |
| --- | --- |
| `pytest` | Test runner (transitive via pytest-sugar) |
| `pytest-mock` | `mocker` fixture for mocking |
| `pytest-sugar` | Pretty progress bar output |
