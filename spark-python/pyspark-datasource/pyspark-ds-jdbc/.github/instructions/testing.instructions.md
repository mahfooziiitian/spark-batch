---
applyTo: "tests/**/*.py"
---

# Testing Conventions

## Framework

- Use **pytest** (^8.2.2) as the test framework.
- Test discovery paths: `tests/` and `integration/` (configured in `pyproject.toml`).
- Source modules are importable via `pythonpath = ["src"]` in pytest config.

## Test File Organization

- Mirror the `src/` directory structure under `tests/`:
  ```
  tests/
  ├── reader/
  │   └── test_jdbc_reader.py
  ├── writer/
  │   └── test_jdbc_writer.py
  ├── utils/
  │   └── test_config_reader.py
  └── ...
  ```
- Name test files with the `test_` prefix (e.g., `test_jdbc_reader.py`).
- Name test functions with the `test_` prefix (e.g., `test_read_from_oracle()`).

## SparkSession Fixture

- Create a shared SparkSession fixture scoped to the test session or module to avoid creating/destroying Spark contexts per test:
  ```python
  import pytest
  from utils.spark_util import get_spark_session

  @pytest.fixture(scope="session")
  def spark():
      configs = {
          "spark.app.name": "TestApp",
          "spark.master": "local[*]",
      }
      session = get_spark_session(configs)
      yield session
      session.stop()
  ```
- Use the `spark` fixture in tests that need a SparkSession.
- Place shared fixtures in `conftest.py` at the `tests/` root.

## Mocking Database Connections

- Use `unittest.mock.patch` or `pytest-mock` to mock JDBC calls — tests should not require a running database.
- Mock `ConfigReader` to return test credentials:
  ```python
  from unittest.mock import MagicMock

  def mock_config_reader():
      reader = MagicMock()
      reader.get_user.return_value = "test_user"
      reader.get_password.return_value = "test_pass"
      reader.get_driver.return_value = "com.mysql.cj.jdbc.Driver"
      reader.get_url.return_value = "jdbc:mysql://localhost:3306/testdb"
      return reader
  ```
- Mock `spark.read.jdbc()` and `df.write.jdbc()` to verify call arguments without actual database I/O.

## Testing JDBC Read/Write Logic

- **Read tests:** Verify that the correct JDBC URL, table, and properties are passed to `spark.read.jdbc()`.
- **Write tests:** Verify write mode (`overwrite` / `append`), properties (`batchsize`, `isolationLevel`), and target table.
- **Deduplication tests:** Create a test DataFrame with duplicates, apply the Window rank pattern, and assert the output contains only expected rows.
- **CTE query tests:** Verify that `prepareQuery` and `query` options are set correctly.

## Test Data

- Use `spark.createDataFrame()` to create small in-memory DataFrames for testing:
  ```python
  def test_dedup(spark):
      data = [(1, "A", 10), (1, "A", 20), (2, "B", 30)]
      df = spark.createDataFrame(data, ["job_id", "name", "value"])
      # Apply dedup logic and assert
  ```

## Assertions

- Use plain `assert` statements (pytest style), not `unittest.TestCase` methods.
- For DataFrame comparisons, collect to a list and compare:
  ```python
  result = df.collect()
  assert len(result) == expected_count
  assert result[0]["column"] == expected_value
  ```

## Running Tests

```bash
# Run all tests
poetry run pytest

# Run specific test file
poetry run pytest tests/reader/test_jdbc_reader.py

# Run with verbose output
poetry run pytest -v

# Run matching test name pattern
poetry run pytest -k "test_read"
```
