---
applyTo: "{**/test_*.py,**/*_test.py}"
---

# Testing — pytest + PySpark

## Running Tests

```bash
make test-fast             # stop at first failure (quiet)
make test-cov              # with coverage gate
uv run pytest -k "scd"     # filter by name
uv run pytest -m unit      # filter by marker
```

## SparkSession Fixture

Session-scoped in `tests/conftest.py` — never create a SparkSession per test.
The fixture is tuned for **fast, artifact-free** local runs: it uses the
in-memory catalog (no Derby metastore, so no `derby.log`/`metastore_db`),
a single shuffle partition, and AQE disabled for tiny test data.

```python
@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-session")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="spark-test-"))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

> If a test intentionally needs a Hive-style catalog, override
> `spark.sql.catalogImplementation`; `make clean` removes any stray
> `spark-warehouse`/`metastore_db`/`derby.log` artifacts.

## Test Structure

- Files: `test_*.py`. Functions: `test_*`. Group related tests in classes.
- Tests mirror the `sql/` topic tree (e.g. `tests/scd/` ↔ `sql/scd/`).

## Assertions

```python
from chispa.dataframe_comparer import assert_df_equality

assert_df_equality(actual, expected, ignore_row_order=True)
```

- Use `df.count()` for row counts.
- Use `df.filter(...).first()` for single-row value checks.
- Check schema types: `result.schema["col"].dataType == StringType()`.

## SCD Test Cases (mandatory)

Every SCD implementation must test:
1. New record inserted
2. Changed record versioned/updated
3. Unchanged record skipped
4. Idempotent on rerun
5. No duplicate active rows

## Mocking

Use the **`pytest-mock`** `mocker` fixture — **never** import `unittest.mock`
(`patch`, `MagicMock`, `mock.patch`) or add `@patch` decorators directly.
`mocker` auto-undoes every patch at test teardown, so there is no decorator
stacking or `with` nesting.

```python
def test_lists_catalogs(mocker):
    # patch where the name is *used*, not where it's defined
    client = mocker.patch("spark_sql.dbx_mcp.tools.catalogs.get_workspace_client")
    catalog = mocker.Mock(comment=None)
    catalog.name = "main"  # set separately: `name=` is reserved by Mock
    client.return_value.catalogs.list.return_value = [catalog]

    result = list_catalogs()

    client.return_value.catalogs.list.assert_called_once()
    assert result[0]["name"] == "main"
```

- `mocker.patch("module.path")` instead of `@patch("module.path")`.
- `mocker.Mock()` / `mocker.MagicMock()` instead of `unittest.mock.Mock()`.
- `mocker.patch.object(obj, "attr")`, `mocker.spy(obj, "method")`,
  `mocker.patch.dict(...)` for their `unittest.mock` equivalents.
- Prefer `monkeypatch` for environment variables (`monkeypatch.setenv(...)`);
  use `mocker` for objects, methods, and clients (e.g. patch `get_workspace_client`
  so `dbx_mcp` tool tests need no live Databricks workspace).
- Don't mock Spark itself — use the session-scoped `spark` fixture and assert on
  real DataFrames.

## Rules

- No `df.show()` or `df.printSchema()` in tests.
- No disk writes in unit tests — assert in memory.
- Use `tmp_path` for I/O tests.
- Coverage minimum: 60% (scoped — see [quality.instructions.md](quality.instructions.md)).

## Markers

Declared in `pyproject.toml`: `smoke`, `unit`, `integration`, `slow`, `spark`.

```bash
uv run pytest -m "not slow"
```
