---
applyTo: "{**/test_*.py,**/*_test.py}"
---

# Testing Conventions

## Framework

- Use **pytest** as the test runner.
- Tests live under `tests/` mirroring `src/pys_excel/` structure:
  `tests/reader/`, `tests/writer/`, `tests/table/`, `tests/spark_excel/`.
- Test file names follow the pattern `test_*.py`.

## SparkSession Fixture

A session-scoped fixture lives in `tests/conftest.py` with Hive support
enabled (needed for `saveAsTable`):

```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("pys-excel-tests")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .enableHiveSupport()
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

## Delta-Gated Tests

Table tests default to `file_format="parquet"` to avoid requiring the Delta
JAR in the base session. Delta-specific tests (e.g. `upsert_table_from_excel`)
must build their own `delta_spark` fixture and skip gracefully when
`delta-spark` isn't installed:

```python
import importlib.util

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("delta") is None,
    reason="delta-spark not installed (optional extra: uv sync --extra delta)",
)


@pytest.fixture(scope="module")
def delta_spark():
    from pys_excel import get_spark

    session = get_spark("excel-delta-tests", enable_delta=True)
    yield session
    session.stop()
```

## Writing Tests

### Round-trip read/write

Use pytest's `tmp_path` fixture for isolated temp files — never write to
fixed paths.

```python
from pathlib import Path

from pyspark.sql import SparkSession

from pys_excel import ExcelReader, ExcelWriter, generate_sample_workbook


def test_read_basic(spark: SparkSession, tmp_path: Path) -> None:
    workbook = generate_sample_workbook(tmp_path / "employees.xlsx")
    df = ExcelReader(spark).sheet("Employees").read(str(workbook))

    assert df.count() > 0
    assert "emp_id" in df.columns
```

### Schema assertions

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def test_explicit_schema(spark: SparkSession, tmp_path: Path) -> None:
    schema = StructType(
        [
            StructField("name", StringType(), nullable=True),
            StructField("salary", DoubleType(), nullable=True),
        ]
    )
    df = ExcelReader(spark).with_schema(schema).read(str(workbook))
    assert df.schema == schema
```

### Testing spark_excel (no JVM/network required)

`tests/spark_excel/test_spark_excel.py` covers pure logic — format resolution
and Databricks runtime detection — without requiring the actual Maven package
or a live cluster:

```python
import os

from pys_excel.spark_excel import resolve_excel_format, is_databricks_runtime


def test_resolve_excel_format_local(monkeypatch) -> None:
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert resolve_excel_format() == "com.crealytics.spark.excel"


def test_resolve_excel_format_dbr17(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "17.1")
    assert resolve_excel_format() == "excel"
```

## Guidelines

- Keep tests focused — one behavior per test function.
- Use `tmp_path` (function-scoped) for all file I/O.
- Name test functions descriptively: `test_<feature>_<scenario>`.
- Assert on both schema structure and data content when testing read/write.
- Test both library code (`src/pys_excel`) and any new example utility
  functions.

## Running Tests

```bash
uv run pytest                        # from project root
uv run pytest -v                     # verbose
uv run pytest tests/reader/          # a specific test dir
uv run pytest -k "schema"            # tests matching a pattern
uv sync --extra delta && uv run pytest tests/table/  # include Delta-gated tests
```
