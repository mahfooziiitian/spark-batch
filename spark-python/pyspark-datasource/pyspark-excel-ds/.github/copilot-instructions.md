# PySpark Excel Datasource — Copilot Instructions

## Project Overview

`pyspark-excel-ds` is a reusable library, example suite, and documentation
site for reading Excel workbooks into Spark, writing Spark data back to
Excel, and loading/upserting Excel extracts into governed Spark tables
(Delta or Parquet). It supports two complementary approaches:

1. **Pandas bridge** (`ExcelReader`/`ExcelWriter`) — driver-collected,
   dependency-free, works on any Spark install.
2. **Distributed I/O** (`pys_excel.spark_excel`) — the community
   [spark-excel](https://github.com/crealytics/spark-excel) connector (or
   Databricks' built-in `excel` format on DBR 17.1+) for cluster-scale
   workbooks.

## Tech Stack

- **Python** ≥ 3.11
- **PySpark** `>=3.5.0,<4.0.0` (kept on the 3.5.x line for stable Delta Lake
  compatibility)
- **pandas** + **openpyxl** (read) + **xlsxwriter** (formatted write) — the
  pandas bridge
- **spark-excel** (`com.crealytics:spark-excel_2.12:3.5.1_0.20.4`) — the
  distributed connector, for Spark 3.5.x / Scala 2.12 (matches Databricks
  Runtime 13.3 LTS–16.x)
- **delta-spark** (optional extra, `pip install ".[delta]"`) — Delta table
  support for `excel_to_table`/`upsert_table_from_excel`
- **Build**: hatchling (`pyproject.toml`, `hatchling.build`)
- **Package manager**: `uv` (`uv sync --group dev`, `uv sync --extra delta`)
- **Testing**: pytest with `pythonpath = ["src"]` and `testpaths = ["tests"]`
- **Lint/format/type-check/security**: ruff, mypy, bandit
- **Docs**: MkDocs Material
- **Logging**: Rich-powered logger in `pys_excel._logging`

## Source Structure

```
src/pys_excel/
├── _logging.py          # Rich-powered logging (get_logger, print_* helpers)
├── config.py             # get_spark(), env config, sample workbook generator, path helpers
├── session.py             # create_spark_session() for library/test use
├── reader/
│   └── _reader.py         # ExcelReader — fluent pandas.read_excel wrapper
├── writer/
│   └── _writer.py         # ExcelWriter — fluent pandas.ExcelWriter wrapper
├── table/
│   └── _table.py          # excel_to_table, table_to_excel, upsert_table_from_excel
└── spark_excel.py         # Distributed I/O via crealytics spark-excel / Databricks native format

examples/
├── 01_data_source/        # Basic read/write, all-sheets, distributed I/O
├── 02_table_integration/   # Excel<->table, Delta MERGE upsert
├── 03_properties/          # Header/skiprows, sheet selection, NA/dtypes, formatting
├── 04_schema/               # Explicit schema vs. inference
└── 05_error_handling/       # Missing file, malformed rows

tests/
├── reader/, writer/, table/, spark_excel/

docs/                      # MkDocs Material site (see mkdocs.yml nav for full structure)
```

## Modular Instruction Files

| File | Scope (`applyTo`) | Purpose |
|------|--------------------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python style, imports, type hints |
| `instructions/pyspark-excel.instructions.md` | `src/**/*.py` | Excel reader/writer/table/spark_excel patterns |
| `instructions/testing.instructions.md` | `tests/**/*.py` | pytest conventions, SparkSession fixtures |
| `instructions/examples.instructions.md` | `examples/**/*.py` | Example script structure and conventions |
| `instructions/project-config.instructions.md` | `pyproject.toml` | Package/build/dependency configuration |
| `instructions/mkdocs.instructions.md` | `mkdocs.yml`, `docs/**/*.md` | Documentation style and structure |
| `instructions/logging-rich.instructions.md` | `src/pys_excel/_logging.py`, files using it | Rich logging conventions |

## Things to Avoid

- Do not add Excel-parsing logic that depends on a JVM package for the basic
  pandas-bridge path (`ExcelReader`/`ExcelWriter`) — it must keep working
  with zero extra JARs.
- Do not make `delta-spark` a hard/default dependency — it is an optional
  extra (`uv sync --extra delta`) so basic usage doesn't require network
  access to resolve Maven packages.
- Do not pass a `StructType` directly to `spark.createDataFrame(pdf, schema=...)`
  for pandas-sourced data — infer first, then `.cast()` per-column (see
  `reader/_reader.py::_pandas_to_spark`) to avoid numeric type mismatches.
- Do not blanket `astype(object)` a pandas DataFrame before
  `spark.createDataFrame()` — it breaks datetime64 → `TimestampType`
  inference. Let pandas dtypes flow through per-column.
- Do not hardcode `com.crealytics:spark-excel` version literals outside
  `spark_excel.py` — reference `SPARK_EXCEL_PACKAGE_SCALA_2_12`.
- Do not assume the native `excel` format is available — it requires
  Databricks Runtime 17.1+. Use `resolve_excel_format()` rather than
  hardcoding `"excel"` in reusable code.
- Do not skip `spark.stop()` in standalone example scripts.
- Do not use `from pyspark.sql.functions import *` — always
  `from pyspark.sql import functions as F`.
