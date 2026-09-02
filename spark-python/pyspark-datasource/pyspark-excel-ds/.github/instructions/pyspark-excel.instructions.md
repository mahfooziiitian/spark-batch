---
applyTo: "{src/**/*.py,examples/**/*.py,docs/**/*.md}"
---

# PySpark Excel Datasource Patterns

## PySpark Version

This project targets **PySpark 3.5.x** (`pyspark>=3.5.0,<4.0.0`), pinned
below 4.0 to keep `delta-spark` compatibility stable for the table-integration
workflows. Use standard Spark 3.5 APIs — no Spark Connect / VARIANT-type
assumptions.

## Two Ways to Read/Write Excel

### 1. Pandas bridge (default, no JVM dependency)

Spark has no built-in Excel data source in OSS/vanilla installs below
Databricks Runtime 17.1, so the pandas bridge parses with
`pandas.read_excel`/`ExcelWriter` (openpyxl/xlsxwriter engines) and converts
via `spark.createDataFrame()`/`toPandas()`. Use this for reports, extracts,
and reference data that can be driver-collected.

```python
from pys_excel import ExcelReader, ExcelWriter, get_spark

spark = get_spark("excel-example")
df = ExcelReader(spark).sheet("Employees").header(0).read("employees.xlsx")
ExcelWriter(sheet_name="Report").with_freeze_header().write(df, "report.xlsx")
spark.stop()
```

### 2. Distributed I/O via spark-excel (`pys_excel.spark_excel`)

For cluster-scale workbooks, use the community
[spark-excel](https://github.com/crealytics/spark-excel) connector
(`com.crealytics:spark-excel_2.12:3.5.1_0.20.4` — Spark 3.5.x / Scala 2.12) or
Databricks' built-in `excel` format (DBR 17.1+):

```python
from pys_excel.spark_excel import read_spark_excel, write_spark_excel, resolve_excel_format

fmt = resolve_excel_format()  # auto-picks crealytics vs. native "excel" by DBR version
df = read_spark_excel(spark, "/Volumes/catalog/schema/volume/employees.xlsx", data_address="'Employees'!A1")
write_spark_excel(df, "/Volumes/catalog/schema/volume/report.xlsx", sheet_name="Report")
```

Always call `resolve_excel_format()` rather than hardcoding `"excel"` or
`"com.crealytics.spark.excel"` in reusable code, so behavior adapts correctly
across Databricks Runtime upgrades.

## `_pandas_to_spark` Conversion Rules

`reader/_reader.py::_pandas_to_spark` is the single conversion chokepoint —
any change here must preserve these invariants:

1. **Never** pass a `StructType` directly to `spark.createDataFrame(pdf, schema=...)`.
   Create the DataFrame first with inferred types, then
   `.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])`
   to coerce. Direct schema-application raises `PySparkTypeError` when pandas
   infers a whole-number column as `int64` but the target field is `DoubleType`.
2. **Never** blanket `pdf.astype(object).where(pd.notnull(pdf), None)` before
   `createDataFrame` — this breaks `datetime64` columns, producing an empty
   struct type and a Parquet write failure
   (`InvalidSchemaException: Cannot write a schema with an empty group`).
   Let `spark.createDataFrame(pdf)` handle NaN/NaT-to-null mapping natively.
3. Normalize float/double NaN to SQL `null` explicitly via
   `F.when(F.isnan(f.name), None).otherwise(F.col(f.name))` — pandas NaN does
   not always convert cleanly to `NULL` for numeric columns otherwise.

## Table Integration (`table/_table.py`)

The primary "Data Architect" workflow — read Excel, write into a governed
table:

```python
from pys_excel import excel_to_table, table_to_excel, upsert_table_from_excel

# Full refresh load
excel_to_table(spark, "employees.xlsx", "sales.employees", sheet_name="Employees", file_format="delta")

# Incremental MERGE INTO upsert (requires Delta)
upsert_table_from_excel(spark, "updates.xlsx", "sales.employees", key_columns=["emp_id"])

# Table/query -> Excel export
table_to_excel(spark, "sales.employees", "reports/employees.xlsx")
```

- Default `file_format` is `"delta"`. Delta is an **optional extra**
  (`uv sync --extra delta`) locally, but built into every Databricks Runtime.
- `upsert_table_from_excel` creates the table on first run
  (`saveAsTable`) and only issues `MERGE INTO` once the table exists.
- Gate any new Delta-dependent code behind
  `importlib.util.find_spec("delta")` so it degrades gracefully when the
  extra isn't installed (see `config.get_spark(enable_delta=...)`).

## Databricks Runtime Guidance

| DBR | Excel support | Format to use |
|---|---|---|
| 13.3 LTS – 16.x | Community library only | `com.crealytics.spark.excel` (attach `com.crealytics:spark-excel_2.12:3.5.1_0.20.4` as a cluster Maven library) |
| 17.1+ | Built-in | `excel` — no library install |

`is_databricks_runtime()` reads `DATABRICKS_RUNTIME_VERSION`;
`resolve_excel_format()` uses it to pick the right connector automatically.

## Library Usage (`src/pys_excel`)

Prefer the library wrappers over raw pandas/PySpark boilerplate in examples
and any reusable code:

```python
from pys_excel import ExcelReader, ExcelWriter, create_spark_session

spark = create_spark_session("my-job")
reader = ExcelReader(spark).sheet("Employees").header(0)
df = reader.read("/data/employees.xlsx")
spark.stop()
```
