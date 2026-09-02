# Best Practices

## Choosing pandas bridge vs. spark-excel

- Use `ExcelReader`/`ExcelWriter` (pandas bridge) for reports, extracts, and
  reference data collected on the driver — no JVM dependency, works anywhere.
- Use `pys_excel.spark_excel` (crealytics connector / Databricks native) for
  large workbooks or cluster-scale ingestion where driver-side collection
  would be a bottleneck. See [spark-excel Library](data-source/spark-excel-library.md).

## Schema

- Prefer an [explicit schema](schema/explicit-schema.md) for anything feeding
  a production table. Excel source data is prone to silent type drift.
- Validate post-read with Spark filters rather than assuming clean input —
  see [Malformed Rows](error-handling/malformed-rows.md).

## Table format

- Default to **Delta** (`file_format="delta"`) for governed tables — ACID,
  schema evolution, `MERGE INTO` support. Built into every Databricks
  Runtime; install the `delta` extra locally (`uv sync --extra delta`).
- Use `upsert_table_from_excel` for recurring/incremental loads instead of
  `mode="overwrite"` full refreshes, once you have a stable business key.

## Databricks

- On **DBR 15.x/16.x**, attach `com.crealytics:spark-excel_2.12:3.5.1_0.20.4`
  as a cluster Maven library.
- On **DBR 17.1+**, prefer the built-in `excel` format — no library install.
- Let `resolve_excel_format()` pick the right format automatically so code
  survives a runtime upgrade unchanged. See [Databricks](databricks-runtime/index.md).
- Use Unity Catalog Volumes paths for both source workbooks and Excel
  exports, not DBFS root or local driver paths.

## Performance

- Excel is a row-oriented, driver-collected format in the pandas bridge path
  — keep individual workbook reads to a reasonable size (tens of thousands
  of rows, not millions). For larger files use `spark_excel` instead.
- Use `usecols()`/`nrows()` to avoid parsing unused columns/rows when only a
  subset of a large sheet is needed.

## Testing

- Gate Delta-dependent tests/examples behind
  `importlib.util.find_spec("delta")` so the base test suite runs without the
  optional dependency installed (see `tests/table/test_table.py`).
- Use `generate_sample_workbook()` in tests/examples instead of committing
  binary `.xlsx` fixtures to the repository.

## Logging

- Use `pys_excel._logging.get_logger(name)` (Rich-powered) for consistent,
  readable console output instead of `print()`.
- Set `PYS_EXCEL_LOG_LEVEL=DEBUG` for verbose troubleshooting during local
  development.
