# upsert_table_from_excel (MERGE INTO)

Incrementally merge an Excel extract into an existing **Delta** table by
business key — updating matched rows and inserting new ones, instead of
overwriting the whole table.

```python
from pys_excel import upsert_table_from_excel

upsert_table_from_excel(
    spark,
    "employee_updates.xlsx",
    "sales.employees",
    key_columns=["emp_id"],
    sheet_name="Employees",
)
```

## Behavior

- If `table_name` **does not exist yet**, it is created from the Excel data
  (`saveAsTable`, `format("delta")`) — no MERGE is attempted on the first run.
- If the table **exists**, rows are merged via:

```sql
MERGE INTO sales.employees AS target
USING _pys_excel_upsert_source AS source
ON target.emp_id = source.emp_id
WHEN MATCHED THEN UPDATE SET target.name = source.name, target.salary = source.salary, ...
WHEN NOT MATCHED THEN INSERT (emp_id, name, salary, ...) VALUES (source.emp_id, source.name, source.salary, ...)
```

## Parameters

| Parameter | Description |
|-----------|--------------|
| `key_columns` | Column names that uniquely identify a row — used as the MERGE `ON` clause |
| `sheet_name` | Sheet name or zero-based index to read (default `0`) |
| `reader_options` | Additional `pandas.read_excel` options |

## Requirements

- Requires **Delta Lake**. Locally, install the optional extra:
  `uv sync --extra delta`. On Databricks, Delta is built in — no extra
  install needed.
- `key_columns` must uniquely identify rows in the source sheet; duplicate
  keys will produce a `MERGE` failure ("multiple source rows matched").

!!! tip "Recurring loads"
    This is the recommended pattern for scheduled/recurring Excel drops
    (e.g. a nightly HR export) where you want to preserve existing rows and
    only update what changed, rather than re-running `excel_to_table` with
    `mode="overwrite"` each time.
