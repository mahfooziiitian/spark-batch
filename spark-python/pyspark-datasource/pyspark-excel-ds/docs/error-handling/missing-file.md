# Missing File

Reads propagate the underlying pandas/openpyxl exceptions unchanged, so
catch them explicitly rather than relying on `pys_excel` to wrap them.

## Nonexistent file path

```python
from pys_excel import ExcelReader

try:
    ExcelReader(spark).read("/data/does_not_exist.xlsx")
except FileNotFoundError as exc:
    print(f"Expected failure: {exc}")
```

## Nonexistent sheet name

```python
try:
    ExcelReader(spark).sheet("DoesNotExist").read("workbook.xlsx")
except ValueError as exc:
    print(f"Expected failure: {exc}")
```

## Recommended pattern for scheduled ingestion

```python
from pys_excel import excel_to_table

try:
    excel_to_table(spark, incoming_path, "sales.employees", sheet_name="Employees")
except FileNotFoundError:
    # e.g. alert, skip this run, or fall back to the previous extract
    logger.warning("No new extract found at %s; skipping this run", incoming_path)
```

See `examples/05_error_handling/01_missing_file.py`.
