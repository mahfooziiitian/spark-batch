# Writing Excel

`ExcelWriter` provides a fluent, immutable API over `pandas.ExcelWriter`.

## Basic write

```python
from pys_excel import ExcelWriter

ExcelWriter(sheet_name="Employees").write(df, "output/employees.xlsx")
```

## Multiple sheets in one workbook

```python
ExcelWriter().write_many(
    {"Employees": employees_df, "Departments": departments_df},
    "output/summary.xlsx",
)
```

## Fluent modifiers

| Method | Purpose |
|--------|---------|
| `with_sheet_name(name)` | Target worksheet name |
| `with_index(bool)` | Include/exclude the pandas row index column |
| `with_engine(name)` | `xlsxwriter` (styling) or `openpyxl` (compatibility) |
| `with_freeze_header(bool)` | Freeze the header row (xlsxwriter only) |
| `with_autofit_columns(bool)` | Auto-size column widths to content (xlsxwriter only) |
| `with_option(key, value)` | Escape hatch for any `to_excel` option |
| `date_format(pattern)` | Output date format string |

```python
(
    ExcelWriter(sheet_name="Report")
    .with_engine("xlsxwriter")
    .with_freeze_header()
    .with_autofit_columns()
    .write(df, "output/report.xlsx")
)
```

!!! note "Formatting requires xlsxwriter"
    Freeze panes and column autofit are implemented via the `xlsxwriter`
    engine's worksheet API. They are silently skipped when using the
    `openpyxl` engine.

## Choosing an engine

- **xlsxwriter** (default) — richer formatting (freeze panes, column widths,
  number formats). Cannot append to an existing file.
- **openpyxl** — broader compatibility, supports reading *and* writing, useful
  when you need to edit an existing template workbook.

See [Formatting](../properties/formatting.md) for more styling examples.
