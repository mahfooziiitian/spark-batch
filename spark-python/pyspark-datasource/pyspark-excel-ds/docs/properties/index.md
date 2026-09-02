# Properties & Options

`ExcelReader` and `ExcelWriter` expose the most common `pandas.read_excel` /
`ExcelWriter` options as typed fluent methods, plus an escape hatch
(`with_option`/`with_options`) for anything else.

| Topic | Page |
|-------|------|
| Header rows, skipping rows, row limits | [Header & Rows](header-and-rows.md) |
| Sheet selection, column selection/renaming | [Sheets & Columns](sheets-and-columns.md) |
| NA values, dtypes | [NA Values & Types](na-values-and-types.md) |
| Freeze panes, autofit, date formats | [Formatting](formatting.md) |

All modifier methods return a **new** `ExcelReader`/`ExcelWriter` instance —
both classes are immutable dataclasses, so chaining is safe to share/reuse:

```python
base = ExcelReader(spark).header(0).engine("openpyxl")
employees = base.sheet("Employees").read("workbook.xlsx")
departments = base.sheet("Departments").read("workbook.xlsx")
```
