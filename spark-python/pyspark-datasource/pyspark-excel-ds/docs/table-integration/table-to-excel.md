# table_to_excel

Export a Spark table — or an arbitrary SQL query — to an Excel workbook.

```python
from pys_excel import table_to_excel

table_to_excel(spark, "sales.employees", "reports/employees.xlsx")
```

## Parameters

| Parameter | Default | Description |
|-----------|---------|--------------|
| `sheet_name` | `"Sheet1"` | Worksheet name to write |
| `query` | `None` | Optional SQL query to run instead of `SELECT * FROM table_name` |
| `writer_options` | `None` | Forwarded to `ExcelWriter` fluent modifiers (`index`, `engine`, `freeze_header`, `autofit_columns`, ...) |

## Exporting a filtered/aggregated report

```python
table_to_excel(
    spark,
    table_name="sales.employees",  # ignored when query is given
    path="reports/high_earners.xlsx",
    sheet_name="High Earners",
    query="""
        SELECT name, department, salary
        FROM sales.employees
        WHERE salary > 100000
        ORDER BY salary DESC
    """,
    writer_options={"freeze_header": True, "autofit_columns": True},
)
```

`writer_options` keys are mapped to `ExcelWriter.with_<key>(...)` when such a
method exists (e.g. `freeze_header` → `with_freeze_header`), otherwise they
fall back to a raw `with_option(key, value)`.
