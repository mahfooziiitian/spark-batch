# Sheets & Columns

## Selecting a sheet

```python
ExcelReader(spark).sheet("Employees").read("workbook.xlsx")
ExcelReader(spark).sheet(0).read("workbook.xlsx")  # zero-based index
```

## Reading every sheet at once

```python
sheets = ExcelReader(spark).read_all_sheets("workbook.xlsx")
# {"Employees": DataFrame, "Departments": DataFrame}
for name, df in sheets.items():
    df.write.format("delta").mode("overwrite").saveAsTable(f"sales.{name.lower()}")
```

## Restricting/selecting columns

```python
ExcelReader(spark).usecols("A:C").read("workbook.xlsx")  # Excel-style range
ExcelReader(spark).usecols(["emp_id", "name", "salary"]).read("workbook.xlsx")  # by name
```

## Writing multiple sheets to one workbook

```python
from pys_excel import ExcelWriter

ExcelWriter().write_many(
    {"Employees": employees_df, "Departments": departments_df},
    "output/summary.xlsx",
)
```

See `examples/03_properties/02_sheet_selection.py`.
