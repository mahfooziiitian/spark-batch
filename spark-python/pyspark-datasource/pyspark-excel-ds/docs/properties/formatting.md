# Formatting

`ExcelWriter`'s `xlsxwriter` engine (default) supports styling the output
workbook.

## Freeze the header row

```python
ExcelWriter(sheet_name="Report").with_freeze_header().write(df, "report.xlsx")
```

## Auto-fit column widths

```python
ExcelWriter(sheet_name="Report").with_autofit_columns().write(df, "report.xlsx")
```

## Combine both

```python
(ExcelWriter(sheet_name="Report").with_freeze_header().with_autofit_columns().write(df, "report.xlsx"))
```

## Custom date format

```python
ExcelWriter(sheet_name="Report").date_format("yyyy-mm-dd").write(df, "report.xlsx")
```

## Excluding the pandas index column

```python
ExcelWriter(sheet_name="Report").with_index(False).write(df, "report.xlsx")  # default
ExcelWriter(sheet_name="Report").with_index(True).write(df, "report.xlsx")  # include row index
```

!!! warning "openpyxl engine"
    `with_freeze_header()` and `with_autofit_columns()` only take effect with
    the `xlsxwriter` engine (the default). They are silently ignored under
    `with_engine("openpyxl")`.

See `examples/03_properties/04_formatting_and_styles.py`.
