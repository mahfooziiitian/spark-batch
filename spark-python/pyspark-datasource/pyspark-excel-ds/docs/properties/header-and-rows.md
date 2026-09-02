# Header & Rows

## Header row

```python
ExcelReader(spark).header(0).read("workbook.xlsx")  # first row is the header (default)
ExcelReader(spark).header(None).read("workbook.xlsx")  # headerless sheet -> integer column names
ExcelReader(spark).header(2).read("workbook.xlsx")  # header is the 3rd row (0-based index 2)
```

## Skipping rows

Useful for workbooks with title rows/banners above the real header:

```python
ExcelReader(spark).skiprows(3).header(0).read("workbook.xlsx")
```

## Limiting rows read

```python
ExcelReader(spark).nrows(1000).read("workbook.xlsx")
```

## Headerless sheets with custom column names

```python
(ExcelReader(spark).header(None).names(["emp_id", "name", "salary"]).read("workbook.xlsx"))
```

See `examples/03_properties/01_header_and_skiprows.py` for a runnable version of these patterns.
