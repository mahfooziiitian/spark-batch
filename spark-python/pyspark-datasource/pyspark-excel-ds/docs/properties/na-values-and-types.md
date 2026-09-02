# NA Values & Types

## Custom NA/NaN strings

Excel extracts often use non-standard missing-value markers:

```python
ExcelReader(spark).na_values(["N/A", "--", "unknown"]).read("workbook.xlsx")
```

## Disabling default NA recognition

```python
ExcelReader(spark).keep_default_na(False).na_values(["MISSING"]).read("workbook.xlsx")
```

## Forcing pandas dtypes before the Spark conversion

Useful when a column that should stay textual (e.g. zip codes, IDs with
leading zeros) would otherwise be inferred as numeric:

```python
ExcelReader(spark).dtype({"zip_code": str, "emp_id": str}).read("workbook.xlsx")
```

## Null handling in the Spark conversion

`_pandas_to_spark` (used internally by every `read()`/`read_all_sheets()` call)
normalizes:

- pandas `NaT` in datetime columns → Spark `null` `TimestampType`
- pandas `NaN` in float/double columns → Spark `null` (not `NaN`) via
  `when(isnan(...), None)`
- An explicit `schema` is applied via `cast()` **after** initial inference, so
  whole-number columns can be safely cast to `DoubleType`, etc.

See `examples/03_properties/03_na_values_and_dtypes.py`.
