# Schema Inference

Without an explicit schema, `ExcelReader` lets pandas infer column types from
cell contents, then bridges to Spark:

```python
df = ExcelReader(spark).sheet("Employees").read("employees.xlsx")
df.printSchema()
```

## What gets inferred

- Whole-number columns → Spark `LongType`
- Decimal columns → Spark `DoubleType`
- Text columns → Spark `StringType`
- Excel date/datetime cells → Spark `TimestampType`
- Blank cells → `null` (see [NA Values & Types](../properties/na-values-and-types.md))

## Nudging inference with pandas options

```python
ExcelReader(spark).dtype({"zip_code": str}).read("employees.xlsx")  # force a column to stay text
ExcelReader(spark).na_values(["N/A", "--"]).read("employees.xlsx")  # recognize custom blanks
```

## Risks of pure inference

Because inference runs per read, a workbook with a single stray text value in
an otherwise-numeric column (e.g. `"N/A"` in a salary column not covered by
`na_values`) can silently produce a `StringType` column instead of
`DoubleType`. For anything feeding a production table, prefer
[Explicit Schema](explicit-schema.md).

See `examples/04_schema/02_schema_inference.py`.
