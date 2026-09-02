# Malformed Rows

Business-authored Excel extracts often contain blank cells, invalid values,
or type-inconsistent columns. Rather than failing the whole read, quarantine
bad rows with Spark expressions after loading.

```python
from pyspark.sql import functions as F
from pys_excel import ExcelReader

df = ExcelReader(spark).read("employees.xlsx")

is_valid = F.col("emp_id").isNotNull() & F.col("name").isNotNull() & (F.col("salary") >= 0)

valid_rows = df.filter(is_valid)
quarantined_rows = df.filter(~is_valid)

if quarantined_rows.count() > 0:
    quarantined_rows.write.mode("append").saveAsTable("sales.employees_quarantine")

valid_rows.write.mode("overwrite").saveAsTable("sales.employees")
```

## Combining with explicit schema casting

An [explicit schema](../schema/explicit-schema.md) coerces incompatible
values to `null` via `cast()` rather than raising — pair it with an
`isNotNull()` validity check to catch those coerced nulls:

```python
df = ExcelReader(spark).with_schema("emp_id INT, name STRING, salary DOUBLE").read("employees.xlsx")
invalid_salary = df.filter(F.col("salary").isNull())
```

See `examples/05_error_handling/02_malformed_rows.py`.
