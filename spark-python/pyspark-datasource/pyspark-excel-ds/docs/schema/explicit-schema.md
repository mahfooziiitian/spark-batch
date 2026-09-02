# Explicit Schema

Pass a Spark `StructType` (or a DDL string) via `with_schema()` to guarantee
the output DataFrame's types regardless of what pandas infers.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pys_excel import ExcelReader

schema = StructType(
    [
        StructField("emp_id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("salary", DoubleType(), nullable=True),
        StructField("hire_date", TimestampType(), nullable=True),
    ]
)

df = ExcelReader(spark).with_schema(schema).sheet("Employees").read("employees.xlsx")
df.printSchema()
```

## DDL string shorthand

```python
df = ExcelReader(spark).with_schema("emp_id INT, name STRING, salary DOUBLE").read("employees.xlsx")
```

## How casting works

The reader first creates the DataFrame with pandas-inferred types (avoiding
strict `createDataFrame(..., schema=...)` failures like handing a whole-number
`int` column directly to a `DoubleType` field), then applies:

```python
df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])
```

This means any pandas-inferred type that Spark's `cast()` can coerce
(numeric ↔ numeric, string → numeric, etc.) will succeed; genuinely
incompatible values (e.g. non-numeric text in a numeric column) become `null`
after the cast, per Spark's standard `cast()` semantics.

See `examples/04_schema/01_explicit_schema.py`.
