# excel_to_table

Load an Excel sheet directly into a Spark table (full refresh / initial load).

```python
from pys_excel import excel_to_table

excel_to_table(
    spark,
    "employees.xlsx",
    "sales.employees",
    sheet_name="Employees",
    mode="overwrite",
    file_format="delta",
)
```

## Parameters

| Parameter | Default | Description |
|-----------|---------|--------------|
| `sheet_name` | `0` | Sheet name or zero-based index |
| `mode` | `"overwrite"` | Spark save mode: `overwrite`, `append`, `ignore`, `error` |
| `file_format` | `"delta"` | Table storage format (`delta` or `parquet`) |
| `schema` | `None` | Optional explicit `StructType`/DDL string |
| `reader_options` | `None` | Extra `pandas.read_excel` options (header, skiprows, usecols, ...) |
| `partition_by` | `None` | Columns to partition the table by |

## With an explicit schema and extra reader options

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType(
    [
        StructField("emp_id", IntegerType()),
        StructField("name", StringType()),
        StructField("salary", DoubleType()),
    ]
)

excel_to_table(
    spark,
    "employees.xlsx",
    "sales.employees",
    sheet_name="Employees",
    schema=schema,
    reader_options={"skiprows": 1, "na_values": ["N/A"]},
    partition_by=["department"],
)
```

The function returns the written DataFrame so you can inspect it before/after
the table write:

```python
df = excel_to_table(spark, "employees.xlsx", "sales.employees")
df.printSchema()
```
