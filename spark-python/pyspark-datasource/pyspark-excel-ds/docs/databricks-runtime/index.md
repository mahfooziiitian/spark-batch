# Databricks

This project runs unmodified on Databricks, with two supported paths depending
on your Databricks Runtime (DBR) version.

## Databricks Runtime 15.x / 16.x — spark-excel Maven library

DBR 15.x and 16.x ship **Spark 3.5** with **Scala 2.12**, matching this
project's target versions. Excel support is **not built in** on these
runtimes, so attach the community
[spark-excel](https://github.com/crealytics/spark-excel) connector as a
cluster library:

1. **Compute** → your cluster → **Libraries** → **Install new**.
2. Library source: **Maven**.
3. Coordinates: `com.crealytics:spark-excel_2.12:3.5.1_0.20.4`
4. Install and restart the cluster.

```python
# Notebook cell — no extra config needed once the library is attached
df = (
    spark.read.format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dataAddress", "'Employees'!A1")
    .load("/Volumes/catalog/schema/volume/employees.xlsx")
)

df.write.mode("overwrite").saveAsTable("catalog.schema.employees")
```

Or use the library's helpers directly, which auto-detect this runtime via
`resolve_excel_format()`:

```python
from pys_excel.spark_excel import read_spark_excel, write_spark_excel

df = read_spark_excel(spark, "/Volumes/catalog/schema/volume/employees.xlsx", data_address="'Employees'!A1")
write_spark_excel(df, "/Volumes/catalog/schema/volume/report.xlsx", sheet_name="Report")
```

## Databricks Runtime 17.1+ — built-in Excel connector

DBR 17.1 and above include a **native** Excel data source — no library
install required:

```python
df = spark.read.excel("/Volumes/catalog/schema/volume/employees.xlsx")

# Or with options
df = (
    spark.read
    .option("headerRows", 1)
    .option("dataAddress", "Sheet1!A1:E10")
    .excel("/Volumes/catalog/schema/volume/employees.xlsx")
)
```

`resolve_excel_format()` returns `"excel"` automatically once
`DATABRICKS_RUNTIME_VERSION` indicates 17.1 or later, so code written against
`pys_excel.spark_excel` works unchanged across runtime upgrades.

## Which format am I on?

```python
from pys_excel.spark_excel import is_databricks_runtime, resolve_excel_format

print(is_databricks_runtime())   # e.g. "15.4" or None locally
print(resolve_excel_format())    # "com.crealytics.spark.excel" or "excel"
```

## Recommended workflow on Databricks

1. Land raw Excel extracts as Delta tables with `excel_to_table(..., file_format="delta")`
   (Delta is built into every Databricks Runtime — no extra install needed).
2. Use Unity Catalog Volumes paths (`/Volumes/catalog/schema/volume/...`) for
   both the source workbook and any Excel exports.
3. For recurring extracts (e.g. a nightly HR export), use
   `upsert_table_from_excel()` to MERGE INTO the target table by business key
   instead of overwriting it each run.
4. For large workbooks (tens of thousands+ rows, or many workbooks per run),
   prefer the [spark-excel distributed connector](../data-source/spark-excel-library.md)
   over the pandas-based `ExcelReader`/`ExcelWriter`.

## Compatibility matrix

| Databricks Runtime | Spark | Excel support | Recommended format |
|---|---|---|---|
| 13.3 LTS – 14.x | 3.4 – 3.5 | Community library only | `com.crealytics.spark.excel` (Maven library) |
| **15.x – 16.x** | 3.5 | Community library only | `com.crealytics.spark.excel` (Maven library) |
| 17.1+ | 4.0+ | Built-in | `excel` (no library needed) |
