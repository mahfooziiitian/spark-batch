# spark-excel Library (Distributed I/O)

For cluster-scale Excel ingestion, use `pys_excel.spark_excel` instead of the
pandas-based `ExcelReader`/`ExcelWriter`. It drives Spark's native
`.format(...)` API against the community
[spark-excel](https://github.com/crealytics/spark-excel) connector, so
reads/writes are distributed across executors instead of collected to the
driver.

## Formats supported

| Format string | Connector | Runtime |
|----------------|-----------|---------|
| `com.crealytics.spark.excel` | Community **spark-excel** (crealytics) | Any Spark 3.x/4.x cluster, OSS or Databricks. **Databricks Runtime 15.x–16.x**: attach as a cluster Maven library. |
| `excel` | Databricks **built-in** Excel connector | **Databricks Runtime 17.1+** only — no library install needed. |

`resolve_excel_format()` picks the right one automatically based on the
`DATABRICKS_RUNTIME_VERSION` environment variable (set automatically on
Databricks clusters):

```python
from pys_excel.spark_excel import resolve_excel_format

fmt = resolve_excel_format()
# DBR 17.1+        -> "excel"                       (built-in)
# DBR 15.x / 16.x  -> "com.crealytics.spark.excel"   (Maven library required)
# Local / OSS Spark -> "com.crealytics.spark.excel"  (spark.jars.packages required)
```

## Reading

```python
from pys_excel.spark_excel import read_spark_excel

df = read_spark_excel(
    spark,
    "/Volumes/catalog/schema/volume/employees.xlsx",
    data_address="'Employees'!A1",  # sheet name + top-left cell of the range
    header=True,
    infer_schema=True,
)
```

- `data_address` targets a sheet (and optionally a cell range), e.g.
  `"'Sheet1'!A1:F100"`.
- Pass `options={...}` for connector-specific tuning (`maxRowsInMemory`,
  `workbookPassword`, `timestampFormat`, `excerptSize`, ...).

## Writing

```python
from pys_excel.spark_excel import write_spark_excel

write_spark_excel(df, "/Volumes/catalog/schema/volume/report.xlsx", sheet_name="Report", mode="overwrite")
```

## Running locally (OSS Spark)

The connector's JAR must be on the classpath. Use
`get_spark_with_excel_package()` to preload it via `spark.jars.packages`
(requires network access the first time, to resolve from Maven Central):

```python
from pys_excel.spark_excel import get_spark_with_excel_package

spark = get_spark_with_excel_package()  # loads com.crealytics:spark-excel_2.12:3.5.1_0.20.4
```

## Running on Databricks Runtime 15.x / 16.x

Databricks Runtime 15.x ships **Spark 3.5** (Scala 2.12) — the same
compatibility matrix as the local Maven coordinate above. Attach the connector
as a **cluster library**:

1. Cluster settings → **Libraries** → **Install new** → **Maven**.
2. Coordinates: `com.crealytics:spark-excel_2.12:3.5.1_0.20.4`
3. Restart the cluster, then use `format("com.crealytics.spark.excel")`
   directly — no `spark.jars.packages` config needed inside notebooks.

```python
df = (
    spark.read.format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dataAddress", "'Employees'!A1")
    .load("/Volumes/catalog/schema/volume/employees.xlsx")
)
```

See [Databricks](../databricks-runtime/index.md) for the full runbook, including the
built-in Excel format available on DBR 17.1+.

## When to prefer this over the pandas bridge

| | pandas bridge (`ExcelReader`/`ExcelWriter`) | spark-excel (`read_spark_excel`/`write_spark_excel`) |
|---|---|---|
| Setup | None (pure Python) | JVM package (Maven coordinate or DBR built-in) |
| Scale | Driver-collected — thousands of rows | Distributed — cluster-scale workbooks |
| Multi-sheet workbooks | `read_all_sheets()` / `write_many()` | One `data_address` per read/write call |
| Best for | Reports, extracts, small reference data | Large Excel ingestion pipelines |
