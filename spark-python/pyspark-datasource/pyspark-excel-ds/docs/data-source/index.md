# Data Source

Two complementary ways to move data between Excel and Spark.

## pandas bridge (default)

`ExcelReader` and `ExcelWriter` wrap `pandas.read_excel()` / `pandas.ExcelWriter`
and bridge through `spark.createDataFrame()` / `DataFrame.toPandas()`. No JVM
Excel package is required — this works out of the box anywhere PySpark runs.

```python
from pys_excel import ExcelReader, ExcelWriter, get_spark

spark = get_spark("data-source-demo")

df = ExcelReader(spark).sheet("Employees").read("data/employees.xlsx")
ExcelWriter(sheet_name="Report").write(df, "output/report.xlsx")
```

!!! warning "Driver-collected"
    Both sides collect data through the Spark driver (`createDataFrame`,
    `toPandas`). Fine for reporting-sized workbooks (thousands to tens of
    thousands of rows); for larger workbooks, use the distributed
    [spark-excel connector](spark-excel-library.md) instead.

## spark-excel (distributed)

`read_spark_excel()` / `write_spark_excel()` in `pys_excel.spark_excel` drive
Spark's native `.format(...)` API — reads/writes are distributed across
executors like any other Spark data source. See
[spark-excel library](spark-excel-library.md) for setup details, Maven
coordinates, and Databricks Runtime compatibility.

## Pages in this section

- [Reading Excel](reading.md)
- [Writing Excel](writing.md)
- [spark-excel library (distributed I/O)](spark-excel-library.md)
