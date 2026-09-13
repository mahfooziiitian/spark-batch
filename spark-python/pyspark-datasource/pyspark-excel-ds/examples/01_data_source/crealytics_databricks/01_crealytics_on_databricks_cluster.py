"""com.crealytics.spark.excel on a Databricks cluster (Maven library attached).

Key concepts:
    - On Databricks, the crealytics connector is attached as a **cluster
      library** (Cluster settings -> Libraries -> Install new -> Maven) —
      unlike local runs, you never call `get_spark_with_excel_package()` here
      (there is no `spark.jars.packages` step; the JAR is already on the
      cluster's classpath before your code runs)
    - The Scala build must match the cluster's **Databricks Runtime**, not
      your local machine's PySpark version:
        - DBR 13.3 LTS - 16.x (Spark 3.5.x, Scala 2.12):
          `com.crealytics:spark-excel_2.12:3.5.1_0.20.4`
        - DBR 17.1+: prefer the built-in `excel` format instead (see
          `databricks/01_native_excel_format.py`) — no library needed at all
    - Once attached, use `excel_format=CREALYTICS_EXCEL_FORMAT` (or let
      `resolve_excel_format()` pick it automatically on DBR < 17.1) exactly
      as you would locally — the read/write option names are identical

Running this example:
    - **On a Databricks cluster with the library attached**: run as-is, in a
      notebook or job.
    - **Everywhere else** (local Spark, or a Databricks cluster without the
      library attached): the connector class isn't on the classpath, so this
      script detects that it isn't running on Databricks and skips
      gracefully. See `crealytics_local/` to run the same connector locally.

Reference: https://github.com/crealytics/spark-excel#as-a-maven-dependency-in-databricks
"""

from pys_excel import (
    generate_sample_workbook,
    get_spark,
    output_path,
    print_dataframe,
    print_header,
    print_path,
    print_success,
    print_warning,
    set_log_level,
)
from pys_excel.logs import get_logger
from pys_excel.spark_excel import CREALYTICS_EXCEL_FORMAT, is_databricks_runtime, read_spark_excel, write_spark_excel

set_log_level("DEBUG")
logger = get_logger("example.crealytics_on_databricks")


if __name__ == "__main__":
    dbr = is_databricks_runtime()
    if dbr is None:
        print_warning(
            "Not running on Databricks. This example assumes com.crealytics:spark-excel is "
            "attached as a cluster Maven library — see the module docstring for the coordinate. "
            "Skipping. See crealytics_local/ to run the same connector on local Spark."
        )
        raise SystemExit(0)

    print_header(f"Databricks Runtime {dbr} detected — using cluster-attached '{CREALYTICS_EXCEL_FORMAT}'")
    # No get_spark_with_excel_package() call: the connector is already on the
    # cluster's classpath via the attached Maven library, not spark.jars.packages.
    spark = get_spark(app_name="crealytics-on-databricks")

    workbook = generate_sample_workbook()
    print_path("Sample workbook", workbook)

    print_header("1. Read via the cluster-attached crealytics library")
    df = read_spark_excel(spark, workbook, data_address="'Employees'!A1", excel_format=CREALYTICS_EXCEL_FORMAT)
    print_dataframe(df, title="Employees")

    print_header("2. Write via the same cluster-attached library")
    out_path = output_path("crealytics_databricks_output.xlsx")
    write_spark_excel(df.limit(3), out_path, sheet_name="Top3", mode="overwrite", excel_format=CREALYTICS_EXCEL_FORMAT)
    print_success(f"Wrote 3 row(s) to {out_path} via '{CREALYTICS_EXCEL_FORMAT}'")

    spark.stop()
