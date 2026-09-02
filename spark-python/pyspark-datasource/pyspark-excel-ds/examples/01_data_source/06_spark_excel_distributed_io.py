"""Distributed Excel reads/writes via the spark-excel (crealytics) data source.

Key concepts:
    - read_spark_excel()/write_spark_excel() drive Spark's native
      .format("com.crealytics.spark.excel") reader/writer instead of collecting
      through pandas — suitable for cluster-scale workbooks
    - resolve_excel_format() auto-selects the built-in "excel" format on
      Databricks Runtime 17.1+, or "com.crealytics.spark.excel" everywhere else
      (including Databricks Runtime 15.x/16.x with the Maven library attached)

Running locally requires the spark-excel Maven package on the classpath, which
this example loads via get_spark_with_excel_package(). On Databricks, skip that
call and instead attach the library (or rely on the runtime's built-in support):

    Cluster libraries -> Maven -> com.crealytics:spark-excel_2.12:3.5.1_0.20.4
"""

from pys_excel import (
    generate_sample_workbook,
    output_path,
    print_header,
    print_path,
    print_success,
    print_warning,
    set_log_level,
)
from pys_excel._logging import get_logger
from pys_excel.spark_excel import (
    SPARK_EXCEL_PACKAGE_SCALA_2_12,
    get_spark_with_excel_package,
    read_spark_excel,
    resolve_excel_format,
    write_spark_excel,
)

set_log_level("DEBUG")
logger = get_logger("example.spark_excel_read_write")


if __name__ == "__main__":
    print_header("1. Resolve the Excel format for this runtime")
    fmt = resolve_excel_format()
    print_success(f"Resolved format: {fmt}")

    print_header("2. Load a local SparkSession with the spark-excel package")
    print_path("Maven coordinate", SPARK_EXCEL_PACKAGE_SCALA_2_12)
    try:
        spark = get_spark_with_excel_package()
    except Exception as exc:
        print_warning(f"Could not initialize spark-excel (likely no network access): {exc}")
        raise SystemExit(0) from None

    workbook = generate_sample_workbook()

    print_header("3. Read distributed via .format('com.crealytics.spark.excel')")
    df = read_spark_excel(spark, workbook, data_address="'Employees'!A1")
    df.show()

    print_header("4. Write distributed via the same format")
    write_spark_excel(df.limit(3), output_path("spark_excel_output.xlsx"), sheet_name="Top3")
    print_success("Distributed write complete")

    spark.stop()
