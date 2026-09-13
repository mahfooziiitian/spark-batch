"""spark-excel multi-sheet, explicit schema, and multi-file ingestion patterns.

Complements `01_spark_excel_distributed_io.py` (happy path) and
`02_spark_excel_option_reference.py` (per-option reference) with three
higher-level distributed ingestion patterns built on the same
`com.crealytics.spark.excel` data source:

Key concepts:
    - Looping over every sheet in a workbook and reading each with
      `read_spark_excel()` — spark-excel has no `read_all_sheets()`
      equivalent (unlike the pandas bridge), so sheet names must be
      discovered up front and read one `data_address` at a time
    - Enforcing an explicit `schema` (bypasses `inferSchema` sampling
      entirely) for stable, guaranteed types across runs/files — pass
      `schema=` to `read_spark_excel()`
    - Distributed multi-file ingestion: `com.crealytics.spark.excel` opens
      its `path` directly via Hadoop's `FileSystem` — it does **not** support
      glob patterns like Spark's generic file-based sources (csv/parquet).
      The working pattern is: discover matching files yourself, read each
      one, tag it with its source path, and `unionByName()` the results into
      a single distributed DataFrame

Reference: https://github.com/crealytics/spark-excel#configuration
"""

import openpyxl

from pys_excel import (
    data_path,
    generate_sample_workbook,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
)
from pys_excel.logs import get_logger
from pys_excel.spark_excel import get_spark_with_excel_package, read_spark_excel, resolve_spark_excel_package

set_log_level("DEBUG")
logger = get_logger("example.spark_excel_multi_sheet_and_schema")


if __name__ == "__main__":
    print_header("Setup: SparkSession with the matching spark-excel package")
    print_path("Maven coordinate", resolve_spark_excel_package())
    try:
        spark = get_spark_with_excel_package(app_name="spark-excel-multi-sheet-and-schema")
    except Exception as exc:
        print_warning(f"Could not initialize spark-excel (likely no network access): {exc}")
        raise SystemExit(0) from None

    workbook = generate_sample_workbook()
    print_path("Sample workbook", workbook)

    print_header("1. Loop over every sheet in a workbook (no read_all_sheets() for spark-excel)")
    sheet_names = openpyxl.load_workbook(workbook, read_only=True).sheetnames
    print_success(f"Discovered sheets: {sheet_names}")
    sheets = {name: read_spark_excel(spark, workbook, data_address=f"'{name}'!A1") for name in sheet_names}
    for name, df in sheets.items():
        print_dataframe(df, title=f"Sheet: {name}")

    print_header("2. Enforce an explicit schema (skips inferSchema sampling)")
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

    employees_schema = StructType(
        [
            StructField("emp_id", DoubleType(), True),
            StructField("name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("salary", DoubleType(), True),
            StructField("hire_date", TimestampType(), True),
        ]
    )
    df_typed = read_spark_excel(spark, workbook, data_address="'Employees'!A1", schema=employees_schema)
    print_schema(df_typed, title="Explicit schema — no sampling, stable types across runs")

    print_header("3. Distributed multi-file ingestion (read-each, tag, unionByName)")
    import glob as globmod
    from functools import reduce

    from pyspark.sql import functions as F

    second_workbook = generate_sample_workbook(data_path("file_data", "excel", "employees_branch2.xlsx"))
    print_path("Second workbook", second_workbook)

    matched_paths = sorted(globmod.glob(data_path("file_data", "excel", "employees*.xlsx")))
    print_success(f"Discovered {len(matched_paths)} workbook(s): {matched_paths}")

    per_file_dfs = [
        read_spark_excel(spark, p, data_address="'Employees'!A1", schema=employees_schema).withColumn(
            "source_file", F.lit(p)
        )
        for p in matched_paths
    ]
    df_all_branches = reduce(lambda left, right: left.unionByName(right), per_file_dfs)
    print_success(
        f"Read {df_all_branches.count()} row(s) across "
        f"{df_all_branches.select('source_file').distinct().count()} file(s)"
    )
    print_dataframe(
        df_all_branches.select("name", "department", "source_file"),
        title="Union of all matching workbooks, tagged with their source file",
    )

    spark.stop()
