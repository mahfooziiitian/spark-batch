"""Export a Spark table (or ad-hoc query) to an Excel workbook.

Key concepts:
    - table_to_excel() runs `SELECT * FROM table` (or a custom query) and writes the
      result with ExcelWriter
    - Pass writer_options to tweak formatting (index, sheet_name, engine, ...)
"""

from pys_excel import (
    excel_to_table,
    generate_sample_workbook,
    get_spark,
    output_path,
    print_header,
    print_path,
    print_success,
    set_log_level,
    table_to_excel,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.table_to_excel")

TABLE_NAME = "default.employees_export"


if __name__ == "__main__":
    spark = get_spark("table-to-excel")

    workbook = generate_sample_workbook()
    excel_to_table(spark, workbook, TABLE_NAME, sheet_name="Employees", file_format="parquet")

    print_header("1. Export the full table")
    out_path = output_path("employees_export.xlsx")
    table_to_excel(spark, TABLE_NAME, out_path, sheet_name="Employees")
    print_path("Output workbook", out_path)

    print_header("2. Export a filtered query instead of the whole table")
    query_path = output_path("high_earners_export.xlsx")
    table_to_excel(
        spark,
        TABLE_NAME,
        query_path,
        sheet_name="HighEarners",
        query=f"SELECT * FROM {TABLE_NAME} WHERE salary > 70000",  # noqa: S608
    )
    print_path("Output workbook", query_path)
    print_success("Export complete")

    spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    spark.stop()
