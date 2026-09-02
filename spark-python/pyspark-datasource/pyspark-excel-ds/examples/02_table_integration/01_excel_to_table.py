"""Read Excel data and land it as a managed Spark table.

Key concepts:
    - excel_to_table() reads a sheet with ExcelReader and calls df.write.saveAsTable()
    - Use file_format="delta" in production (requires the optional 'delta-spark' extra
      locally, or run on Databricks where Delta is built in)
"""

from pys_excel import excel_to_table, generate_sample_workbook, get_spark, print_dataframe, print_header, set_log_level
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.excel_to_table")

TABLE_NAME = "default.employees"


if __name__ == "__main__":
    spark = get_spark("excel-to-table")

    workbook = generate_sample_workbook()

    print_header("1. Load Excel sheet into a managed table")
    excel_to_table(
        spark,
        workbook,
        TABLE_NAME,
        sheet_name="Employees",
        mode="overwrite",
        file_format="parquet",  # swap to "delta" with enable_delta=True / on Databricks
    )

    print_header("2. Query the table like any other Spark table")
    result = spark.sql(f"SELECT department, COUNT(*) AS headcount FROM {TABLE_NAME} GROUP BY department")  # noqa: S608
    print_dataframe(result, title="Headcount by Department")

    spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    spark.stop()
