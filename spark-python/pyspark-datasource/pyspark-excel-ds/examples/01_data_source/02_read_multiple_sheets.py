"""Read every sheet from a multi-sheet workbook into a dict of DataFrames.

Key concepts:
    - read_all_sheets() returns {sheet_name: DataFrame}
    - Useful when a workbook models multiple related entities (e.g. Employees, Departments)
"""

from pys_excel import ExcelReader, generate_sample_workbook, get_spark, print_dataframe, print_header, set_log_level
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.read_multiple_sheets")


if __name__ == "__main__":
    spark = get_spark("read-excel-multi-sheet")

    workbook = generate_sample_workbook()

    print_header("1. Read all sheets")
    sheets = ExcelReader(spark).read_all_sheets(workbook)
    for name, df in sheets.items():
        print_dataframe(df, title=name)

    spark.stop()
