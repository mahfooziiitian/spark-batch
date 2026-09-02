"""Selecting sheets by name or index, and reading a subset of columns.

Key concepts:
    - sheet() accepts a sheet name or zero-based index
    - usecols() takes an Excel-style column range string (e.g. "A:B") or a list of names
"""

from pys_excel import ExcelReader, generate_sample_workbook, get_spark, print_dataframe, print_header, set_log_level
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.sheet_selection")


if __name__ == "__main__":
    spark = get_spark("excel-sheet-selection")

    workbook = generate_sample_workbook()

    print_header("1. Select sheet by index (0 = first sheet)")
    df_by_index = ExcelReader(spark).sheet(0).read(workbook)
    print_dataframe(df_by_index, title="Sheet 0 (Employees)")

    print_header("2. Select sheet by name and a column subset")
    df_subset = ExcelReader(spark).sheet("Employees").usecols(["name", "salary"]).read(workbook)
    print_dataframe(df_subset, title="Name + Salary only")

    spark.stop()
