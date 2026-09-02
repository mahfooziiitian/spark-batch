"""Write multiple DataFrames as separate sheets in one workbook.

Key concepts:
    - write_many() takes a dict of {sheet_name: DataFrame}
    - Useful for multi-entity reports (e.g. a summary workbook with one tab per table)
"""

from pyspark.sql import functions as F

from pys_excel import (
    ExcelReader,
    ExcelWriter,
    generate_sample_workbook,
    get_spark,
    output_path,
    print_header,
    print_path,
    print_success,
    set_log_level,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.write_multiple_sheets")


if __name__ == "__main__":
    spark = get_spark("write-excel-multi-sheet")

    workbook = generate_sample_workbook()
    sheets = ExcelReader(spark).read_all_sheets(workbook)
    dept_headcount = sheets["Employees"].groupBy("department").agg(F.count("*").alias("headcount"))

    print_header("1. Write a multi-sheet summary workbook")
    out_path = output_path("summary.xlsx")
    ExcelWriter().write_many(
        {
            "Employees": sheets["Employees"],
            "Departments": sheets["Departments"],
            "Headcount": dept_headcount,
        },
        out_path,
    )
    print_path("Output workbook", out_path)
    print_success("Write complete")

    spark.stop()
