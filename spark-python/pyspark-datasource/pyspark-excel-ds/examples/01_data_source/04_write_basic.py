"""Write a Spark DataFrame to a single-sheet Excel workbook.

Key concepts:
    - ExcelWriter collects the DataFrame with toPandas() then writes via pandas.ExcelWriter
    - autofit_columns() and freeze_header() improve the reporting UX for free
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
logger = get_logger("example.write_basic")


if __name__ == "__main__":
    spark = get_spark("write-excel-basic")

    workbook = generate_sample_workbook()
    employees = ExcelReader(spark).sheet("Employees").read(workbook)
    high_earners = employees.filter(F.col("salary") > 70000).orderBy(F.desc("salary"))

    print_header("1. Write filtered DataFrame to Excel")
    out_path = output_path("high_earners.xlsx")
    ExcelWriter(sheet_name="HighEarners").write(high_earners, out_path)
    print_path("Output workbook", out_path)
    print_success("Write complete")

    spark.stop()
