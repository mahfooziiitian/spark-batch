"""Formatting output workbooks: sheet name, frozen header, autofit columns, engine choice.

Key concepts:
    - with_freeze_header() keeps the header row visible while scrolling
    - with_autofit_columns() sizes columns to fit their content (xlsxwriter only)
    - with_engine() switches between xlsxwriter (styling) and openpyxl (compatibility)
"""

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
logger = get_logger("example.formatting_and_styles")


if __name__ == "__main__":
    spark = get_spark("excel-formatting")

    workbook = generate_sample_workbook()
    employees = ExcelReader(spark).sheet("Employees").read(workbook)

    print_header("1. xlsxwriter with frozen header + autofit columns (defaults)")
    styled_path = output_path("employees_styled.xlsx")
    ExcelWriter(sheet_name="Employees").write(employees, styled_path)
    print_path("Styled workbook", styled_path)

    print_header("2. openpyxl engine, no autofit (broadest compatibility)")
    plain_path = output_path("employees_plain.xlsx")
    ExcelWriter(sheet_name="Employees").with_engine("openpyxl").with_autofit_columns(False).write(employees, plain_path)
    print_path("Plain workbook", plain_path)

    print_success("Formatting examples complete")
    spark.stop()
