"""Handling a missing or invalid Excel file path.

Key concepts:
    - Reading a nonexistent path raises FileNotFoundError from pandas
    - Reading a nonexistent sheet name raises a ValueError-style pandas error
    - Wrap reads defensively in production ingestion pipelines
"""

from pys_excel import (
    ExcelReader,
    generate_sample_workbook,
    get_spark,
    print_error,
    print_header,
    print_success,
    set_log_level,
    temp_excel_path,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.missing_file")


if __name__ == "__main__":
    spark = get_spark("excel-missing-file")

    print_header("1. Missing file path")
    try:
        ExcelReader(spark).read(temp_excel_path("does_not_exist"))
    except FileNotFoundError as exc:
        print_error(f"Expected failure: {exc}")

    print_header("2. Missing sheet name")
    workbook = generate_sample_workbook()
    try:
        ExcelReader(spark).sheet("DoesNotExist").read(workbook)
    except ValueError as exc:
        print_error(f"Expected failure: {exc}")

    print_success("Both failure modes handled gracefully")
    spark.stop()
