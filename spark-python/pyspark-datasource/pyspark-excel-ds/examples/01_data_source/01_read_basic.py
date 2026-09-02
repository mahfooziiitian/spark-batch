"""Read a single Excel sheet into a Spark DataFrame — the basic case.

Key concepts:
    - ExcelReader wraps pandas.read_excel() and bridges to spark.createDataFrame()
    - sheet() selects a worksheet by name or zero-based index
"""

from pys_excel import (
    ExcelReader,
    generate_sample_workbook,
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    set_log_level,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.read_basic")


if __name__ == "__main__":
    spark = get_spark("read-excel-basic")

    workbook = generate_sample_workbook()

    print_header("1. Read the 'Employees' sheet")
    df = ExcelReader(spark).sheet("Employees").read(workbook)
    print_schema(df, title="Employees Schema")
    print_dataframe(df, title="Employees")

    spark.stop()
