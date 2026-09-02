"""Handling malformed / inconsistent rows in a hand-authored Excel extract.

Key concepts:
    - Business users often merge cells, leave blank rows, or type text into numeric
      columns — validate after reading rather than assuming clean input
    - Use Spark expressions to quarantine bad rows instead of failing the whole job
"""

import pandas as pd
from pyspark.sql import functions as F

from pys_excel import (
    ExcelReader,
    get_spark,
    print_dataframe,
    print_header,
    print_warning,
    set_log_level,
    temp_excel_path,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.malformed_rows")


if __name__ == "__main__":
    spark = get_spark("excel-malformed-rows")

    workbook = temp_excel_path("malformed_rows_demo")
    pd.DataFrame(
        {
            "emp_id": [1, 2, None, 4],
            "name": ["Alice", "Bob", "Carol", None],
            "salary": [95000.0, -1.0, 88000.0, 68000.0],
        }
    ).to_excel(workbook, index=False, engine="openpyxl")

    print_header("1. Read as-is")
    df = ExcelReader(spark).read(workbook)
    print_dataframe(df, title="Raw Data")

    print_header("2. Quarantine invalid rows instead of failing the job")
    is_valid = F.col("emp_id").isNotNull() & F.col("name").isNotNull() & (F.col("salary") >= 0)
    valid_rows = df.filter(is_valid)
    quarantined_rows = df.filter(~is_valid)

    print_dataframe(valid_rows, title="Valid Rows")
    if quarantined_rows.count() > 0:
        print_warning(f"Quarantined {quarantined_rows.count()} invalid row(s)")
        print_dataframe(quarantined_rows, title="Quarantined Rows")

    spark.stop()
