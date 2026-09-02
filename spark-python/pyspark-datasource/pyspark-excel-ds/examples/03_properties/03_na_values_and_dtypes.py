"""Custom NA value handling and forced pandas dtypes before the Spark bridge.

Key concepts:
    - na_values() adds extra strings (e.g. "N/A", "-") to treat as null
    - dtype() forces specific pandas column types (e.g. force an ID column to str)
    - keep_default_na() toggles pandas' built-in NA recognition (e.g. "NA", "")
"""

import pandas as pd

from pys_excel import ExcelReader, get_spark, print_dataframe, print_header, set_log_level, temp_excel_path
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.na_values_and_dtypes")


if __name__ == "__main__":
    spark = get_spark("excel-na-values-dtypes")

    workbook = temp_excel_path("na_values_demo")
    pd.DataFrame(
        {
            "emp_id": ["001", "002", "003"],
            "name": ["Alice", "Bob", "Carol"],
            "bonus": ["1000", "N/A", "-"],
        }
    ).to_excel(workbook, index=False, engine="openpyxl")

    print_header("1. Default read — 'N/A' and '-' stay as strings")
    default_df = ExcelReader(spark).read(workbook)
    print_dataframe(default_df, title="Default NA handling")

    print_header("2. Treat 'N/A' and '-' as null, keep emp_id as string")
    custom_df = ExcelReader(spark).na_values(["N/A", "-"]).dtype({"emp_id": str}).read(workbook)
    print_dataframe(custom_df, title="Custom NA handling")

    spark.stop()
