"""Schema inference behavior — how pandas + Spark infer column types from Excel data.

Key concepts:
    - Numeric-looking columns become LongType/DoubleType automatically
    - Mixed-type columns (numbers + text) fall back to StringType/object
    - printSchema()-equivalent (print_schema) shows the inferred result
"""

import pandas as pd

from pys_excel import (
    ExcelReader,
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    set_log_level,
    temp_excel_path,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.schema_inference")


if __name__ == "__main__":
    spark = get_spark("excel-schema-inference")

    workbook = temp_excel_path("schema_inference_demo")
    pd.DataFrame(
        {
            "id": [1, 2, 3],
            "amount": [10.5, 20, 30.25],
            "note": ["ok", 42, "pending"],  # mixed types -> inferred as object/string
        }
    ).to_excel(workbook, index=False, engine="openpyxl")

    print_header("1. Inferred schema from a mixed-type sheet")
    df = ExcelReader(spark).read(workbook)
    print_schema(df, title="Inferred Schema")
    print_dataframe(df, title="Data")

    spark.stop()
