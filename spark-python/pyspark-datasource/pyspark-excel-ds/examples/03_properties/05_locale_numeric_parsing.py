"""Currency/locale-aware numeric parsing (accounting formats pandas can't infer).

Key concepts:
    - Finance templates format numbers as text: "$1,200.50", "$(500.00)" for a
      negative (accounting parentheses), or European "1.234,56" (dot as
      thousands separator, comma as decimal) — pandas reads all of these as
      plain strings, not numbers
    - dtype() forces the source column to stay a string so no information is
      lost before a custom cleanup pass parses it
    - Clean each locale format explicitly rather than guessing: strip
      currency symbols/thousands separators, detect parentheses as negative,
      and swap decimal separators before casting to double
"""

import re

import pandas as pd
from pyspark.sql import functions as F

from pys_excel import ExcelReader, get_spark, print_dataframe, print_header, set_log_level, temp_excel_path
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.locale_numeric_parsing")


def _clean_us_accounting(value: str) -> float:
    """Parse a US-style accounting string like '$(500.00)' -> -500.0."""
    text = value.strip()
    is_negative = "(" in text and ")" in text
    digits = re.sub(r"[^0-9.\-]", "", text)
    parsed = float(digits) if digits else 0.0
    return -parsed if is_negative else parsed


if __name__ == "__main__":
    spark = get_spark("excel-locale-numeric-parsing")

    print_header("1. US accounting format: '$1,200.50', '$(500.00)' (negative)")
    us_path = temp_excel_path("locale_us_demo")
    pd.DataFrame(
        {
            "emp_id": ["001", "002", "003"],
            "bonus_us": ["$1,200.50", "$(500.00)", "$2,000"],
        }
    ).to_excel(us_path, index=False, engine="openpyxl")

    us_pdf = ExcelReader(spark).dtype({"bonus_us": str}).read(us_path).toPandas()
    us_pdf["bonus_clean"] = us_pdf["bonus_us"].map(_clean_us_accounting)
    print_dataframe(spark.createDataFrame(us_pdf), title="US accounting cleanup")

    print_header("2. European format: '.' as thousands separator, ',' as decimal")
    eu_path = temp_excel_path("locale_eu_demo")
    pd.DataFrame(
        {
            "emp_id": ["001", "002"],
            "salary_eu": ["1.234,56", "98.000,00"],
        }
    ).to_excel(eu_path, index=False, engine="openpyxl")

    eu_pdf = ExcelReader(spark).dtype({"salary_eu": str}).read(eu_path).toPandas()
    eu_pdf["salary_clean"] = (
        eu_pdf["salary_eu"].str.replace(".", "", regex=False).str.replace(",", ".", regex=False).astype(float)
    )
    eu_df = spark.createDataFrame(eu_pdf).withColumn("salary_clean", F.col("salary_clean").cast("double"))
    print_dataframe(eu_df, title="European decimal cleanup")

    spark.stop()
