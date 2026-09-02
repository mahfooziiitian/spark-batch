"""Header row, skipped rows, and column selection when reading Excel.

Key concepts:
    - header() sets the header row index (0-based); use None for headerless sheets
    - skiprows() skips leading rows (e.g. title rows above the real header)
    - usecols() restricts which columns are parsed (Excel-style range or list)
"""

from pathlib import Path

import pandas as pd

from pys_excel import ExcelReader, get_spark, print_dataframe, print_header, set_log_level, temp_excel_path
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.header_and_skiprows")


def _write_workbook_with_title_row(path: str) -> None:
    """Write a workbook with a title row above the real header, like many hand-authored reports."""
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame([["Quarterly Report - Confidential"]]).to_excel(
            writer, sheet_name="Report", index=False, header=False
        )
        book = writer.book
        sheet = writer.sheets["Report"]
        headers = ["emp_id", "name", "salary"]
        rows = [[1, "Alice", 95000], [2, "Bob", 72000]]
        for col_idx, header in enumerate(headers):
            sheet.cell(row=2, column=col_idx + 1, value=header)
        for row_idx, row in enumerate(rows, start=3):
            for col_idx, value in enumerate(row):
                sheet.cell(row=row_idx, column=col_idx + 1, value=value)
        _ = book


if __name__ == "__main__":
    spark = get_spark("excel-header-skiprows")

    workbook = temp_excel_path("title_row_report")
    _write_workbook_with_title_row(workbook)

    print_header("1. Skip the title row, use row 0 (after skip) as header")
    df = ExcelReader(spark).sheet("Report").skiprows(1).header(0).read(workbook)
    print_dataframe(df, title="Parsed Report")

    Path(workbook).unlink(missing_ok=True)
    spark.stop()
