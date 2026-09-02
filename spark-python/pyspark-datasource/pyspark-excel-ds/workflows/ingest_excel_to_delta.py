"""Databricks job task: land an Excel extract into a Delta table.

Consumes the ``pys_excel`` wheel library (attached to the job cluster via
``libraries.whl`` in resources/excel_ingestion_job.job.yml) and its pandas
bridge (``ExcelReader``/``excel_to_table``) — no JVM Excel package required
for this task. Unity Catalog Volumes paths (``/Volumes/catalog/schema/...``)
are FUSE-mounted on the driver, so they read like local files.

Run via: databricks bundle run excel_ingestion_job -t <target>
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from pys_excel import excel_to_table


def _parse_sheet_name(value: str) -> str | int:
    """Allow the sheet to be selected by name or zero-based index."""
    return int(value) if value.isdigit() else value


def main() -> None:
    parser = argparse.ArgumentParser(description="Load an Excel extract into a Delta table.")
    parser.add_argument("--input-path", required=True, help="Volumes path to the source .xlsx workbook.")
    parser.add_argument("--table-name", required=True, help="Destination catalog.schema.table.")
    parser.add_argument("--sheet-name", default="0", help="Sheet name or zero-based index (default: 0).")
    parser.add_argument("--mode", default="overwrite", help="Save mode: overwrite, append, ignore, error.")
    parser.add_argument("--file-format", default="delta", help="Table storage format (delta or parquet).")
    args = parser.parse_args()

    # Databricks job clusters expose `spark`/`dbutils` globally, but building the
    # session explicitly keeps this script runnable outside a notebook context too.
    spark = SparkSession.builder.getOrCreate()

    df = excel_to_table(
        spark,
        args.input_path,
        args.table_name,
        sheet_name=_parse_sheet_name(args.sheet_name),
        mode=args.mode,
        file_format=args.file_format,
    )
    print(f"Loaded {df.count()} rows from {args.input_path} into {args.table_name} (format={args.file_format})")


if __name__ == "__main__":
    main()
