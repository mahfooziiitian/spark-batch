"""Databricks job task: export a Delta table (or query) back to an Excel report.

Consumes the ``pys_excel`` wheel library's ``table_to_excel``, which bridges
through pandas (``ExcelWriter``, xlsxwriter engine) to produce a formatted
workbook under a Unity Catalog Volumes path.

Run via: databricks bundle run excel_ingestion_job -t <target>
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from pys_excel import table_to_excel


def main() -> None:
    parser = argparse.ArgumentParser(description="Export a Delta table to an Excel report.")
    parser.add_argument("--table-name", required=True, help="Source catalog.schema.table.")
    parser.add_argument("--output-path", required=True, help="Volumes path for the destination .xlsx workbook.")
    parser.add_argument("--sheet-name", default="Report", help="Worksheet name to write (default: Report).")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    table_to_excel(spark, args.table_name, args.output_path, sheet_name=args.sheet_name)
    print(f"Exported {args.table_name} to {args.output_path} (sheet={args.sheet_name})")


if __name__ == "__main__":
    main()
