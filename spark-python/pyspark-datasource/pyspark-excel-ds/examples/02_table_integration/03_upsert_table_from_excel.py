"""Upsert (MERGE INTO) an Excel extract into an existing Delta table.

Key concepts:
    - upsert_table_from_excel() creates the table on first run, then MERGEs on
      subsequent runs using key_columns to match rows
    - Requires Delta Lake: install the optional 'delta-spark' extra locally
      (`uv sync --extra delta`), or run this on Databricks where Delta is built in

This example is skipped gracefully if 'delta-spark' is not installed.
"""

import importlib.util

from pys_excel import (
    generate_sample_workbook,
    print_header,
    print_success,
    print_warning,
    set_log_level,
    upsert_table_from_excel,
)
from pys_excel._logging import get_logger
from pys_excel.config import get_spark

set_log_level("DEBUG")
logger = get_logger("example.upsert_table_from_excel")

TABLE_NAME = "default.employees_upserted"


def main() -> None:
    if importlib.util.find_spec("delta") is None:
        print_warning(
            "Skipping: install the optional 'delta-spark' extra to run this example (`uv sync --extra delta`)."
        )
        return

    spark = get_spark("upsert-table-from-excel", enable_delta=True)
    try:
        workbook = generate_sample_workbook()

        print_header("1. Initial load (creates the table)")
        upsert_table_from_excel(spark, workbook, TABLE_NAME, key_columns=["emp_id"])
        print_success(f"Row count after initial load: {spark.table(TABLE_NAME).count()}")

        print_header("2. Re-run with the same workbook (idempotent MERGE)")
        upsert_table_from_excel(spark, workbook, TABLE_NAME, key_columns=["emp_id"])
        print_success(f"Row count after re-run: {spark.table(TABLE_NAME).count()}")

        spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
