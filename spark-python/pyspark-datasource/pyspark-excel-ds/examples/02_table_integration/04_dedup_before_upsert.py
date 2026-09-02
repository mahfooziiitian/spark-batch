"""Deduplicate a source Excel batch before upserting into a Delta table.

Key concepts:
    - Business users often submit the same key twice within one workbook
      (e.g. an employee row submitted twice with different values)
    - Delta MERGE INTO fails outright when multiple source rows match the
      same target row ("multiple source rows matched") — catch it and
      dedupe first rather than letting the whole batch fail
    - Use a window function (row_number over the key, ordered by a recency
      column) to keep only the last-submitted row per key before merging

Requires Delta Lake: install the optional 'delta-spark' extra locally
(`uv sync --extra delta`), or run this on Databricks where Delta is built in.
This example is skipped gracefully if 'delta-spark' is not installed.
"""

import importlib.util

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

from pys_excel import (
    ExcelReader,
    ExcelWriter,
    get_spark,
    print_dataframe,
    print_error,
    print_header,
    print_success,
    print_warning,
    set_log_level,
    temp_excel_path,
    upsert_table_from_excel,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.dedup_before_upsert")

TABLE_NAME = "default.employees_dedup_demo"


def main() -> None:
    if importlib.util.find_spec("delta") is None:
        print_warning(
            "Skipping: install the optional 'delta-spark' extra to run this example (`uv sync --extra delta`)."
        )
        return

    spark = get_spark("dedup-before-upsert", enable_delta=True)
    try:
        print_header("1. Source batch with a duplicate emp_id (submitted twice)")
        dup_path = temp_excel_path("dedup_demo_batch")
        pd.DataFrame(
            {
                "emp_id": ["001", "002", "002"],
                "name": ["Alice", "Bob", "Bob"],
                "salary": [95000, 82000, 86000],
                "last_updated": ["2024-01-01", "2024-01-02", "2024-01-05"],
            }
        ).to_excel(dup_path, index=False, engine="openpyxl")
        raw_df = ExcelReader(spark).read(dup_path)
        print_dataframe(raw_df, title="Raw batch (has a duplicate key)")

        print_header("2. Attempting to MERGE this straight in fails")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        try:
            # First run just creates the table (no target rows to match yet).
            upsert_table_from_excel(spark, dup_path, TABLE_NAME, key_columns=["emp_id"])
            # Second run MERGEs the same duplicate source against the now-existing
            # table, where each duplicate key matches more than one target row.
            upsert_table_from_excel(spark, dup_path, TABLE_NAME, key_columns=["emp_id"])
        except Exception as exc:
            print_error(f"MERGE failed as expected: {exc}")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")

        print_header("3. Dedupe by key, keeping the most recently updated row")
        window = Window.partitionBy("emp_id").orderBy(F.col("last_updated").desc())
        deduped_df = raw_df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
        print_dataframe(deduped_df, title="Deduplicated batch")

        clean_path = temp_excel_path("dedup_demo_batch_clean")
        ExcelWriter().write(deduped_df, clean_path)

        print_header("4. Upsert the cleaned batch")
        upsert_table_from_excel(spark, clean_path, TABLE_NAME, key_columns=["emp_id"])
        print_success(f"Row count after clean upsert: {spark.table(TABLE_NAME).count()}")

        spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
