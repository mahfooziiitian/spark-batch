"""Nightly reconciliation: Excel GL export vs. an existing Delta ledger table.

Key concepts:
    - A common finance close process compares a general-ledger extract
      (Excel, from an upstream ERP) against the governed Delta "book of
      record" table, flagging mismatches for review rather than blindly
      overwriting or merging
    - A full outer join on the natural key (account_id) classifies every row
      as MATCH, MISMATCH (balance differs), NEW_IN_SOURCE (in Excel but not
      yet in the table), or MISSING_IN_SOURCE (in the table but absent from
      today's extract)
    - The reconciliation report itself is written back out to Excel for
      finance to review before anyone applies the change

Requires Delta Lake: install the optional 'delta-spark' extra locally
(`uv sync --extra delta`), or run this on Databricks where Delta is built in.
This example is skipped gracefully if 'delta-spark' is not installed.
"""

import importlib.util

import pandas as pd
from pyspark.sql import functions as F

from pys_excel import (
    ExcelReader,
    ExcelWriter,
    get_spark,
    output_path,
    print_dataframe,
    print_header,
    print_path,
    print_success,
    print_warning,
    set_log_level,
    temp_excel_path,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.gl_reconciliation")

TABLE_NAME = "default.gl_ledger_demo"


def main() -> None:
    if importlib.util.find_spec("delta") is None:
        print_warning(
            "Skipping: install the optional 'delta-spark' extra to run this example (`uv sync --extra delta`)."
        )
        return

    spark = get_spark("gl-reconciliation", enable_delta=True)
    try:
        print_header("1. Seed the Delta ledger table (yesterday's book of record)")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        ledger_df = spark.createDataFrame(
            [
                ("1000", "Cash", 500000.00),
                ("2000", "Accounts Receivable", 125000.00),
                ("3000", "Accounts Payable", 80000.00),
                ("4000", "Retained Earnings", 250000.00),
            ],
            schema="account_id string, account_name string, balance double",
        )
        ledger_df.write.format("delta").mode("overwrite").saveAsTable(TABLE_NAME)
        print_success(f"Ledger table seeded with {spark.table(TABLE_NAME).count()} accounts")

        print_header("2. Today's GL export from the upstream ERP (Excel)")
        gl_path = temp_excel_path("gl_export_demo")
        pd.DataFrame(
            {
                "account_id": ["1000", "2000", "3000", "5000"],
                "account_name": ["Cash", "Accounts Receivable", "Accounts Payable", "Sales Revenue"],
                # 1000 unchanged, 2000 changed, 3000 unchanged, 5000 is brand new,
                # 4000 (Retained Earnings) is absent from today's extract on purpose.
                "balance": [500000.00, 131500.00, 80000.00, 42000.00],
            }
        ).to_excel(gl_path, index=False, engine="openpyxl")
        gl_df = ExcelReader(spark).read(gl_path)
        print_dataframe(gl_df, title="Today's GL export")

        print_header("3. Full outer join & classify each account")
        table_df = spark.table(TABLE_NAME)
        reconciliation = (
            table_df.alias("t")
            .join(gl_df.alias("s"), on="account_id", how="full_outer")
            .select(
                F.coalesce(F.col("t.account_id"), F.col("s.account_id")).alias("account_id"),
                F.coalesce(F.col("t.account_name"), F.col("s.account_name")).alias("account_name"),
                F.col("t.balance").alias("table_balance"),
                F.col("s.balance").alias("source_balance"),
            )
            .withColumn(
                "status",
                F.when(F.col("table_balance").isNull(), "NEW_IN_SOURCE")
                .when(F.col("source_balance").isNull(), "MISSING_IN_SOURCE")
                .when(F.col("table_balance") == F.col("source_balance"), "MATCH")
                .otherwise("MISMATCH"),
            )
            .withColumn(
                "variance",
                F.round(
                    F.coalesce(F.col("source_balance"), F.lit(0.0)) - F.coalesce(F.col("table_balance"), F.lit(0.0)), 2
                ),
            )
            .orderBy("account_id")
        )
        print_dataframe(reconciliation, title="Reconciliation report")

        exceptions = reconciliation.filter(F.col("status") != "MATCH")
        if exceptions.count() > 0:
            print_warning(f"{exceptions.count()} account(s) need finance review before the ledger is updated")

        print_header("4. Write the reconciliation report back out for finance review")
        report_path = output_path("gl_reconciliation_report.xlsx")
        ExcelWriter(sheet_name="Reconciliation").write(reconciliation, report_path)
        print_path("Reconciliation report", report_path)

        spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
