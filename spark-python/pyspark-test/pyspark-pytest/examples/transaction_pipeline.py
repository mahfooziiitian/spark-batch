"""Example: Transaction classification pipeline end-to-end.

Demonstrates the data processing pipeline that joins transactions
with accounts and classifies them as debit/credit/other.

Run:
    uv run python examples/transaction_pipeline.py
"""

import os

from pyspark.sql import SparkSession

from pys.data_processing import classify_debit_credit_transactions


def main() -> None:
    """Run the transaction classification pipeline with sample data."""
    spark = (
        SparkSession.builder.appName("example-transaction-pipeline")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Sample transactions
    transactions_df = spark.createDataFrame(
        [
            ("TXN001", 1500.00, "123-456-789-PAYMENT"),
            ("TXN002", 250.00, "222222222EUR-TRANSFER"),
            ("TXN003", 3200.00, "333333333-SALARY"),
            ("TXN004", 75.50, "444444444USD-PURCHASE"),
        ],
        schema=["transaction_id", "amount", "transaction_information"],
    )

    # Account reference data
    accounts_df = spark.createDataFrame(
        [
            ("123456789", "101"),  # Credit
            ("222222222", "202"),  # Debit
            ("333333333", "103"),  # Credit
            ("444444444", "999"),  # Other
        ],
        schema=["account_number", "business_line_id"],
    )

    print("=== Transactions ===")
    transactions_df.show(truncate=False)

    print("=== Accounts ===")
    accounts_df.show()

    # Run classification pipeline
    result = classify_debit_credit_transactions(transactions_df, accounts_df)

    print("=== Classified Transactions ===")
    result.select(
        "transaction_id", "amount", "account_number", "business_line_id", "business_line"
    ).show(truncate=False)

    # Summary
    print("=== Classification Summary ===")
    result.groupBy("business_line").count().show()

    spark.stop()


if __name__ == "__main__":
    main()
