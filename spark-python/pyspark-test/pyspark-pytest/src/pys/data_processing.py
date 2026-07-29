"""Transaction classification pipeline.

Joins transaction data with account information and classifies
transactions as debit, credit, or other based on business line IDs.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CREDIT_BUSINESS_LINE_IDS = ["101", "102", "103"]
DEBIT_BUSINESS_LINE_IDS = ["201", "202", "203"]


def classify_debit_credit_transactions(
    transactions_df: DataFrame, accounts_df: DataFrame
) -> DataFrame:
    """Join transactions with account information and classify as debit/credit.

    Args:
        transactions_df: Raw transaction data with 'transaction_information' column.
        accounts_df: Account reference data with 'account_number' and 'business_line_id'.

    Returns:
        DataFrame with transactions classified by business line (debit/credit/other).
    """
    transactions_df = normalise_transaction_information(transactions_df)
    transactions_accounts_df = join_transactions_to_accounts(transactions_df, accounts_df)
    return apply_debit_credit_classification(transactions_accounts_df)


def normalise_transaction_information(transactions_df: DataFrame) -> DataFrame:
    """Remove special characters from the transaction_information column.

    Args:
        transactions_df: DataFrame with a 'transaction_information' column.

    Returns:
        DataFrame with an added 'transaction_information_cleaned' column
        containing only uppercase alphanumeric characters.
    """
    return transactions_df.withColumn(
        "transaction_information_cleaned",
        F.regexp_replace(F.col("transaction_information"), r"[^A-Z0-9]+", ""),
    )


def join_transactions_to_accounts(transactions_df: DataFrame, accounts_df: DataFrame) -> DataFrame:
    """Join transactions to accounts using the first 9 chars of cleaned transaction info.

    Args:
        transactions_df: DataFrame with 'transaction_information_cleaned' column.
        accounts_df: DataFrame with 'account_number' column.

    Returns:
        Inner-joined DataFrame matching transaction info prefix to account number.
    """
    return transactions_df.join(
        accounts_df,
        on=F.substring(F.col("transaction_information_cleaned"), 1, 9) == F.col("account_number"),
        how="inner",
    )


def apply_debit_credit_classification(
    transactions_accounts_df: DataFrame,
) -> DataFrame:
    """Classify transactions as debit, credit, or other based on business line ID.

    Args:
        transactions_accounts_df: Joined DataFrame with 'business_line_id' column.

    Returns:
        DataFrame with a 'business_line' column set to 'credit', 'debit', or 'other'.
    """
    return transactions_accounts_df.withColumn(
        "business_line",
        F.when(F.col("business_line_id").isin(CREDIT_BUSINESS_LINE_IDS), F.lit("credit"))
        .when(F.col("business_line_id").isin(DEBIT_BUSINESS_LINE_IDS), F.lit("debit"))
        .otherwise(F.lit("other")),
    )
