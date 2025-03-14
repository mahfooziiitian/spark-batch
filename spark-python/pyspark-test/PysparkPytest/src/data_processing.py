import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def classify_debit_credit_transactions(
    transactions_df: DataFrame, accounts_df: DataFrame
) -> DataFrame:
    """Join transactions with account information and classify as debit/credit"""

    transactions_df = normalise_transaction_information(transactions_df)

    transactions_accounts_df = join_transactions_df_to_accounts_df(
        transactions_df, accounts_df
    )

    transactions_accounts_df = apply_debit_credit_business_classification(
        transactions_accounts_df
    )

    return transactions_accounts_df


def normalise_transaction_information(transactions_df: DataFrame) -> DataFrame:
    """Remove special characters from transaction information"""
    return transactions_df.withColumn(
        "transaction_information_cleaned",
        F.regexp_replace(F.col("transaction_information"), r"[^A-Z0-9]+", ""),
    )


def join_transactions_df_to_accounts_df(
    transactions_df: DataFrame, accounts_df: DataFrame
) -> DataFrame:
    """Join transactions to accounts information"""
    return transactions_df.join(
        accounts_df,
        on=F.substring(F.col("transaction_information_cleaned"), 1, 9)
        == F.col("account_number"),
        how="inner",
    )


def apply_debit_credit_business_classification(
    transactions_accounts_df: DataFrame,
) -> DataFrame:
    """Classify transactions as coming from debit or credit account customers"""
    credit_account_ids = ["101", "102", "103"]
    debit_account_ids = ["202", "202", "203"]

    return transactions_accounts_df.withColumn(
        "business_line",
        F.when(F.col("business_line_id").isin(credit_account_ids), F.lit("credit"))
        .when(F.col("business_line_id").isin(debit_account_ids), F.lit("debit"))
        .otherwise(F.lit("other")),
    )
