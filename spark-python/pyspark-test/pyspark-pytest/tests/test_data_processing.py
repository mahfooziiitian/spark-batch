"""Tests for the data processing pipeline."""

import pytest

from pys.data_processing import (
    apply_debit_credit_classification,
    classify_debit_credit_transactions,
    join_transactions_to_accounts,
    normalise_transaction_information,
)


class TestDataProcessing:
    """Tests for transaction classification pipeline."""

    def test_classify_debit_credit_transactions(self, spark):
        """End-to-end classification joins and labels correctly."""
        transactions_df = spark.createDataFrame(
            data=[
                ("1", 1000.00, "123-456-789"),
                ("3", 3000.00, "222222222EUR"),
            ],
            schema=["transaction_id", "amount", "transaction_information"],
        )

        accounts_df = spark.createDataFrame(
            data=[
                ("123456789", "101"),
                ("222222222", "202"),
                ("000000000", "302"),
            ],
            schema=["account_number", "business_line_id"],
        )

        output = classify_debit_credit_transactions(transactions_df, accounts_df)

        expected_classifications = ["credit", "debit"]
        assert output.count() == 2
        assert [row.business_line for row in output.collect()] == expected_classifications

    def test_normalise_transaction_information(self, spark):
        """Special characters are stripped from transaction info."""
        data = ["123-456-789", "123456789", "123456789EUR", "TEXT*?WITH.*CHARACTERS"]
        test_df = spark.createDataFrame(data, "string").toDF("transaction_information")

        expected = ["123456789", "123456789", "123456789EUR", "TEXTWITHCHARACTERS"]
        output = normalise_transaction_information(test_df)
        assert [row.transaction_information_cleaned for row in output.collect()] == expected

    def test_join_transactions_to_accounts(self, spark):
        """Transactions join to accounts on first 9 chars."""
        transactions_df = spark.createDataFrame(["123456789", "222222222EUR"], "string").toDF(
            "transaction_information_cleaned"
        )

        accounts_df = spark.createDataFrame(["123456789", "222222222", "000000000"], "string").toDF(
            "account_number"
        )

        output = join_transactions_to_accounts(transactions_df, accounts_df)
        assert output.count() == 2

    def test_apply_debit_credit_classification(self, spark):
        """Business line IDs map to credit, debit, or other."""
        data = ["101", "202", "000"]
        df = spark.createDataFrame(data, "string").toDF("business_line_id")
        output = apply_debit_credit_classification(df)

        expected = ["credit", "debit", "other"]
        assert [row.business_line for row in output.collect()] == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
