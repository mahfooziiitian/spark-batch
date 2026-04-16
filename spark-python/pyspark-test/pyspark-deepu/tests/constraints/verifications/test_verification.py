"""Tests for PyDeequ constraint verification."""

import pytest
from pyspark.sql import functions as F

from dpu.constraints.verifications.constraint_verification import verify_basic_constraints, verify_retail_constraints
from dpu.sample_data import create_retail_df, create_sample_df


class TestVerification:
    """Tests for VerificationSuite constraint checks."""

    def test_basic_verification_returns_results(self, spark):
        df = create_sample_df(spark)
        result_df = verify_basic_constraints(spark, df)
        assert result_df.count() >= 1

    def test_basic_verification_has_expected_columns(self, spark):
        df = create_sample_df(spark)
        result_df = verify_basic_constraints(spark, df)
        expected_cols = {
            "check",
            "check_level",
            "check_status",
            "constraint",
            "constraint_status",
            "constraint_message",
        }
        assert expected_cols.issubset(set(result_df.columns))

    def test_unique_a_passes(self, spark):
        """Column a has distinct values → isUnique should pass."""
        df = create_sample_df(spark)
        result_df = verify_basic_constraints(spark, df)
        unique_row = result_df.filter(F.col("constraint").contains("Uniqueness")).first()
        if unique_row:
            assert unique_row["constraint_status"] == "Success"

    def test_retail_verification_returns_results(self, spark):
        df = create_retail_df(spark)
        result_df = verify_retail_constraints(spark, df)
        assert result_df.count() >= 2

    def test_retail_region_is_complete(self, spark):
        """All retail rows have a region → isComplete should pass."""
        df = create_retail_df(spark)
        result_df = verify_retail_constraints(spark, df)
        region_row = result_df.filter(F.col("constraint").contains("Completeness")).first()
        if region_row:
            assert region_row["constraint_status"] == "Success"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
