"""Tests for PyDeequ VerificationSuite constraint checking."""

import pytest
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import Row
from pyspark.sql import functions as F


class TestConstraintVerification:
    """Tests for constraint verification results."""

    def test_all_constraints_pass(self, spark):
        """All constraints pass on well-formed data."""
        df = spark.createDataFrame([Row(a="foo", b=1), Row(a="bar", b=2), Row(a="baz", b=3)])
        check = Check(spark, CheckLevel.Error, "pass-check")
        result = (
            VerificationSuite(spark)
            .onData(df)
            .addCheck(
                check.hasSize(lambda x: x == 3).isComplete("a").isUnique("a").isNonNegative("b")
            )
            .run()
        )
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
        failures = result_df.filter(F.col("constraint_status") == "Failure").count()
        assert failures == 0

    def test_completeness_fails_with_nulls(self, spark):
        """isComplete fails when column has null values."""
        df = spark.createDataFrame([Row(a="foo", b=1), Row(a=None, b=2)])
        check = Check(spark, CheckLevel.Error, "null-check")
        result = VerificationSuite(spark).onData(df).addCheck(check.isComplete("a")).run()
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
        row = result_df.first()
        assert row["constraint_status"] == "Failure"

    def test_uniqueness_fails_with_duplicates(self, spark):
        """isUnique fails when column has duplicate values."""
        df = spark.createDataFrame([Row(a="foo", b=1), Row(a="foo", b=2)])
        check = Check(spark, CheckLevel.Error, "unique-check")
        result = VerificationSuite(spark).onData(df).addCheck(check.isUnique("a")).run()
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
        row = result_df.first()
        assert row["constraint_status"] == "Failure"

    def test_size_constraint(self, spark):
        """hasSize validates row count against a condition."""
        df = spark.createDataFrame([Row(a="x"), Row(a="y")])
        check = Check(spark, CheckLevel.Error, "size-check")
        result = VerificationSuite(spark).onData(df).addCheck(check.hasSize(lambda x: x >= 5)).run()
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
        row = result_df.first()
        assert row["constraint_status"] == "Failure"

    def test_contained_in_constraint(self, spark):
        """isContainedIn passes when all values are in the allowed set."""
        df = spark.createDataFrame([Row(color="red"), Row(color="blue"), Row(color="green")])
        check = Check(spark, CheckLevel.Error, "contained-check")
        result = (
            VerificationSuite(spark)
            .onData(df)
            .addCheck(check.isContainedIn("color", ["red", "blue", "green"]))
            .run()
        )
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
        failures = result_df.filter(F.col("constraint_status") == "Failure").count()
        assert failures == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
