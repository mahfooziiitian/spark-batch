import pytest
from chispa.column_comparer import assert_column_equality
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from data_frame.columns.column_equality import (
    extract_email_domain,
    normalize_whitespace,
    null_safe_trim,
    remove_non_word_characters,
    title_case,
)


class TestRemoveNonWordCharacters:
    """Tests for the remove_non_word_characters column function."""

    def test_removes_special_characters(self, spark):
        data = [("jo&&se", "jose"), ("**li**", "li"), ("#::luisa", "luisa"), (None, None)]
        df = spark.createDataFrame(data, ["name", "expected"]).withColumn(
            "actual", remove_non_word_characters(F.col("name"))
        )
        assert_column_equality(df, "actual", "expected")

    def test_preserves_digits(self, spark):
        data = [("matt7", "matt7"), ("abc123", "abc123"), ("7!", "7")]
        df = spark.createDataFrame(data, ["name", "expected"]).withColumn(
            "actual", remove_non_word_characters(F.col("name"))
        )
        assert_column_equality(df, "actual", "expected")

    def test_empty_string(self, spark):
        data = [("", ""), ("@#$", "")]
        df = spark.createDataFrame(data, ["name", "expected"]).withColumn(
            "actual", remove_non_word_characters(F.col("name"))
        )
        assert_column_equality(df, "actual", "expected")


class TestNormalizeWhitespace:
    """Tests for the normalize_whitespace column function."""

    def test_collapses_multiple_spaces(self, spark):
        data = [("hello   world", "hello world"), ("  padded  ", "padded")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn("actual", normalize_whitespace(F.col("text")))
        assert_column_equality(df, "actual", "expected")

    def test_handles_tabs_and_newlines(self, spark):
        data = [("a\t\tb", "a b"), ("x\ny", "x y")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn("actual", normalize_whitespace(F.col("text")))
        assert_column_equality(df, "actual", "expected")

    def test_null_propagation(self, spark):
        schema = StructType([StructField("text", StringType()), StructField("expected", StringType())])
        df = spark.createDataFrame([(None, None)], schema).withColumn("actual", normalize_whitespace(F.col("text")))
        assert_column_equality(df, "actual", "expected")


class TestExtractEmailDomain:
    """Tests for the extract_email_domain column function."""

    def test_valid_emails(self, spark):
        data = [("alice@example.com", "example.com"), ("bob@work.org", "work.org")]
        df = spark.createDataFrame(data, ["email", "expected"]).withColumn(
            "actual", extract_email_domain(F.col("email"))
        )
        assert_column_equality(df, "actual", "expected")

    def test_no_at_sign_returns_null(self, spark):
        schema = StructType([StructField("email", StringType()), StructField("expected", StringType())])
        df = spark.createDataFrame([("invalid-email", None), (None, None)], schema).withColumn(
            "actual", extract_email_domain(F.col("email"))
        )
        assert_column_equality(df, "actual", "expected")


class TestTitleCase:
    """Tests for the title_case column function."""

    def test_basic_conversion(self, spark):
        data = [("hello world", "Hello World"), ("HELLO WORLD", "Hello World")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn("actual", title_case(F.col("text")))
        assert_column_equality(df, "actual", "expected")

    def test_null_propagation(self, spark):
        schema = StructType([StructField("text", StringType()), StructField("expected", StringType())])
        df = spark.createDataFrame([(None, None)], schema).withColumn("actual", title_case(F.col("text")))
        assert_column_equality(df, "actual", "expected")


class TestNullSafeTrim:
    """Tests for the null_safe_trim column function."""

    def test_trims_spaces(self, spark):
        data = [("  hello  ", "hello"), ("  padded  ", "padded")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn("actual", null_safe_trim(F.col("text")))
        assert_column_equality(df, "actual", "expected")

    def test_preserves_null(self, spark):
        schema = StructType([StructField("text", StringType()), StructField("expected", StringType())])
        df = spark.createDataFrame([(None, None)], schema).withColumn("actual", null_safe_trim(F.col("text")))
        assert_column_equality(df, "actual", "expected")

    def test_already_trimmed(self, spark):
        data = [("hello", "hello")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn("actual", null_safe_trim(F.col("text")))
        assert_column_equality(df, "actual", "expected")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
