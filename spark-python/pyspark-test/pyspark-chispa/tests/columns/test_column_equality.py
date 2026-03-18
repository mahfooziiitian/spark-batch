import pytest
from chispa.column_comparer import assert_column_equality
from pyspark.sql import functions as F

from data_frame.columns.column_equality import (
    extract_email_domain,
    normalize_whitespace,
    null_safe_trim,
    remove_non_word_characters,
    title_case,
)


class TestRemoveNonWordCharacters:
    """Tests for remove_non_word_characters column transformation."""

    def test_removes_special_characters(self, spark):
        data = [("jo&&se", "jose"), ("**li**", "li"), ("#::luisa", "luisa")]
        df = spark.createDataFrame(data, ["name", "expected_name"]).withColumn(
            "clean_name", remove_non_word_characters(F.col("name"))
        )
        assert_column_equality(df, "clean_name", "expected_name")

    def test_preserves_digits(self, spark):
        data = [("matt7", "matt7"), ("abc123", "abc123"), ("42", "42")]
        df = spark.createDataFrame(data, ["name", "expected_name"]).withColumn(
            "clean_name", remove_non_word_characters(F.col("name"))
        )
        assert_column_equality(df, "clean_name", "expected_name")

    def test_handles_nulls(self, spark):
        data = [(None, None), ("hello", "hello")]
        df = spark.createDataFrame(data, ["name", "expected_name"]).withColumn(
            "clean_name", remove_non_word_characters(F.col("name"))
        )
        assert_column_equality(df, "clean_name", "expected_name")

    def test_preserves_whitespace(self, spark):
        data = [("hello world", "hello world"), ("a b c", "a b c")]
        df = spark.createDataFrame(data, ["name", "expected_name"]).withColumn(
            "clean_name", remove_non_word_characters(F.col("name"))
        )
        assert_column_equality(df, "clean_name", "expected_name")

    def test_empty_string(self, spark):
        data = [("", "")]
        df = spark.createDataFrame(data, ["name", "expected_name"]).withColumn(
            "clean_name", remove_non_word_characters(F.col("name"))
        )
        assert_column_equality(df, "clean_name", "expected_name")

    def test_only_special_characters(self, spark):
        data = [("@#$%^&*", ""), ("!!!", "")]
        df = spark.createDataFrame(data, ["name", "expected_name"]).withColumn(
            "clean_name", remove_non_word_characters(F.col("name"))
        )
        assert_column_equality(df, "clean_name", "expected_name")

    def test_mixed_special_and_word_characters(self, spark):
        data = [("bill&", "bill"), ("isabela*", "isabela"), ("[test]", "test")]
        df = spark.createDataFrame(data, ["name", "expected_name"]).withColumn(
            "clean_name", remove_non_word_characters(F.col("name"))
        )
        assert_column_equality(df, "clean_name", "expected_name")


class TestNormalizeWhitespace:
    """Tests for normalize_whitespace column transformation."""

    def test_collapses_multiple_spaces(self, spark):
        data = [("hello   world", "hello world"), ("a  b  c", "a b c")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn(
            "result", normalize_whitespace(F.col("text"))
        )
        assert_column_equality(df, "result", "expected")

    def test_trims_leading_and_trailing(self, spark):
        data = [("  hello  ", "hello"), ("  a b  ", "a b")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn(
            "result", normalize_whitespace(F.col("text"))
        )
        assert_column_equality(df, "result", "expected")

    def test_handles_tabs_and_newlines(self, spark):
        data = [("hello\t\tworld", "hello world"), ("a\n\nb", "a b")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn(
            "result", normalize_whitespace(F.col("text"))
        )
        assert_column_equality(df, "result", "expected")

    def test_null_passthrough(self, spark):
        data = [("hello", "hello"), (None, None)]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn(
            "result", normalize_whitespace(F.col("text"))
        )
        assert_column_equality(df, "result", "expected")

    def test_already_normalized(self, spark):
        data = [("no change", "no change")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn(
            "result", normalize_whitespace(F.col("text"))
        )
        assert_column_equality(df, "result", "expected")


class TestExtractEmailDomain:
    """Tests for extract_email_domain column transformation."""

    def test_extracts_domain(self, spark):
        data = [("alice@example.com", "example.com"), ("bob@work.org", "work.org")]
        df = spark.createDataFrame(data, ["email", "expected"]).withColumn(
            "result", extract_email_domain(F.col("email"))
        )
        assert_column_equality(df, "result", "expected")

    def test_no_at_sign_returns_null(self, spark):
        data = [("alice@ok.com", "ok.com"), ("invalid-email", None), ("noatsign", None)]
        df = spark.createDataFrame(data, ["email", "expected"]).withColumn(
            "result", extract_email_domain(F.col("email"))
        )
        assert_column_equality(df, "result", "expected")

    def test_null_input(self, spark):
        data = [("bob@test.io", "test.io"), (None, None)]
        df = spark.createDataFrame(data, ["email", "expected"]).withColumn(
            "result", extract_email_domain(F.col("email"))
        )
        assert_column_equality(df, "result", "expected")


class TestTitleCase:
    """Tests for title_case column transformation."""

    def test_basic_conversion(self, spark):
        data = [("hello world", "Hello World"), ("john doe", "John Doe")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn(
            "result", title_case(F.col("text"))
        )
        assert_column_equality(df, "result", "expected")

    def test_already_title_case(self, spark):
        data = [("Hello", "Hello")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn(
            "result", title_case(F.col("text"))
        )
        assert_column_equality(df, "result", "expected")

    def test_all_uppercase(self, spark):
        data = [("HELLO WORLD", "Hello World")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn(
            "result", title_case(F.col("text"))
        )
        assert_column_equality(df, "result", "expected")


class TestNullSafeTrim:
    """Tests for null_safe_trim column transformation."""

    def test_trims_whitespace(self, spark):
        data = [("  hello  ", "hello"), (" world ", "world")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn(
            "result", null_safe_trim(F.col("text"))
        )
        assert_column_equality(df, "result", "expected")

    def test_preserves_null(self, spark):
        data = [(None, None), ("ok", "ok")]
        df = spark.createDataFrame(data, ["text", "expected"]).withColumn(
            "result", null_safe_trim(F.col("text"))
        )
        assert_column_equality(df, "result", "expected")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

