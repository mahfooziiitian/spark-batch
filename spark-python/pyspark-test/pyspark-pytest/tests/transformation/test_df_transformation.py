"""Tests for DataFrame text transformations."""

import pytest
from pyspark.testing import assertDataFrameEqual

from pys.transformation.df_transformation import (
    remove_extra_spaces,
    replace_nulls_with_default,
    to_lower_case,
    to_title_case,
    to_upper_case,
    trim_all_columns,
)


class TestRemoveExtraSpaces:
    """Tests for remove_extra_spaces."""

    def test_collapses_multiple_spaces(self, spark):
        """Multiple spaces are collapsed to a single space."""
        sample_data = [
            {"name": "John    D.", "age": 30},
            {"name": "Alice   G.", "age": 25},
            {"name": "Bob  T.", "age": 35},
            {"name": "Eve   A.", "age": 28},
        ]
        original_df = spark.createDataFrame(sample_data)
        transformed_df = remove_extra_spaces(original_df, "name")

        expected_data = [
            {"name": "John D.", "age": 30},
            {"name": "Alice G.", "age": 25},
            {"name": "Bob T.", "age": 35},
            {"name": "Eve A.", "age": 28},
        ]
        expected_df = spark.createDataFrame(expected_data)
        assertDataFrameEqual(transformed_df, expected_df)

    def test_no_extra_spaces_unchanged(self, spark):
        """Strings without extra spaces remain unchanged."""
        data = [{"name": "John D.", "age": 30}]
        df = spark.createDataFrame(data)
        result = remove_extra_spaces(df, "name")
        expected = spark.createDataFrame(data)
        assertDataFrameEqual(result, expected)


class TestTrimAllColumns:
    """Tests for trim_all_columns."""

    def test_trims_leading_trailing_whitespace(self, spark):
        """Leading and trailing whitespace is removed from all string columns."""
        data = [{"name": "  Alice  ", "city": " Boston "}]
        df = spark.createDataFrame(data)
        result = trim_all_columns(df)
        expected = spark.createDataFrame([{"name": "Alice", "city": "Boston"}])
        assertDataFrameEqual(result, expected)

    def test_non_string_columns_untouched(self, spark):
        """Non-string columns are not modified."""
        data = [{"name": " Bob ", "age": 25}]
        df = spark.createDataFrame(data)
        result = trim_all_columns(df)
        row = result.first()
        assert row["name"] == "Bob"
        assert row["age"] == 25


class TestCaseTransformations:
    """Tests for to_lower_case, to_upper_case, to_title_case."""

    def test_to_lower_case(self, spark):
        """Converts column values to lower case."""
        df = spark.createDataFrame([{"name": "ALICE"}, {"name": "BOB"}])
        result = to_lower_case(df, "name")
        expected = spark.createDataFrame([{"name": "alice"}, {"name": "bob"}])
        assertDataFrameEqual(result, expected)

    def test_to_upper_case(self, spark):
        """Converts column values to upper case."""
        df = spark.createDataFrame([{"name": "alice"}, {"name": "bob"}])
        result = to_upper_case(df, "name")
        expected = spark.createDataFrame([{"name": "ALICE"}, {"name": "BOB"}])
        assertDataFrameEqual(result, expected)

    def test_to_title_case(self, spark):
        """Converts column values to title case."""
        df = spark.createDataFrame([{"name": "alice smith"}, {"name": "bob jones"}])
        result = to_title_case(df, "name")
        expected = spark.createDataFrame([{"name": "Alice Smith"}, {"name": "Bob Jones"}])
        assertDataFrameEqual(result, expected)


class TestReplaceNullsWithDefault:
    """Tests for replace_nulls_with_default."""

    def test_replaces_null_values(self, spark):
        """Null values are replaced with the specified default."""
        df = spark.createDataFrame([("Alice",), (None,)], ["name"])
        result = replace_nulls_with_default(df, "name", "Unknown")
        values = [row["name"] for row in result.collect()]
        assert values == ["Alice", "Unknown"]

    def test_non_null_values_unchanged(self, spark):
        """Non-null values remain unchanged."""
        df = spark.createDataFrame([("Alice",), ("Bob",)], ["name"])
        result = replace_nulls_with_default(df, "name", "Unknown")
        values = [row["name"] for row in result.collect()]
        assert values == ["Alice", "Bob"]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
