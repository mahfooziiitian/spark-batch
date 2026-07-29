import pytest
from chispa import assert_df_equality
from chispa.dataframe_comparer import DataFramesNotEqualError, assert_approx_df_equality

from data_frame.equality.df_equality import columns_match, row_diff, sort_columns, union_dedup


class TestSortColumns:
    """Tests for the sort_columns DataFrame utility."""

    def test_ascending(self, spark):
        source_df = spark.createDataFrame([("jose", "oak", "switch")], ["name", "tree", "gaming_system"])
        actual_df = sort_columns(source_df, "asc")
        expected_df = spark.createDataFrame([("switch", "jose", "oak")], ["gaming_system", "name", "tree"])
        assert_df_equality(actual_df, expected_df)

    def test_descending(self, spark):
        source_df = spark.createDataFrame([("jose", "oak", "switch")], ["name", "tree", "gaming_system"])
        actual_df = sort_columns(source_df, "desc")
        assert actual_df.columns == ["tree", "name", "gaming_system"]

    def test_invalid_sort_order_raises(self, spark):
        df = spark.createDataFrame([("a",)], ["col"])
        with pytest.raises(ValueError, match="valid sort orders"):
            sort_columns(df, "invalid")


class TestApproxDfEquality:
    """Tests for approximate DataFrame equality."""

    def test_within_threshold(self, spark):
        df1 = spark.createDataFrame([(1.1, "a"), (2.2, "b"), (None, None)], ["num", "letter"])
        df2 = spark.createDataFrame([(1.05, "a"), (2.13, "b"), (None, None)], ["num", "letter"])
        assert_approx_df_equality(df1, df2, 0.1)

    def test_exceeds_threshold_raises(self, spark):
        df1 = spark.createDataFrame([(1.1, "a"), (2.2, "b")], ["num", "letter"])
        df2 = spark.createDataFrame([(1.1, "a"), (5.0, "b")], ["num", "letter"])
        with pytest.raises(DataFramesNotEqualError):
            assert_approx_df_equality(df1, df2, 0.1)


class TestColumnsMatch:
    """Tests for the columns_match utility."""

    def test_matching_columns(self, spark):
        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        df2 = spark.createDataFrame([(2, "b")], ["id", "val"])
        assert columns_match(df1, df2) is True

    def test_different_columns(self, spark):
        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        df2 = spark.createDataFrame([(2, "b")], ["id", "name"])
        assert columns_match(df1, df2) is False


class TestRowDiff:
    """Tests for the row_diff utility."""

    def test_returns_rows_only_in_left(self, spark):
        left = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        right = spark.createDataFrame([(2,), (3,)], ["id"])
        result = row_diff(left, right)
        expected = spark.createDataFrame([(1,)], ["id"])
        assert_df_equality(result, expected)

    def test_identical_returns_empty(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["id"])
        result = row_diff(df, df)
        assert result.count() == 0


class TestUnionDedup:
    """Tests for the union_dedup utility."""

    def test_combines_and_deduplicates(self, spark):
        df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        df2 = spark.createDataFrame([(2, "b"), (3, "c")], ["id", "val"])
        result = union_dedup(df1, df2)
        assert result.count() == 3

    def test_no_overlap(self, spark):
        df1 = spark.createDataFrame([(1,)], ["id"])
        df2 = spark.createDataFrame([(2,)], ["id"])
        result = union_dedup(df1, df2)
        assert result.count() == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
