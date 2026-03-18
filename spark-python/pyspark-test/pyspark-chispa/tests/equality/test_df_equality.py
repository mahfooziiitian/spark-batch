import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from data_frame.equality.df_equality import columns_match, row_diff, sort_columns, union_dedup


class TestSortColumns:
    """Tests for sort_columns DataFrame utility."""

    def test_ascending(self, spark):
        source_data = [
            ("jose", "oak", "switch"),
            ("li", "redwood", "xbox"),
            ("luisa", "maple", "ps4"),
        ]
        source_df = spark.createDataFrame(source_data, ["name", "tree", "gaming_system"])
        actual_df = sort_columns(source_df, "asc")
        expected_data = [
            ("switch", "jose", "oak"),
            ("xbox", "li", "redwood"),
            ("ps4", "luisa", "maple"),
        ]
        expected_df = spark.createDataFrame(expected_data, ["gaming_system", "name", "tree"])
        assert_df_equality(actual_df, expected_df)

    def test_descending(self, spark):
        source_data = [("a", 1, True), ("b", 2, False)]
        source_df = spark.createDataFrame(source_data, ["alpha", "beta", "gamma"])
        actual_df = sort_columns(source_df, "desc")
        assert actual_df.columns == ["gamma", "beta", "alpha"]
        assert actual_df.count() == 2

    def test_invalid_sort_order_raises(self, spark):
        df = spark.createDataFrame([(1,)], ["col"])
        with pytest.raises(ValueError, match="valid sort orders"):
            sort_columns(df, "random")

    def test_single_column(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["only"])
        result = sort_columns(df, "asc")
        assert result.columns == ["only"]
        assert result.count() == 2


class TestColumnsMatch:
    """Tests for columns_match comparison utility."""

    def test_matching_columns(self, spark):
        df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
        df2 = spark.createDataFrame([(2, "b")], ["id", "name"])
        assert columns_match(df1, df2) is True

    def test_different_column_names(self, spark):
        df1 = spark.createDataFrame([(1,)], ["id"])
        df2 = spark.createDataFrame([(1,)], ["key"])
        assert columns_match(df1, df2) is False

    def test_different_column_order(self, spark):
        df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
        df2 = spark.createDataFrame([("b", 2)], ["name", "id"])
        assert columns_match(df1, df2) is False

    def test_different_column_count(self, spark):
        df1 = spark.createDataFrame([(1,)], ["id"])
        df2 = spark.createDataFrame([(1, "a")], ["id", "name"])
        assert columns_match(df1, df2) is False


class TestRowDiff:
    """Tests for row_diff set difference utility."""

    def test_returns_rows_only_in_left(self, spark):
        left = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        right = spark.createDataFrame([(2,), (3,)], ["id"])
        result = row_diff(left, right)
        expected = spark.createDataFrame([(1,)], ["id"])
        assert_df_equality(result, expected)

    def test_identical_dataframes_returns_empty(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["id"])
        result = row_diff(df, df)
        assert result.count() == 0

    def test_no_overlap(self, spark):
        left = spark.createDataFrame([(1,), (2,)], ["id"])
        right = spark.createDataFrame([(3,), (4,)], ["id"])
        result = row_diff(left, right)
        assert_df_equality(result, left, ignore_row_order=True)


class TestUnionDedup:
    """Tests for union_dedup merge utility."""

    def test_union_with_duplicates(self, spark):
        df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        df2 = spark.createDataFrame([(2, "b"), (3, "c")], ["id", "val"])
        result = union_dedup(df1, df2)
        assert result.count() == 3

    def test_no_overlap(self, spark):
        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        df2 = spark.createDataFrame([(2, "b")], ["id", "val"])
        result = union_dedup(df1, df2)
        expected = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        assert_df_equality(result, expected, ignore_row_order=True)

    def test_identical_dataframes(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        result = union_dedup(df, df)
        assert_df_equality(result, df)


class TestApproxDfEquality:
    """Tests demonstrating chispa approximate DataFrame equality."""

    def test_within_threshold(self, spark):
        from chispa.dataframe_comparer import assert_approx_df_equality

        data1 = [(1.1, "a"), (2.2, "b"), (3.3, "c"), (None, None)]
        df1 = spark.createDataFrame(data1, ["num", "letter"])
        data2 = [(1.05, "a"), (2.13, "b"), (3.3, "c"), (None, None)]
        df2 = spark.createDataFrame(data2, ["num", "letter"])
        assert_approx_df_equality(df1, df2, 0.1)

    def test_exceeds_threshold_raises(self, spark):
        from chispa.dataframe_comparer import assert_approx_df_equality

        data1 = [(1.1, "a"), (2.2, "b")]
        df1 = spark.createDataFrame(data1, ["num", "letter"])
        data2 = [(1.1, "a"), (5.0, "b")]
        df2 = spark.createDataFrame(data2, ["num", "letter"])
        with pytest.raises(Exception):
            assert_approx_df_equality(df1, df2, 0.1)

    def test_exact_match(self, spark):
        from chispa.dataframe_comparer import assert_approx_df_equality

        data = [(1.0, "x"), (2.0, "y")]
        df1 = spark.createDataFrame(data, ["num", "letter"])
        df2 = spark.createDataFrame(data, ["num", "letter"])
        assert_approx_df_equality(df1, df2, 0.0)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

