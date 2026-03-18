import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from data_frame.helper import string_helper as sh
from data_frame.transformation import df_transformations as t


class TestModifyColumnNames:
    """Tests for modify_column_names DataFrame transformation."""

    def test_dots_to_underscores(self, spark):
        source_df = spark.createDataFrame(
            [("jose", 8), ("li", 23), ("luisa", 48)],
            ["first.name", "person.favorite.number"],
        )
        actual_df = t.modify_column_names(source_df, sh.dots_to_underscores)
        expected_df = spark.createDataFrame(
            [("jose", 8), ("li", 23), ("luisa", 48)],
            ["first_name", "person_favorite_number"],
        )
        assert_df_equality(actual_df, expected_df)

    def test_columns_without_dots_unchanged(self, spark):
        source_df = spark.createDataFrame([(1, "a")], ["id", "name"])
        actual_df = t.modify_column_names(source_df, sh.dots_to_underscores)
        assert actual_df.columns == ["id", "name"]
        assert actual_df.count() == 1

    def test_with_uppercase_transform(self, spark):
        source_df = spark.createDataFrame([(1, "a")], ["id", "name"])
        actual_df = t.modify_column_names(source_df, str.upper)
        assert actual_df.columns == ["ID", "NAME"]

    def test_single_column(self, spark):
        source_df = spark.createDataFrame([(1,), (2,)], ["val.ue"])
        actual_df = t.modify_column_names(source_df, sh.dots_to_underscores)
        assert actual_df.columns == ["val_ue"]
        assert actual_df.count() == 2

    def test_empty_dataframe(self, spark):
        from pyspark.sql.types import LongType, StringType, StructField, StructType

        schema = StructType([StructField("a.b", StringType()), StructField("c.d", LongType())])
        source_df = spark.createDataFrame([], schema)
        actual_df = t.modify_column_names(source_df, sh.dots_to_underscores)
        assert actual_df.columns == ["a_b", "c_d"]
        assert actual_df.count() == 0


class TestWithRowNumber:
    """Tests for with_row_number transformation."""

    def test_sequential_numbering(self, spark):
        df = spark.createDataFrame([(3, "c"), (1, "a"), (2, "b")], ["id", "val"])
        result = t.with_row_number(df, "id")
        rows = result.orderBy("id").select("row_number").collect()
        assert [r["row_number"] for r in rows] == [1, 2, 3]

    def test_custom_alias(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        result = t.with_row_number(df, "id", alias="rn")
        assert "rn" in result.columns

    def test_preserves_original_columns(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        result = t.with_row_number(df, "id")
        assert set(result.columns) == {"id", "val", "row_number"}


class TestWithRunningTotal:
    """Tests for with_running_total window transformation."""

    def test_running_total(self, spark):
        data = [(1, 10), (2, 20), (3, 30)]
        df = spark.createDataFrame(data, ["step", "val"])
        result = t.with_running_total(df, "val", "step")
        last_row = result.orderBy(F.desc("step")).first()
        assert last_row["running_total"] == 60

    def test_single_row(self, spark):
        df = spark.createDataFrame([(1, 100)], ["step", "val"])
        result = t.with_running_total(df, "val", "step")
        assert result.first()["running_total"] == 100

    def test_partitioned_running_total(self, spark):
        data = [("A", 1, 10), ("A", 2, 20), ("B", 1, 100), ("B", 2, 200)]
        df = spark.createDataFrame(data, ["group", "step", "val"])
        result = t.with_running_total(df, "val", "step", partition_col="group")
        row_a = result.filter((F.col("group") == "A") & (F.col("step") == 2)).first()
        row_b = result.filter((F.col("group") == "B") & (F.col("step") == 2)).first()
        assert row_a["running_total"] == 30
        assert row_b["running_total"] == 300


class TestDeduplicate:
    """Tests for deduplicate transformation."""

    def test_keeps_first_occurrence(self, spark):
        data = [(1, "a", 1), (1, "b", 2), (2, "c", 1)]
        df = spark.createDataFrame(data, ["id", "val", "ts"])
        result = t.deduplicate(df, subset=["id"], order_col="ts", keep="first")
        assert result.count() == 2
        row = result.filter(F.col("id") == 1).first()
        assert row["val"] == "a"

    def test_keeps_last_occurrence(self, spark):
        data = [(1, "a", 1), (1, "b", 2), (2, "c", 1)]
        df = spark.createDataFrame(data, ["id", "val", "ts"])
        result = t.deduplicate(df, subset=["id"], order_col="ts", keep="last")
        assert result.count() == 2
        row = result.filter(F.col("id") == 1).first()
        assert row["val"] == "b"

    def test_no_duplicates(self, spark):
        data = [(1, "a", 1), (2, "b", 2)]
        df = spark.createDataFrame(data, ["id", "val", "ts"])
        result = t.deduplicate(df, subset=["id"], order_col="ts")
        assert_df_equality(result, df)

    def test_invalid_keep_raises(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        with pytest.raises(ValueError, match="'first' or 'last'"):
            t.deduplicate(df, subset=["id"], order_col="id", keep="middle")


class TestFilterNulls:
    """Tests for filter_nulls transformation."""

    def test_removes_rows_with_nulls(self, spark):
        data = [(1, "a"), (None, "b"), (3, None)]
        df = spark.createDataFrame(data, ["id", "name"])
        result = t.filter_nulls(df, ["id", "name"])
        assert result.count() == 1
        assert result.first()["id"] == 1

    def test_no_nulls_returns_all(self, spark):
        data = [(1, "a"), (2, "b")]
        df = spark.createDataFrame(data, ["id", "name"])
        result = t.filter_nulls(df, ["id", "name"])
        assert_df_equality(result, df)

    def test_single_column_filter(self, spark):
        data = [(1, "a"), (None, "b"), (3, None)]
        df = spark.createDataFrame(data, ["id", "name"])
        result = t.filter_nulls(df, ["id"])
        assert result.count() == 2

    def test_empty_dataframe(self, spark):
        from pyspark.sql.types import LongType, StructField, StructType

        schema = StructType([StructField("id", LongType())])
        df = spark.createDataFrame([], schema)
        result = t.filter_nulls(df, ["id"])
        assert result.count() == 0


class TestDotsToUnderscores:
    """Tests for dots_to_underscores pure Python helper."""

    def test_single_dot(self):
        assert sh.dots_to_underscores("first.name") == "first_name"

    def test_multiple_dots(self):
        assert sh.dots_to_underscores("a.b.c.d") == "a_b_c_d"

    def test_no_dots(self):
        assert sh.dots_to_underscores("hello") == "hello"

    def test_empty_string(self):
        assert sh.dots_to_underscores("") == ""

    def test_only_dots(self):
        assert sh.dots_to_underscores("...") == "___"


class TestSnakeCase:
    """Tests for snake_case pure Python helper."""

    def test_spaces(self):
        assert sh.snake_case("Hello World") == "hello_world"

    def test_hyphens(self):
        assert sh.snake_case("my-column-name") == "my_column_name"

    def test_dots(self):
        assert sh.snake_case("first.last") == "first_last"

    def test_mixed_separators(self):
        assert sh.snake_case("My Column.Name-here") == "my_column_name_here"

    def test_already_snake_case(self):
        assert sh.snake_case("already_snake") == "already_snake"

    def test_empty_string(self):
        assert sh.snake_case("") == ""


class TestTruncate:
    """Tests for truncate pure Python helper."""

    def test_within_limit(self):
        assert sh.truncate("hello", 10) == "hello"

    def test_at_exact_limit(self):
        assert sh.truncate("hello", 5) == "hello"

    def test_exceeds_limit(self):
        assert sh.truncate("hello world", 8) == "hello..."

    def test_custom_suffix(self):
        assert sh.truncate("hello world", 8, suffix="~") == "hello w~"

    def test_max_length_less_than_suffix_raises(self):
        with pytest.raises(ValueError, match="max_length"):
            sh.truncate("hello", 2, suffix="...")

    def test_empty_string(self):
        assert sh.truncate("", 5) == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

