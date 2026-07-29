import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from data_frame.helper.string_helper import dots_to_underscores
from data_frame.transformation.df_transformations import (
    deduplicate,
    filter_nulls,
    modify_column_names,
    with_row_number,
    with_running_total,
)


class TestModifyColumnNames:
    """Tests for the modify_column_names transformation."""

    def test_dots_to_underscores(self, spark):
        source_df = spark.createDataFrame(
            [("jose", 8), ("li", 23), ("luisa", 48)],
            ["first.name", "person.favorite.number"],
        )
        actual_df = modify_column_names(source_df, dots_to_underscores)
        expected_df = spark.createDataFrame(
            [("jose", 8), ("li", 23), ("luisa", 48)],
            ["first_name", "person_favorite_number"],
        )
        assert_df_equality(actual_df, expected_df)

    def test_uppercase(self, spark):
        df = spark.createDataFrame([("a",)], ["name"])
        result = modify_column_names(df, str.upper)
        assert result.columns == ["NAME"]

    def test_empty_dataframe(self, spark):
        from pyspark.sql.types import LongType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("a.b", StringType()),
                StructField("c.d", LongType()),
            ]
        )
        source_df = spark.createDataFrame([], schema)
        actual_df = modify_column_names(source_df, dots_to_underscores)
        assert actual_df.columns == ["a_b", "c_d"]
        assert actual_df.count() == 0


class TestWithRowNumber:
    """Tests for the with_row_number transformation."""

    def test_adds_sequential_numbers(self, spark):
        df = spark.createDataFrame([(3, "c"), (1, "a"), (2, "b")], ["id", "val"])
        result = with_row_number(df, order_col="id")
        rows = result.orderBy("id").select("row_number").collect()
        assert [r["row_number"] for r in rows] == [1, 2, 3]

    def test_custom_alias(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        result = with_row_number(df, order_col="id", alias="rn")
        assert "rn" in result.columns


class TestWithRunningTotal:
    """Tests for the with_running_total transformation."""

    def test_global_running_total(self, spark):
        df = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["step", "val"])
        result = with_running_total(df, value_col="val", order_col="step")
        rows = result.orderBy("step").select("running_total").collect()
        assert [r["running_total"] for r in rows] == [10, 30, 60]

    def test_partitioned_running_total(self, spark):
        df = spark.createDataFrame(
            [("A", 1, 10), ("A", 2, 20), ("B", 1, 5), ("B", 2, 15)],
            ["region", "step", "val"],
        )
        result = with_running_total(df, value_col="val", order_col="step", partition_col="region")
        a_rows = result.filter(F.col("region") == "A").orderBy("step").select("running_total").collect()
        assert [r["running_total"] for r in a_rows] == [10, 30]


class TestDeduplicate:
    """Tests for the deduplicate transformation."""

    def test_keep_first(self, spark):
        df = spark.createDataFrame(
            [(1, "old", 1), (1, "new", 2), (2, "only", 1)],
            ["id", "val", "ts"],
        )
        result = deduplicate(df, subset=["id"], order_col="ts", keep="first")
        assert result.count() == 2
        row = result.filter(F.col("id") == 1).first()
        assert row["val"] == "old"

    def test_keep_last(self, spark):
        df = spark.createDataFrame(
            [(1, "old", 1), (1, "new", 2)],
            ["id", "val", "ts"],
        )
        result = deduplicate(df, subset=["id"], order_col="ts", keep="last")
        row = result.first()
        assert row["val"] == "new"

    def test_invalid_keep_raises(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        with pytest.raises(ValueError, match="must be 'first' or 'last'"):
            deduplicate(df, subset=["id"], order_col="id", keep="middle")

    def test_no_duplicates_unchanged(self, spark):
        df = spark.createDataFrame([(1, "a", 1), (2, "b", 2)], ["id", "val", "ts"])
        result = deduplicate(df, subset=["id"], order_col="ts")
        assert result.count() == 2


class TestFilterNulls:
    """Tests for the filter_nulls transformation."""

    def test_removes_null_rows(self, spark):
        df = spark.createDataFrame(
            [(1, "a"), (None, "b"), (3, None), (4, "d")],
            ["id", "name"],
        )
        result = filter_nulls(df, columns=["id", "name"])
        assert result.count() == 2

    def test_all_columns_present(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        result = filter_nulls(df, columns=["id", "name"])
        assert result.count() == 2

    def test_empty_dataframe(self, spark):
        from pyspark.sql.types import LongType, StringType, StructField, StructType

        schema = StructType([StructField("id", LongType()), StructField("name", StringType())])
        df = spark.createDataFrame([], schema)
        result = filter_nulls(df, columns=["id", "name"])
        assert result.count() == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
