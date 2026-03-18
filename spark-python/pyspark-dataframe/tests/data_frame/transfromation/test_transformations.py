import pytest
from chispa.dataframe_comparer import assert_df_equality

from data_frame._shared.helper import string_helpers as sh
from data_frame.transformation import transformations as t
from data_frame.transformation.transformations import sort_columns


def test_sort_columns_asc(spark):
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
    expected_df = spark.createDataFrame(
        expected_data, ["gaming_system", "name", "tree"]
    )
    assert_df_equality(actual_df, expected_df)


def test_sort_columns_desc(spark):
    source_data = [
        ("jose", "oak", "switch"),
        ("li", "redwood", "xbox"),
        ("luisa", "maple", "ps4"),
    ]
    source_df = spark.createDataFrame(source_data, ["name", "tree", "gaming_system"])
    actual_df = sort_columns(source_df, "desc")
    expected_data = [
        ("oak", "jose", "switch"),
        ("redwood", "li", "xbox"),
        ("maple", "luisa", "ps4"),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["tree", "name", "gaming_system"]
    )
    assert_df_equality(actual_df, expected_df)


def test_modify_column_names(spark):
    source_data = [("jose", 8), ("li", 23), ("luisa", 48)]
    source_df = spark.createDataFrame(
        source_data, ["first.name", "person.favorite.number"]
    )
    actual_df = t.modify_column_names(source_df, sh.dots_to_underscores)
    expected_data = [("jose", 8), ("li", 23), ("luisa", 48)]
    expected_df = spark.createDataFrame(
        expected_data, ["first_name", "person_favorite_number"]
    )
    assert_df_equality(actual_df, expected_df)


if __name__ == "__main__":
    import pytest as _pytest

    _pytest.main([__file__, "-v", "--tb=short"])
