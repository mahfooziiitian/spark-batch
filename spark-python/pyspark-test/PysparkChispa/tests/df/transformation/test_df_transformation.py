import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

from df.transformation import df_transformations as t
from df.helper import string_helper as sh


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("df_equality") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_modify_column_names_error(spark):
    source_data = [
        ("jose", 8),
        ("li", 23),
        ("luisa", 48),
    ]
    source_df = spark.createDataFrame(source_data, ["first.name", "person.favorite.number"])
    actual_df = t.modify_column_names(source_df, sh.dots_to_underscores)
    expected_data = [
        ("jose", 8),
        ("li", 23),
        ("luisa", 48),
    ]
    expected_df = spark.createDataFrame(expected_data, ["first_name", "person_favorite_number"])
    assert_df_equality(actual_df, expected_df)
