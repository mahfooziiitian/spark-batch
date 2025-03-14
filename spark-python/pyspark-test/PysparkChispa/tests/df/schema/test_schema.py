import pytest
from chispa.schema_comparer import assert_schema_equality
from chispa import assert_df_equality
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("functions") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_schema_mismatch_message(spark):
    data1 = [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["num", "letter"])
    data2 = [
        (1, 6),
        (2, 7),
        (3, 8),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "num2"])
    assert_df_equality(df1, df2)


def test_schema_match_message(spark):
    data1 = [
        (1, 4),
        (2, 3),
        (3, 5),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["num", "letter"])
    data2 = [
        (1, 6),
        (2, 7),
        (3, 8),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "letter"])
    assert_schema_equality(df1, df2)
