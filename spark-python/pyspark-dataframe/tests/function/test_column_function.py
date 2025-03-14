import pytest
from chispa import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from function.functions import remove_non_word_characters


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("functions") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_remove_non_word_characters(spark):
    data = [
        ("jo&&se", "jose"),
        ("**li**", "li"),
        ("#::luisa", "luisa"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["name", "expected_name"]) \
        .withColumn("clean_name", remove_non_word_characters(f.col("name")))
    assert_column_equality(df, "clean_name", "expected_name")


def test_remove_non_word_characters_nice_error(spark):
    data = [
        ("matt7", "matt"),
        ("bill&", "bill"),
        ("isabela*", "isabela"),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["name", "expected_name"])\
        .withColumn("clean_name", remove_non_word_characters(f.col("name")))
    assert_column_equality(df, "clean_name", "expected_name")

