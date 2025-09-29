import pytest
from pyspark.sql import SparkSession

from pair_rdd.pair_rdd import check_vowel_function, check_vowel_function_tuple


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("functions").getOrCreate()
    yield spark
    spark.stop()


def test_checkVowelFunction_ThenYes():
    assert check_vowel_function("a") == 1


def test_checkVowelFunctionNo_ThenNo():
    assert check_vowel_function_tuple("b") == ("b", 0)


def test_checkVowelFunction_pair_rdd_tuple(spark):
    pythonList = ["b", "d", "m", "t", "e", "u"]
    rdd1 = spark.sparkContext.parallelize(pythonList).map(
        lambda letter: check_vowel_function_tuple(letter)
    )
    assert rdd1.collect() == [
        ("b", 0),
        ("d", 0),
        ("m", 0),
        ("t", 0),
        ("e", 1),
        ("u", 1),
    ]


def test_checkVowelFunction_pair_rdd(spark):
    pythonList = ["b", "d", "m", "t", "e", "u"]
    rdd1 = spark.sparkContext.parallelize(pythonList).map(
        lambda letter: (letter, check_vowel_function(letter))
    )
    assert rdd1.collect() == [
        ("b", 0),
        ("d", 0),
        ("m", 0),
        ("t", 0),
        ("e", 1),
        ("u", 1),
    ]
