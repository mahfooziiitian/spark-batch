import pytest
from pyspark.sql import SparkSession

from pairrdd.parirdd1 import checkVowelFunction, checkVowelFunctionTuple


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("functions") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_checkVowelFunction_ThenYes():
    assert checkVowelFunctionTuple("a") == ("a", 1)


def test_checkVowelFunctionNo_ThenNo():
    assert checkVowelFunctionTuple("b") == ("b", 0)


def test_checkVowelFunction_pair_rdd_tuple(spark):
    pythonList = ['b', 'd', 'm', 't', 'e', 'u']
    rdd1 = spark.sparkContext.parallelize(pythonList).map(lambda letter: checkVowelFunctionTuple(letter))
    assert rdd1.collect() == [('b', 0), ('d', 0), ('m', 0), ('t', 0), ('e', 1), ('u', 1)]


def test_checkVowelFunction_pair_rdd(spark):
    pythonList = ['b', 'd', 'm', 't', 'e', 'u']
    rdd1 = spark.sparkContext.parallelize(pythonList).map(lambda letter: (letter, checkVowelFunction(letter)))
    assert rdd1.collect() == [('b', 0), ('d', 0), ('m', 0), ('t', 0), ('e', 1), ('u', 1)]
