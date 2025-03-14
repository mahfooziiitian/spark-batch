import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("functions") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_set_operation(spark):
    data2001 = ['RIN1', 'RIN2', 'RIN3', 'RIN4', 'RIN5', 'RIN6', 'RIN7']
    data2002 = ['RIN3', 'RIN4', 'RIN7', 'RIN8', 'RIN9']
    data2003 = ['RIN4', 'RIN8', 'RIN10', 'RIN11', 'RIN12']
    sc = spark.sparkContext
    parData2001 = sc.parallelize(data2001, 2)
    parData2002 = sc.parallelize(data2002, 2)
    parData2003 = sc.parallelize(data2003, 2)
    allResearches = parData2001.union(parData2002).union(parData2003)
    allUniqueResearches = allResearches.distinct()
    expected_data = ['RIN1', 'RIN10', 'RIN12',
                     'RIN2', 'RIN3', 'RIN5',
                     'RIN8', 'RIN4', 'RIN9',
                     'RIN11', 'RIN6', 'RIN7']

    assert allUniqueResearches.collect() == expected_data
