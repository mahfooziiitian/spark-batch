import pytest
from chispa import assert_approx_column_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from df.functions.functions import divide_by_three


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("functions") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_divide_by_three(spark):
    data = [
        (1, 0.33),
        (2, 0.66),
        (3, 1.0),
        (None, None)
    ]
    df = spark.createDataFrame(data, ["num", "expected"]) \
        .withColumn("num_divided_by_three", divide_by_three(f.col("num")))
    assert_approx_column_equality(df, "num_divided_by_three", "expected", 0.01)
