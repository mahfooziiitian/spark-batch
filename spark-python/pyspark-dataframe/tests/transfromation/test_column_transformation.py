import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, cast, col


@pytest.fixture(scope='session')
def spark():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("test_column_transformation")
             .getOrCreate())
    yield spark
    spark.stop()


def test_add_column(spark):
    df = spark.createDataFrame([(1, "apple"), (2, "banana"), (3, "orange")], ["id", "fruit"])
    df = df.withColumn("mandatory", lit("Y"))
    assert df.columns == ["id", "fruit", "mandatory"]


def test_renaming_column(spark):
    df = spark.createDataFrame([(1, "apple"), (2, "banana"), (3, "orange")], ["id", "fruit"])
    df = df.withColumnRenamed("id", "fruit_id")
    assert df.columns == ["fruit_id", "fruit"]


def test_remove_column(spark):
    df = spark.createDataFrame([(1, "apple"), (2, "banana"), (3, "orange")], ["id", "fruit"])
    df = df.withColumn("mandatory", lit("Y"))
    df = df.drop("id")
    assert df.columns == ["fruit", "mandatory"]


def test_change_column_type(spark):
    df = spark.createDataFrame([(1, "apple"), (2, "banana"), (3, "orange")], ["id", "fruit"])
    df = df.withColumn("mandatory", lit("True"))
    df = df.withColumn("mandatory", col("mandatory").cast("boolean"))
    assert df.columns == ["id", "fruit", "mandatory"]
