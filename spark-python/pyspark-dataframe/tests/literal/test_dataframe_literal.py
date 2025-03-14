import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("pytest_spark_test") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_dataframe_literal(spark_session):
    # Use the spark_session fixture to create DataFrames
    df = spark_session.createDataFrame([(1, "apple"), (2, "banana"), (3, "orange")], ["id", "fruit"])

    assert df.count() == 3
    assert df.columns == ["id", "fruit"]

    df = df.withColumn("is_even_pos", lit("Y"))

    # Verify values in the DataFrame
    fruit_data = df.select("fruit").collect()
    assert [row["fruit"] for row in fruit_data] == ["apple", "banana", "orange"]
