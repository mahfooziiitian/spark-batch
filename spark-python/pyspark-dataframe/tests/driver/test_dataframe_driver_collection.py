"""
Spark maintains the state of the cluster in the driver.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("pytest_spark_test") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_dataframe_collect(spark_session):
    data = [(1, "apple"), (2, "banana"), (3, "orange"), (4, "grape")]
    schema = ["id", "fruit"]
    df = spark_session.createDataFrame(data, schema)

    # Collect the DataFrame data to the driver
    collected_data = df.collect()

    # Verify that the collected data matches the expected results
    expected_data = [(1, "apple"), (2, "banana"), (3, "orange"), (4, "grape")]
    assert collected_data == expected_data


def test_dataframe_collect_iterator(spark_session):
    data = [(1, "apple"), (2, "banana"), (3, "orange"), (4, "grape")]
    schema = ["id", "fruit"]
    df = spark_session.createDataFrame(data, schema)

    # Collect the DataFrame data to the driver
    collected_data = df.toLocalIterator()
    for row in collected_data:
        print(row)
    # Verify that the collected data matches the expected results
    # expected_data = [(1, "apple"), (2, "banana"), (3, "orange"), (4, "grape")]
    # assert collected_data == expected_data
