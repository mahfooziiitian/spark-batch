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


def test_dataframe_deduplication(spark_session):
    data = [(1, "apple"), (2, "banana"), (1, "apple"), (3, "orange")]
    schema = ["id", "fruit"]
    df = spark_session.createDataFrame(data, schema)

    # Perform deduplication based on 'id' column
    deduplicated_df = df.dropDuplicates(["id"])

    result_data = deduplicated_df.collect()
    expected_data = [(1, "apple"), (2, "banana"), (3, "orange")]

    assert result_data == expected_data


def test_dataframe_deduplication_distinct(spark_session):
    data = [(1, "apple"), (2, "banana"), (1, "apple"), (3, "orange")]
    schema = ["id", "fruit"]
    df = spark_session.createDataFrame(data, schema)

    # Deduplicate DataFrame using the distinct method
    deduplicated_df = df.distinct()

    result_data = deduplicated_df.collect()
    expected_data = [(1, "apple"), (2, "banana"), (3, "orange")]

    assert result_data == expected_data
