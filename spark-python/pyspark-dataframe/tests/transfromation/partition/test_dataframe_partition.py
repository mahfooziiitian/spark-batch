"""
Optimization opportunity is to partition the data according to some frequently filtered columns, which control the physical
layout of data across the cluster including the partitioning scheme and the number of partitions
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


def test_dataframe_repartition(spark_session):
    data = [(1, "apple"), (2, "banana"), (3, "orange"), (4, "grape")]
    schema = ["id", "fruit"]
    df = spark_session.createDataFrame(data, schema)

    assert df.rdd.getNumPartitions() == 8

    # Repartition the DataFrame
    repartitioned_df = df.repartition(2)

    # Verify the number of partitions in the repartitioned DataFrame
    assert repartitioned_df.rdd.getNumPartitions() == 2

    # Perform an action to trigger the repartitioning
    assert repartitioned_df.count() == 4


def test_dataframe_coalesce(spark_session):
    data = [(1, "apple"), (2, "banana"), (3, "orange"), (4, "grape")]
    schema = ["id", "fruit"]
    df = spark_session.createDataFrame(data, schema)

    # Coalesce the DataFrame
    coalesced_df = df.coalesce(2)

    # Verify the number of partitions in the coalesced DataFrame
    assert coalesced_df.rdd.getNumPartitions() == 2

    # Perform an action to trigger the coalescing
    assert coalesced_df.count() == 4
