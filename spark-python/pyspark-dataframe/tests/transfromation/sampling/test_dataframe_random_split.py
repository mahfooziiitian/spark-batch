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


def test_dataframe_random_split(spark_session):
    data = [(1, "apple"), (2, "banana"), (3, "orange"), (4, "grape"), (5, "melon")]
    schema = ["id", "fruit"]
    df = spark_session.createDataFrame(data, schema)

    # Perform random split on the DataFrame
    splits = df.randomSplit([0.6, 0.4], seed=42)  # Split into two DataFrames: 60% and 40%

    # Check the number of rows in each split DataFrame
    assert splits[0].count() == 3
    assert splits[1].count() == 2

    # Verify that the rows in the split DataFrames contain valid values
    split_data = splits[0].select("id").collect() + splits[1].select("id").collect()
    assert all(row["id"] in [1, 2, 3, 4, 5] for row in split_data)
