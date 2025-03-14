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


def test_dataframe_random_sample(spark_session):
    data = [(1, "apple"), (2, "banana"), (3, "orange"), (4, "grape"), (5, "melon")]
    schema = ["id", "fruit"]
    df = spark_session.createDataFrame(data, schema)

    # Perform random sampling on the DataFrame
    sampled_df = df.sample(fraction=0.4, seed=42)  # Sample 40% of the data

    # Check the number of rows in the sampled DataFrame
    assert sampled_df.count() == 2

    # Verify that the sampled DataFrame contains valid values
    sampled_data = sampled_df.select("id").collect()
    assert all(row["id"] in [1, 2, 3, 4, 5] for row in sampled_data)
