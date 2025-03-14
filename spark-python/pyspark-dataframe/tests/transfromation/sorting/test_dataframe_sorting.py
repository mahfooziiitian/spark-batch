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


def test_dataframe_sorting(spark_session):
    data = [(3, "orange"), (1, "apple"), (4, "grape"), (2, "banana")]
    schema = ["id", "fruit"]
    df = spark_session.createDataFrame(data, schema)

    # Perform sorting operation on the DataFrame
    sorted_df = df.sort("id")

    sorted_df.explain(True)

    # Verify that the rows in the sorted DataFrame are in ascending order
    sorted_data = sorted_df.select("id").collect()
    assert [row["id"] for row in sorted_data] == [1, 2, 3, 4]
