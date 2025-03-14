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


def test_dataframe_transformation(spark_session):
    data = [(1, "apple"), (2, "banana"), (3, "orange")]
    schema = ["id", "fruit"]
    df = spark_session.createDataFrame(data, schema)

    transformed_df = df.withColumn("fruit_upper", df["fruit"])

    result_data = transformed_df.collect()
    expected_data = [(1, "apple", "APPLE"), (2, "banana", "BANANA"), (3, "orange", "ORANGE")]

    assert result_data == expected_data
