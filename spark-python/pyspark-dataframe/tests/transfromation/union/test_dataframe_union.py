"""
Unions are currently performed based on location, not on the schema.
This means that columns will not automatically line up the way you think they might.
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


def test_dataframe_union(spark_session):
    data1 = [(1, "apple"), (2, "banana")]
    data2 = [(3, "orange"), (4, "grape")]
    schema = ["id", "fruit"]
    df1 = spark_session.createDataFrame(data1, schema)
    df2 = spark_session.createDataFrame(data2, schema)

    # Perform union operation on the DataFrames
    union_df = df1.union(df2)

    # Check the number of rows in the union DataFrame
    assert union_df.count() == 4

    # Verify that the rows in the union DataFrame contain valid values
    union_data = union_df.select("id").collect()
    assert all(row["id"] in [1, 2, 3, 4] for row in union_data)
