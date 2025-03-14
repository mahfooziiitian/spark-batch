import pytest
from pyspark.sql import Row, SparkSession


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("test_dataframe_row") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_row_attributes(spark):
    # Create a Row instance manually
    row_data = Row(id=1, name="John", age=30)
    assert row_data.id == 1
    assert row_data.name == "John"
    assert row_data.age == 30

    data = [(1, "Alice", 25), (2, "Bob", 30)]
    schema = ["id", "name", "age"]
    df = spark.createDataFrame(data, schema)
    row_from_df = df.first()  # Get the first Row from the DataFrame

    assert row_from_df.id == 1 and row_from_df.name == "Alice" and row_from_df.age == 25
