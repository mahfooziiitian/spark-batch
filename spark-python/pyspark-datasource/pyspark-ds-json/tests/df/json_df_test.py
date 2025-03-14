import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("pytest-spark") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_json_processing_pandas(spark_session):
    # Define test JSON data
    test_json = [
        '{"name": "Alice", "age": 30}',
        '{"name": "Bob", "age": 25}'
    ]

    # Create test DataFrame from JSON data
    df = spark_session.read.json(spark_session.sparkContext.parallelize(test_json))

    # Perform some transformations (e.g., filtering)
    filtered_df = df.filter(df['age'] > 25)

    # Convert DataFrame to Pandas for easier assertion
    result = filtered_df.toPandas()

    # Assert the expected output
    expected_result = [{'name': 'Alice', 'age': 30}]
    assert result.to_dict(orient='records') == expected_result


def test_json_processing(spark_session):
    # Define test JSON data
    test_json = [
        '{"name": "Alice", "age": 30}',
        '{"name": "Bob", "age": 25}'
    ]

    # Create test DataFrame from JSON data
    df = spark_session.read.json(spark_session.sparkContext.parallelize(test_json))

    # Perform some transformations (e.g., filtering)
    filtered_df = df.filter(df['age'] > 25)

    # Collect the results as a list of dictionaries
    result = [x.asDict() for x in filtered_df.collect()]

    # Assert the expected output
    expected_result = [{'name': 'Alice', 'age': 30}]
    assert result == expected_result
