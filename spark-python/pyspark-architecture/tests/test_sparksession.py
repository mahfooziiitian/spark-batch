import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session_default():
    spark = (SparkSession.builder
             .appName("pytest_spark_test_default")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def spark_session_custom():
    spark = (SparkSession.builder
             .appName("pytest_spark_test_custom")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    yield spark
    spark.stop()


def test_default_configuration(spark_session_default):
    # Test using the default SparkSession configuration
    print(spark_session_default.sparkContext)
    assert spark_session_default.conf.get("spark.sql.shuffle.partitions") == "4"


def test_custom_configuration(spark_session_custom):
    # Test using the custom SparkSession configuration
    print(spark_session_custom.sparkContext)
    assert spark_session_custom.conf.get("spark.sql.shuffle.partitions") == "8"


def test_multiple_spark_session():
    spark_session1 = SparkSession.builder.appName("SparkSession#1").master("local[*]").getOrCreate()
    spark_session2 = spark_session1.newSession()

    assert spark_session1.sparkContext == spark_session2.sparkContext
    spark_session1.stop()
    """Since both SparkContexts are equal, stopping the one
    for spark_session1 will make the context of sparkSession2 stopped too
    and that despite the different sessions"""
    assert spark_session1 != spark_session2
