import os

import pytest
from unittest.mock import Mock
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DoubleType

from reader.spark_reader import load_csv


@pytest.fixture
def spark():
    spark_mock = Mock()
    type(spark_mock).write = spark_mock
    type(spark_mock).read = spark_mock
    spark_mock.table.return_value = spark_mock
    spark_mock.format.return_value = spark_mock
    spark_mock.option.return_value = spark_mock
    spark_mock.mode.return_value = spark_mock
    spark_mock.save.return_value = None
    return spark_mock


@pytest.fixture
def sample_df(spark):
    data = [("Walter", 32, "Germany", 10000.0)]

    # for simple use cases you can also omit the schema
    # return spark.createDataFrame(data, ["name", "age", "country", "salary"])

    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
            StructField("country", StringType(), False),
            StructField("salary", DoubleType(), False),
        ]
    )
    return spark.createDataFrame(data, schema)


def test_mock(spark, sample_df):
    # return a sample df when spark.load is called
    spark.load.return_value = sample_df
    data_home = os.environ['DATA_HOME'].replace("\\", "/")
    csv_data_file = f"file:///{data_home}/FileData/Csv/peoples.csv"

    # here we supply the mock to load_csv
    df = load_csv(spark, csv_data_file)

    assert spark.format.called
    assert not spark.save.called
    spark.option.assert_any_call("sep", ",")

    # here we check if load was indeed called with the supplied file
    spark.load.assert_called_with(csv_data_file)

    row = df.withColumn("salary", 1.5 * F.col("salary")).head()
    # assert row == Row(name="Walter", age=32, country="Germany", salary=15000.0)
