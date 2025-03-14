from typing import Callable, Any, Tuple

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

from etl.etl import transform_data


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("etl") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_etl(spark):
    # 1. Prepare an input data frame that mimics our source data.
    input_schema = StructType([
        StructField('StoreID', IntegerType(), True),
        StructField('Location', StringType(), True),
        StructField('Date', StringType(), True),
        StructField('ItemCount', IntegerType(), True)
    ])
    input_data = [(1, "Bangalore", "2021-12-01", 5),
                  (2, "Bangalore", "2021-12-01", 3),
                  (5, "Amsterdam", "2021-12-02", 10),
                  (6, "Amsterdam", "2021-12-01", 1),
                  (8, "Warsaw", "2021-12-02", 15),
                  (7, "Warsaw", "2021-12-01", 99)]

    input_df = spark.createDataFrame(data=input_data, schema=input_schema)

    # 2. Prepare an expected data frame which is the output that we expect.
    expected_schema = StructType([
        StructField('Location', StringType(), True),
        StructField('TotalItemCount', LongType(), True)
    ])

    expected_data = [("Bangalore", 8),
                     ("Warsaw", 114),
                     ("Amsterdam", 11)]

    expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)

    # 3. Apply our transformation to the input data frame
    transformed_df = transform_data(input_df)

    # 4. Assert the output of the transformation to the expected data frame.
    field_list: Callable[[Any], tuple[Any, Any, Any]] = lambda fields: (fields.name, fields.dataType, fields.nullable)
    # field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
    fields1 = [*map(field_list, transformed_df.schema.fields)]
    fields2 = [*map(field_list, expected_df.schema.fields)]
    # Compare schema of transformed_df and expected_df
    res = set(fields1) == set(fields2)

    # assert
    assert res

    # Compare data in transformed_df and expected_df
    assert sorted(expected_df.collect()) == sorted(transformed_df.collect())
