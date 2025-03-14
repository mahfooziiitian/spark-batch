import pytest
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import pyspark.pandas as ps


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("local-tests")
        .config("spark.executor.cores", "8")
        .config("spark.executor.instances", "2")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_pandas_df_creation(spark):
    ps_df = ps.DataFrame(
        {'a': [1, 2, 3, 4, 5, 6],
         'b': [100, 200, 300, 400, 500, 600],
         'c': ["one", "two", "three", "four", "five", "six"]},
        index=[10, 20, 30, 40, 50, 60])
    assert isinstance(ps_df, ps.DataFrame)


def test_pandas_series_creation(spark):
    pss = ps.Series([1, 3, 5, np.nan, 6, 8])
    assert isinstance(pss, ps.Series)