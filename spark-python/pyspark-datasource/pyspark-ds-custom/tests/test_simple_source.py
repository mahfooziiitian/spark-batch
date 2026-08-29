from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from custom_ds import SimpleDataSource

pytestmark = pytest.mark.pyspark


def test_simple_source_default_row_count(spark: SparkSession) -> None:
    spark.dataSource.register(SimpleDataSource)

    df = spark.read.format("simple").load()

    assert df.count() == 10
    assert set(df.columns) == {"id", "value"}


def test_simple_source_respects_num_rows_option(spark: SparkSession) -> None:
    spark.dataSource.register(SimpleDataSource)

    df = spark.read.format("simple").option("numRows", 25).option("numPartitions", 3).load()

    assert df.count() == 25
    assert df.rdd.getNumPartitions() == 3

    rows = sorted(r["id"] for r in df.collect())
    assert rows == list(range(25))


def test_simple_source_value_format(spark: SparkSession) -> None:
    spark.dataSource.register(SimpleDataSource)

    df = spark.read.format("simple").option("numRows", 3).load()
    values = sorted(r["value"] for r in df.collect())

    assert values == ["row-0", "row-1", "row-2"]
