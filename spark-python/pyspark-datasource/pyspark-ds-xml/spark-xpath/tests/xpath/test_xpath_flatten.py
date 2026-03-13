"""Tests for array flattening XPath patterns."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


@pytest.fixture(scope="session")
def spark():
    """Create a shared SparkSession for all tests."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("XPath Flatten Tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_explode_xpath_array(spark):
    """explode(xpath()) flattens repeating elements into rows."""
    data = [
        "<catalog><item><name>A</name></item>"
        "<item><name>B</name></item>"
        "<item><name>C</name></item></catalog>"
    ]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_flat_explode")

    result = spark.sql("""
        SELECT explode(xpath(data, 'catalog/item/name/text()')) AS name
        FROM test_flat_explode
    """)
    names = sorted([row.name for row in result.collect()])
    assert names == ["A", "B", "C"]


def test_arrays_zip_parallel_arrays(spark):
    """arrays_zip combines parallel xpath arrays element-wise."""
    data = [
        "<catalog>"
        "<item><name>Laptop</name><price>999</price></item>"
        "<item><name>Mouse</name><price>29</price></item>"
        "</catalog>"
    ]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_flat_zip")

    result = spark.sql("""
        SELECT
            zipped.`0` AS item_name,
            zipped.`1` AS item_price
        FROM test_flat_zip
        LATERAL VIEW explode(
            arrays_zip(
                xpath(data, 'catalog/item/name/text()'),
                xpath(data, 'catalog/item/price/text()')
            )
        ) t AS zipped
    """)
    rows = [(row.item_name, row.item_price) for row in result.collect()]
    assert rows == [("Laptop", "999"), ("Mouse", "29")]


def test_posexplode_tracks_position(spark):
    """posexplode returns element position alongside value."""
    data = [
        "<list><val>X</val><val>Y</val><val>Z</val></list>"
    ]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_flat_pos")

    result = spark.sql("""
        SELECT pos, val
        FROM test_flat_pos
        LATERAL VIEW posexplode(xpath(data, 'list/val/text()')) AS pos, val
    """)
    rows = [(row.pos, row.val) for row in result.collect()]
    assert rows == [(0, "X"), (1, "Y"), (2, "Z")]


def test_explode_with_parent_context(spark):
    """Exploded rows retain parent-level xpath context."""
    data = [
        "<catalog><store>Downtown</store>"
        "<item><name>A</name></item>"
        "<item><name>B</name></item></catalog>",
        "<catalog><store>Airport</store>"
        "<item><name>C</name></item></catalog>",
    ]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_flat_parent")

    result = spark.sql("""
        SELECT
            xpath_string(data, 'catalog/store') AS store,
            explode(xpath(data, 'catalog/item/name/text()')) AS item
        FROM test_flat_parent
    """)
    rows = [(row.store, row.item) for row in result.collect()]
    assert ("Downtown", "A") in rows
    assert ("Downtown", "B") in rows
    assert ("Airport", "C") in rows
    assert len(rows) == 3


def test_aggregation_after_explode(spark):
    """Aggregation works correctly on exploded xpath arrays."""
    data = [
        "<store><name>S1</name>"
        "<item><price>10</price></item>"
        "<item><price>20</price></item>"
        "<item><price>30</price></item></store>",
    ]
    df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
    df.createOrReplaceTempView("test_flat_agg")

    result = spark.sql("""
        WITH flattened AS (
            SELECT
                xpath_string(data, 'store/name') AS store,
                explode(xpath(data, 'store/item/price/text()')) AS price_str
            FROM test_flat_agg
        )
        SELECT
            store,
            COUNT(*) AS cnt,
            SUM(CAST(price_str AS INT)) AS total
        FROM flattened
        GROUP BY store
    """)
    row = result.collect()[0]
    assert row.store == "S1"
    assert row.cnt == 3
    assert row.total == 60
