"""Tests for flattening deeply nested XML into denormalized rows."""

import xml.etree.ElementTree as ET

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from spark_etree.xmls_nested_flattening import (
    LINE_ITEM_SCHEMA,
    SAMPLE_ORDERS_XML,
    flatten_order,
)


class TestFlattenOrder:
    """Tests for the flatten_order pure function."""

    def test_flattens_order_with_two_items(self):
        xml = (
            '<order id="1" date="2025-01-01">'
            '<customer name="Alice" region="North" />'
            "<items>"
            '<item sku="X1" qty="2" price="10.00" />'
            '<item sku="X2" qty="1" price="20.00" />'
            "</items>"
            "</order>"
        )
        rows = flatten_order(xml)
        assert len(rows) == 2
        assert rows[0] == ("1", "2025-01-01", "Alice", "North", "X1", 2, 10.0)
        assert rows[1] == ("1", "2025-01-01", "Alice", "North", "X2", 1, 20.0)

    def test_single_item_order(self):
        xml = (
            '<order id="2" date="2025-02-01">'
            '<customer name="Bob" region="South" />'
            '<items><item sku="Y1" qty="3" price="5.50" /></items>'
            "</order>"
        )
        rows = flatten_order(xml)
        assert len(rows) == 1
        assert rows[0][4] == "Y1"
        assert rows[0][5] == 3
        assert rows[0][6] == 5.5

    def test_missing_customer_returns_none_fields(self):
        xml = '<order id="3" date="2025-03-01"><items><item sku="Z1" qty="1" price="1.00" /></items></order>'
        rows = flatten_order(xml)
        assert len(rows) == 1
        assert rows[0][2] is None  # customer
        assert rows[0][3] is None  # region

    def test_empty_items_returns_empty_list(self):
        xml = '<order id="4" date="2025-04-01"><customer name="Eve" region="East" /><items /></order>'
        assert flatten_order(xml) == []


class TestFlattenOrderSpark:
    """Integration tests for flatten_order UDF through Spark."""

    def _build_order_df(self, spark):
        root = ET.fromstring(SAMPLE_ORDERS_XML)
        order_strings = [ET.tostring(o, encoding="unicode").strip() for o in root.findall("order")]
        return spark.createDataFrame([Row(xml=xml) for xml in order_strings])

    def test_total_line_item_count(self, spark):
        df = self._build_order_df(spark)
        flatten_udf = udf(flatten_order, LINE_ITEM_SCHEMA)

        line_items = (
            df.withColumn("items", flatten_udf("xml")).select(F.explode("items").alias("item")).select("item.*")
        )
        # Order 1001: 2, 1002: 3, 1003: 1, 1004: 2 → 8
        assert line_items.count() == 8

    def test_flattened_schema_columns(self, spark):
        df = self._build_order_df(spark)
        flatten_udf = udf(flatten_order, LINE_ITEM_SCHEMA)

        line_items = (
            df.withColumn("items", flatten_udf("xml")).select(F.explode("items").alias("item")).select("item.*")
        )
        assert set(line_items.columns) == {
            "order_id",
            "order_date",
            "customer",
            "region",
            "sku",
            "qty",
            "price",
        }

    def test_revenue_by_sku(self, spark):
        df = self._build_order_df(spark)
        flatten_udf = udf(flatten_order, LINE_ITEM_SCHEMA)

        revenue = (
            df.withColumn("items", flatten_udf("xml"))
            .select(F.explode("items").alias("item"))
            .select("item.*")
            .withColumn("line_total", F.col("qty") * F.col("price"))
            .groupBy("sku")
            .agg(F.round(F.sum("line_total"), 2).alias("total_revenue"))
            .orderBy(F.desc("total_revenue"))
            .collect()
        )
        # A100: (2*29.99) + (1*29.99) + (4*29.99) = 209.93
        a100 = next(r for r in revenue if r["sku"] == "A100")
        assert a100["total_revenue"] == 209.93

    def test_distinct_regions(self, spark):
        df = self._build_order_df(spark)
        flatten_udf = udf(flatten_order, LINE_ITEM_SCHEMA)

        regions = (
            df.withColumn("items", flatten_udf("xml"))
            .select(F.explode("items").alias("item"))
            .select("item.region")
            .distinct()
            .collect()
        )
        assert {r["region"] for r in regions} == {"North", "South", "East"}


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
