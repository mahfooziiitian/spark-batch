"""Flatten deeply nested XML (orders → line items) into denormalized rows."""

import os
import xml.etree.ElementTree as ET

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

SAMPLE_ORDERS_XML = """\
<orders>
  <order id="1001" date="2025-06-15">
    <customer name="Alice" region="North" />
    <items>
      <item sku="A100" qty="2" price="29.99" />
      <item sku="B200" qty="1" price="49.99" />
    </items>
  </order>
  <order id="1002" date="2025-06-16">
    <customer name="Bob" region="South" />
    <items>
      <item sku="C300" qty="5" price="9.99" />
      <item sku="A100" qty="1" price="29.99" />
      <item sku="D400" qty="3" price="14.50" />
    </items>
  </order>
  <order id="1003" date="2025-06-16">
    <customer name="Charlie" region="North" />
    <items>
      <item sku="B200" qty="2" price="49.99" />
    </items>
  </order>
  <order id="1004" date="2025-06-17">
    <customer name="Diana" region="East" />
    <items>
      <item sku="A100" qty="4" price="29.99" />
      <item sku="D400" qty="1" price="14.50" />
    </items>
  </order>
</orders>
"""

LINE_ITEM_SCHEMA = ArrayType(
    StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("order_date", StringType(), False),
            StructField("customer", StringType(), True),
            StructField("region", StringType(), True),
            StructField("sku", StringType(), False),
            StructField("qty", IntegerType(), False),
            StructField("price", DoubleType(), False),
        ]
    )
)


def flatten_order(payload: str) -> list[tuple]:
    """Denormalize one <order> element into a list of line-item tuples."""
    order = ET.fromstring(payload)
    order_id = order.attrib["id"]
    order_date = order.attrib["date"]

    cust = order.find("customer")
    customer = cust.attrib.get("name") if cust is not None else None
    region = cust.attrib.get("region") if cust is not None else None

    rows = []
    for item in order.findall("items/item"):
        rows.append(
            (
                order_id,
                order_date,
                customer,
                region,
                item.attrib["sku"],
                int(item.attrib["qty"]),
                float(item.attrib["price"]),
            )
        )
    return rows


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("xml-etree-nested-flatten")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    root = ET.fromstring(SAMPLE_ORDERS_XML)
    order_strings = [ET.tostring(o, encoding="unicode").strip() for o in root.findall("order")]

    order_df = spark.createDataFrame([Row(xml=xml) for xml in order_strings])

    print("=== Raw order XML ===")
    order_df.show(truncate=80)

    # Flatten each order into line items and explode
    flatten_udf = udf(flatten_order, LINE_ITEM_SCHEMA)

    line_items = (
        order_df.withColumn("items", flatten_udf("xml")).select(F.explode("items").alias("item")).select("item.*")
    )

    print("=== Flattened line items ===")
    line_items.show(truncate=False)

    # Revenue per SKU
    print("=== Revenue by SKU ===")
    (
        line_items.withColumn("line_total", F.col("qty") * F.col("price"))
        .groupBy("sku")
        .agg(
            F.round(F.sum("line_total"), 2).alias("total_revenue"),
            F.sum("qty").alias("total_qty"),
        )
        .orderBy(F.desc("total_revenue"))
        .show(truncate=False)
    )

    # Revenue per region
    print("=== Revenue by region ===")
    (
        line_items.withColumn("line_total", F.col("qty") * F.col("price"))
        .groupBy("region")
        .agg(
            F.round(F.sum("line_total"), 2).alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .orderBy(F.desc("total_revenue"))
        .show(truncate=False)
    )

    spark.stop()
