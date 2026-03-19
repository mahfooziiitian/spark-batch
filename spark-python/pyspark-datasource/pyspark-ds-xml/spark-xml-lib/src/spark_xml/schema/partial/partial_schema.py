"""Demonstrate column pruning by providing a partial schema.

When you only need a subset of columns from a large XML file, providing
a partial schema causes spark-xml to skip parsing unused elements,
improving both performance and memory usage. This script shows:
- Reading with a full schema (all columns)
- Reading with a partial schema (selected columns only)
- Combining partial schema with column selection
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

INVENTORY_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <inventory>
      <product>
        <sku>SKU-001</sku>
        <name>Laptop Pro 15</name>
        <description>High-performance laptop with 16GB RAM and 512GB SSD</description>
        <category>Electronics</category>
        <brand>TechCorp</brand>
        <price>1299.99</price>
        <cost>850.00</cost>
        <quantity>45</quantity>
        <warehouse>WH-EAST</warehouse>
        <supplier>Supplier A</supplier>
        <weight>2.1</weight>
        <rating>4.7</rating>
      </product>
      <product>
        <sku>SKU-002</sku>
        <name>Wireless Mouse</name>
        <description>Ergonomic wireless mouse with USB-C receiver</description>
        <category>Accessories</category>
        <brand>PeripheralCo</brand>
        <price>29.99</price>
        <cost>12.50</cost>
        <quantity>500</quantity>
        <warehouse>WH-WEST</warehouse>
        <supplier>Supplier B</supplier>
        <weight>0.1</weight>
        <rating>4.3</rating>
      </product>
      <product>
        <sku>SKU-003</sku>
        <name>USB-C Hub</name>
        <description>7-port USB-C hub with HDMI and ethernet</description>
        <category>Accessories</category>
        <brand>ConnectPlus</brand>
        <price>49.99</price>
        <cost>22.00</cost>
        <quantity>200</quantity>
        <warehouse>WH-EAST</warehouse>
        <supplier>Supplier A</supplier>
        <weight>0.3</weight>
        <rating>4.5</rating>
      </product>
    </inventory>
""")


def write_xml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    xml_path = (
        Path(data_home) / "file_data" / "xml" / "schema_demo" / "inventory.xml"
    )
    write_xml(xml_path, INVENTORY_XML)

    spark = get_spark_session(
        app_name="spark-xml-partial-schema",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # --- 1. Full inferred schema (all 12 columns) ---
    print("=== Full Inferred Schema (all columns) ===")
    df_full = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .load(xml_path.as_posix())
    )
    df_full.printSchema()
    print(f"Columns: {len(df_full.columns)}")
    df_full.show(truncate=False)

    # --- 2. Partial schema: only name, price, quantity ---
    print("\n=== Partial Schema (3 columns only) ===")
    partial_schema = StructType([
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", LongType(), True),
    ])
    df_partial = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .schema(partial_schema)
        .load(xml_path.as_posix())
    )
    df_partial.printSchema()
    print(f"Columns: {len(df_partial.columns)}")
    df_partial.show(truncate=False)

    # --- 3. Partial schema for margin calculation ---
    print("\n=== Partial Schema for Margin Analysis ===")
    margin_schema = StructType([
        StructField("sku", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("cost", DoubleType(), True),
    ])
    df_margin = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .schema(margin_schema)
        .load(xml_path.as_posix())
    )
    from pyspark.sql import functions as F

    df_margin = df_margin.withColumn(
        "margin_pct",
        F.round((F.col("price") - F.col("cost")) / F.col("price") * 100, 1),
    )
    df_margin.show(truncate=False)

    # --- 4. Partial schema via DDL string ---
    print("\n=== Partial Schema via DDL String ===")
    ddl_partial = "sku STRING, category STRING, warehouse STRING"
    df_ddl = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .schema(ddl_partial)
        .load(xml_path.as_posix())
    )
    df_ddl.show(truncate=False)

    spark.stop()
