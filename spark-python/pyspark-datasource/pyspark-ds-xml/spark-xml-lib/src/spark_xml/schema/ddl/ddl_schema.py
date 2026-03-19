"""Demonstrate reading XML using a DDL-string schema definition.

Instead of building a StructType manually, you can pass a DDL string
to ``spark.read.schema()``. This is often more concise for flat schemas.
This script shows:
- Basic DDL string schema
- DDL with nested struct syntax
- Converting between DDL string and StructType
"""

import os
import sys
import textwrap
from pathlib import Path

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

BOOKS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <library>
      <book>
        <title>Effective Java</title>
        <author>Joshua Bloch</author>
        <year>2018</year>
        <price>45.00</price>
        <pages>416</pages>
        <in_stock>true</in_stock>
      </book>
      <book>
        <title>Clean Code</title>
        <author>Robert C. Martin</author>
        <year>2008</year>
        <price>37.50</price>
        <pages>464</pages>
        <in_stock>true</in_stock>
      </book>
      <book>
        <title>Design Patterns</title>
        <author>Gang of Four</author>
        <year>1994</year>
        <price>52.99</price>
        <pages>395</pages>
        <in_stock>false</in_stock>
      </book>
    </library>
""")

ORDERS_WITH_ADDRESS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <orders>
      <order>
        <order_id>ORD-001</order_id>
        <customer>Alice</customer>
        <total>150.00</total>
        <shipping>
          <street>123 Main St</street>
          <city>Seattle</city>
          <zip>98101</zip>
        </shipping>
      </order>
      <order>
        <order_id>ORD-002</order_id>
        <customer>Bob</customer>
        <total>89.99</total>
        <shipping>
          <street>456 Oak Ave</street>
          <city>Portland</city>
          <zip>97201</zip>
        </shipping>
      </order>
    </orders>
""")


def write_xml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "schema_demo"

    books_path = base / "books_ddl.xml"
    orders_path = base / "orders_address.xml"
    write_xml(books_path, BOOKS_XML)
    write_xml(orders_path, ORDERS_WITH_ADDRESS_XML)

    spark = get_spark_session(
        app_name="spark-xml-ddl-schema",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # --- 1. Flat DDL string schema ---
    print("=== Flat DDL String Schema ===")
    flat_ddl = (
        "title STRING, author STRING, year INT, "
        "price DOUBLE, pages INT, in_stock BOOLEAN"
    )
    df_flat = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .schema(flat_ddl)
        .load(books_path.as_posix())
    )
    df_flat.printSchema()
    df_flat.show(truncate=False)

    # --- 2. DDL with nested struct ---
    print("\n=== DDL with Nested Struct ===")
    nested_ddl = (
        "order_id STRING, customer STRING, total DOUBLE, "
        "shipping STRUCT<street: STRING, city: STRING, zip: STRING>"
    )
    df_nested = (
        spark.read.format("xml")
        .option("rowTag", "order")
        .schema(nested_ddl)
        .load(orders_path.as_posix())
    )
    df_nested.printSchema()
    df_nested.show(truncate=False)

    # Access nested fields with dot notation
    print("\n=== Accessing Nested Fields ===")
    df_nested.select(
        "order_id", "customer", "shipping.city", "shipping.zip"
    ).show(truncate=False)

    # --- 3. Convert DDL string → StructType via empty DataFrame ---
    print("\n=== DDL → StructType Conversion ===")
    struct_from_ddl = spark.createDataFrame([], flat_ddl).schema
    print(f"StructType from DDL:\n  {struct_from_ddl.simpleString()}")

    # Round-trip: use the converted StructType to read
    df_roundtrip = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .schema(struct_from_ddl)
        .load(books_path.as_posix())
    )
    df_roundtrip.show(truncate=False)

    spark.stop()
