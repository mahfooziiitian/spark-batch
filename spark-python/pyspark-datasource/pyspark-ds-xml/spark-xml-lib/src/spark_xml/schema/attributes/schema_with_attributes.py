"""Demonstrate schema definitions that include XML attributes.

In spark-xml, XML attributes are mapped to DataFrame columns with a
configurable prefix (default ``_``). This script shows:
- Default attribute prefix (``_``)
- Custom attribute prefix (``attr_``)
- Schema that mixes element text and attributes
- Using _VALUE to access element text when attributes are present
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

BOOKSTORE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <bookstore>
      <book id="bk101" category="fiction" lang="en">
        <title>The Great Gatsby</title>
        <author nationality="American">F. Scott Fitzgerald</author>
        <year>1925</year>
        <price currency="USD">10.99</price>
        <rating>4.5</rating>
      </book>
      <book id="bk102" category="science" lang="en">
        <title>A Brief History of Time</title>
        <author nationality="British">Stephen Hawking</author>
        <year>1988</year>
        <price currency="GBP">12.50</price>
        <rating>4.8</rating>
      </book>
      <book id="bk103" category="fiction" lang="fr">
        <title>Le Petit Prince</title>
        <author nationality="French">Antoine de Saint-Exupéry</author>
        <year>1943</year>
        <price currency="EUR">8.99</price>
        <rating>4.9</rating>
      </book>
    </bookstore>
""")

CATALOG_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <catalog>
      <product sku="PRD-001" status="active">
        <name>Widget</name>
        <variants>
          <variant size="S" color="red">
            <stock>100</stock>
            <price>9.99</price>
          </variant>
          <variant size="M" color="blue">
            <stock>200</stock>
            <price>12.99</price>
          </variant>
        </variants>
      </product>
      <product sku="PRD-002" status="discontinued">
        <name>Gadget</name>
        <variants>
          <variant size="L" color="black">
            <stock>0</stock>
            <price>24.99</price>
          </variant>
        </variants>
      </product>
    </catalog>
""")


def write_xml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "schema_demo"

    books_path = base / "bookstore_attrs.xml"
    catalog_path = base / "catalog_attrs.xml"
    write_xml(books_path, BOOKSTORE_XML)
    write_xml(catalog_path, CATALOG_XML)

    spark = get_spark_session(
        app_name="spark-xml-schema-attributes",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # --- 1. Default attribute prefix (_) ---
    print("=== Default Attribute Prefix (_) ===")
    df_default = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .load(books_path.as_posix())
    )
    df_default.printSchema()
    df_default.show(truncate=False)

    # --- 2. Custom attribute prefix (attr_) ---
    print("\n=== Custom Attribute Prefix (attr_) ===")
    df_custom = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .option("attributePrefix", "attr_")
        .load(books_path.as_posix())
    )
    df_custom.printSchema()
    df_custom.show(truncate=False)

    # --- 3. Explicit schema with attributes ---
    print("\n=== Explicit Schema with Attribute Columns ===")
    book_schema = StructType([
        StructField("_id", StringType(), True),
        StructField("_category", StringType(), True),
        StructField("_lang", StringType(), True),
        StructField("title", StringType(), True),
        StructField("author", StructType([
            StructField("_nationality", StringType(), True),
            StructField("_VALUE", StringType(), True),
        ]), True),
        StructField("year", LongType(), True),
        StructField("price", StructType([
            StructField("_currency", StringType(), True),
            StructField("_VALUE", DoubleType(), True),
        ]), True),
        StructField("rating", DoubleType(), True),
    ])

    df_explicit = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .schema(book_schema)
        .load(books_path.as_posix())
    )
    df_explicit.printSchema()
    df_explicit.show(truncate=False)

    # --- 4. Access _VALUE and attributes together ---
    print("\n=== _VALUE + Attribute Access ===")
    df_explicit.select(
        "_id",
        "title",
        F.col("author._VALUE").alias("author_name"),
        F.col("author._nationality").alias("nationality"),
        F.col("price._VALUE").alias("price_amount"),
        F.col("price._currency").alias("currency"),
    ).show(truncate=False)

    # --- 5. Nested attributes with arrays ---
    print("\n=== Nested Attributes (product catalog) ===")
    variant_schema = StructType([
        StructField("_size", StringType(), True),
        StructField("_color", StringType(), True),
        StructField("stock", LongType(), True),
        StructField("price", DoubleType(), True),
    ])

    catalog_schema = StructType([
        StructField("_sku", StringType(), True),
        StructField("_status", StringType(), True),
        StructField("name", StringType(), True),
        StructField("variants", StructType([
            StructField("variant", ArrayType(variant_schema), True),
        ]), True),
    ])

    df_catalog = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .schema(catalog_schema)
        .load(catalog_path.as_posix())
    )
    df_catalog.printSchema()
    df_catalog.show(truncate=False)

    # Explode variants and show flat view
    print("\n=== Flattened Variant View ===")
    df_flat = (
        df_catalog.select(
            "_sku",
            "_status",
            "name",
            F.explode("variants.variant").alias("v"),
        )
        .select(
            "_sku", "_status", "name",
            F.col("v._size").alias("size"),
            F.col("v._color").alias("color"),
            "v.stock",
            "v.price",
        )
    )
    df_flat.show(truncate=False)

    # --- 6. Exclude attributes entirely ---
    print("\n=== Exclude Attributes ===")
    df_no_attrs = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .option("excludeAttribute", "true")
        .load(books_path.as_posix())
    )
    df_no_attrs.printSchema()
    df_no_attrs.show(truncate=False)

    spark.stop()
