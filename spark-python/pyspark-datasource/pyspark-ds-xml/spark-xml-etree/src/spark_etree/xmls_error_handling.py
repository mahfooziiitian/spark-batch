"""Robust XML parsing that handles malformed, incomplete, and missing data."""

import os
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

SAMPLE_DATA = [
    {"id": 1, "xml": '<product sku="P100"><name>Widget</name><price>19.99</price><category>Tools</category></product>'},
    {"id": 2, "xml": '<product sku="P200"><name>Gadget</name><category>Electronics</category></product>'},
    {"id": 3, "xml": "this is not xml at all"},
    {"id": 4, "xml": "<product><name>Mystery Item</name><price>5.00</price></product>"},
    {"id": 5, "xml": None},
    {"id": 6, "xml": ""},
    {"id": 7, "xml": '<product sku="P300"><name>Gizmo</name><price>invalid</price><category>Toys</category></product>'},
    {"id": 8, "xml": '<product sku="P400"><name></name><price>12.50</price><category>Office</category></product>'},
]

PRODUCT_SCHEMA = StructType(
    [
        StructField("sku", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", StringType(), True),
        StructField("category", StringType(), True),
        StructField("parse_error", StringType(), True),
    ]
)


def safe_parse_product(payload: str | None) -> dict[str, str | None]:
    """Parse a product element, returning error detail instead of raising."""
    if not payload:
        return {"sku": None, "name": None, "price": None, "category": None, "parse_error": "empty or null input"}

    try:
        doc = ET.fromstring(payload)
    except ET.ParseError as e:
        return {"sku": None, "name": None, "price": None, "category": None, "parse_error": str(e)}

    name_el = doc.find("name")
    return {
        "sku": doc.attrib.get("sku"),
        "name": name_el.text if name_el is not None else None,
        "price": _safe_text(doc, "price"),
        "category": _safe_text(doc, "category"),
        "parse_error": None,
    }


def _safe_text(el: ET.Element, tag: str) -> str | None:
    child = el.find(tag)
    return child.text if child is not None else None


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("xml-etree-error-handling")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(SAMPLE_DATA)

    print("=== Raw input ===")
    df.show(truncate=80)

    safe_parse_udf = udf(safe_parse_product, PRODUCT_SCHEMA)

    parsed = df.withColumn("product", safe_parse_udf("xml")).select("id", "product.*")

    print("=== All parsed rows (including errors) ===")
    parsed.show(truncate=False)

    # Separate clean rows from errors
    clean = parsed.filter(F.col("parse_error").isNull())
    errors = parsed.filter(F.col("parse_error").isNotNull())

    print(f"=== Clean rows: {clean.count()} ===")
    clean.drop("parse_error").show(truncate=False)

    print(f"=== Error rows: {errors.count()} ===")
    errors.select("id", "parse_error").show(truncate=False)

    spark.stop()
