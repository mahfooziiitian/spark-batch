"""XML writer with attributes using attributePrefix.

Demonstrates how to write DataFrame columns as XML attributes
instead of child elements, using the attributePrefix option.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable


if __name__ == "__main__":
    out_dir = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "writer_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session(
        app_name="xml-writer-attributes",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ── 1. Default attribute prefix (_) ─────────────────────────────
    # Columns starting with "_" become XML attributes
    print("\n=== 1. Default attributePrefix (_) ===")
    schema_default = StructType([
        StructField("_id", IntegerType()),         # → attribute
        StructField("_category", StringType()),     # → attribute
        StructField("title", StringType()),         # → child element
        StructField("price", DoubleType()),         # → child element
    ])
    data = [
        (1, "fiction", "The Great Gatsby", 12.99),
        (2, "science", "A Brief History of Time", 15.50),
        (3, "fiction", "To Kill a Mockingbird", 10.99),
        (4, "tech", "Clean Code", 35.00),
        (5, "history", "Sapiens", 18.75),
    ]
    df = spark.createDataFrame(data, schema_default)
    df.show(truncate=False)

    attr_path = (out_dir / "books_with_attrs").as_posix()
    (
        df.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "library")
        .option("rowTag", "book")
        .option("attributePrefix", "_")
        .save(attr_path)
    )
    print(f"Written with default prefix to {attr_path}")
    # Result: <book id="1" category="fiction"><title>...</title><price>12.99</price></book>

    df_back = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .option("attributePrefix", "_")
        .load(attr_path)
    )
    print("Round-trip (default prefix):")
    df_back.show(truncate=False)

    # ── 2. Custom attribute prefix (@) ──────────────────────────────
    print("\n=== 2. Custom attributePrefix (@) ===")
    schema_custom = StructType([
        StructField("@id", IntegerType()),
        StructField("@type", StringType()),
        StructField("name", StringType()),
        StructField("population", IntegerType()),
    ])
    city_data = [
        (1, "capital", "Paris", 2161000),
        (2, "capital", "Tokyo", 13960000),
        (3, "metro", "New York", 8336000),
        (4, "capital", "Berlin", 3645000),
    ]
    df_city = spark.createDataFrame(city_data, schema_custom)

    city_path = (out_dir / "cities_at_prefix").as_posix()
    (
        df_city.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "world")
        .option("rowTag", "city")
        .option("attributePrefix", "@")
        .save(city_path)
    )
    # Result: <city id="1" type="capital"><name>Paris</name>...</city>

    df_city_back = (
        spark.read.format("xml")
        .option("rowTag", "city")
        .option("attributePrefix", "@")
        .load(city_path)
    )
    print("Round-trip (@ prefix):")
    df_city_back.show(truncate=False)

    # ── 3. Mix attributes and valueTag ──────────────────────────────
    print("\n=== 3. Attributes + valueTag ===")
    # When a row has BOTH attributes and a text value, use valueTag
    schema_val = StructType([
        StructField("_unit", StringType()),        # attribute
        StructField("_timestamp", StringType()),   # attribute
        StructField("_VALUE", DoubleType()),       # text content via valueTag
    ])
    sensor_data = [
        ("celsius", "2025-01-15T10:30:00", 22.5),
        ("celsius", "2025-01-15T10:31:00", 22.8),
        ("celsius", "2025-01-15T10:32:00", 23.1),
        ("fahrenheit", "2025-01-15T10:30:00", 72.5),
    ]
    df_sensor = spark.createDataFrame(sensor_data, schema_val)

    sensor_path = (out_dir / "sensor_readings").as_posix()
    (
        df_sensor.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "readings")
        .option("rowTag", "reading")
        .option("attributePrefix", "_")
        .option("valueTag", "_VALUE")
        .save(sensor_path)
    )
    # Result: <reading unit="celsius" timestamp="...">22.5</reading>

    df_sensor_back = (
        spark.read.format("xml")
        .option("rowTag", "reading")
        .option("attributePrefix", "_")
        .option("valueTag", "_VALUE")
        .load(sensor_path)
    )
    print("Round-trip (attributes + valueTag):")
    df_sensor_back.show(truncate=False)

    spark.stop()
