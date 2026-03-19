"""XML writer with schema control — explicit schema, column selection, transforms.

Demonstrates writing subsets of columns, renaming columns for XML output,
casting types, and controlling the output schema.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    format_number,
    lit,
    struct,
    upper,
    when,
)
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


def create_product_df(spark: SparkSession):
    """Create a product catalog DataFrame."""
    schema = StructType([
        StructField("product_id", IntegerType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("price", DoubleType()),
        StructField("stock", IntegerType()),
        StructField("supplier", StringType()),
        StructField("weight_kg", DoubleType()),
    ])
    data = [
        (101, "Laptop Pro", "Electronics", 1499.99, 45, "TechCorp", 2.1),
        (102, "Wireless Mouse", "Accessories", 29.99, 500, "PeripheralCo", 0.08),
        (103, "USB-C Hub", "Accessories", 49.99, 200, "PeripheralCo", 0.12),
        (104, "4K Monitor", "Electronics", 599.99, 30, "DisplayTech", 5.5),
        (105, "Ergonomic Chair", "Furniture", 349.99, 15, "OfficePro", 12.3),
        (106, "Standing Desk", "Furniture", 499.99, 10, "OfficePro", 25.0),
        (107, "Webcam HD", "Electronics", 79.99, 150, "TechCorp", 0.15),
        (108, "Noise-Cancel Headphones", "Electronics", 249.99, 80, "AudioMax", 0.32),
    ]
    return spark.createDataFrame(data, schema)


if __name__ == "__main__":
    out_dir = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "writer_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session(
        app_name="xml-writer-schema",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    df = create_product_df(spark)
    print("=== Source DataFrame ===")
    df.printSchema()
    df.show(truncate=False)

    # ── 1. Write a subset of columns ────────────────────────────────
    print("\n=== 1. Column selection (subset) ===")
    subset_path = (out_dir / "products_subset").as_posix()
    (
        df.select("product_id", "name", "price")
        .write.format("xml")
        .mode("overwrite")
        .option("rootTag", "catalog")
        .option("rowTag", "product")
        .save(subset_path)
    )
    spark.read.format("xml").option("rowTag", "product").load(subset_path).show(truncate=False)

    # ── 2. Rename columns for XML-friendly names ────────────────────
    print("\n=== 2. Renamed columns ===")
    renamed_path = (out_dir / "products_renamed").as_posix()
    (
        df.select(
            col("product_id").alias("ProductID"),
            col("name").alias("ProductName"),
            col("category").alias("Category"),
            col("price").alias("UnitPrice"),
            col("stock").alias("QuantityInStock"),
        )
        .write.format("xml")
        .mode("overwrite")
        .option("rootTag", "Catalog")
        .option("rowTag", "Product")
        .save(renamed_path)
    )
    spark.read.format("xml").option("rowTag", "Product").load(renamed_path).show(truncate=False)

    # ── 3. Transform before writing ─────────────────────────────────
    print("\n=== 3. Transform before write (computed columns) ===")
    transform_path = (out_dir / "products_transformed").as_posix()
    (
        df.select(
            col("product_id"),
            upper(col("name")).alias("name_upper"),
            col("category"),
            col("price"),
            col("stock"),
            (col("price") * col("stock")).alias("inventory_value"),
            when(col("stock") < 20, "LOW")
            .when(col("stock") < 100, "MEDIUM")
            .otherwise("HIGH")
            .alias("stock_level"),
        )
        .write.format("xml")
        .mode("overwrite")
        .option("rootTag", "inventory")
        .option("rowTag", "item")
        .save(transform_path)
    )
    spark.read.format("xml").option("rowTag", "item").load(transform_path).show(truncate=False)

    # ── 4. Nest flat columns into structs for XML hierarchy ─────────
    print("\n=== 4. Create nested XML from flat columns ===")
    nested_path = (out_dir / "products_nested_struct").as_posix()
    (
        df.select(
            col("product_id").alias("_id"),  # attribute via prefix
            col("name"),
            struct(
                col("category"),
                col("supplier"),
            ).alias("classification"),
            struct(
                col("price"),
                col("stock"),
                (col("price") * col("stock")).alias("total_value"),
            ).alias("pricing"),
            struct(
                col("weight_kg"),
                when(col("weight_kg") < 1.0, "Light")
                .when(col("weight_kg") < 10.0, "Medium")
                .otherwise("Heavy")
                .alias("weight_class"),
            ).alias("shipping"),
        )
        .write.format("xml")
        .mode("overwrite")
        .option("rootTag", "catalog")
        .option("rowTag", "product")
        .option("attributePrefix", "_")
        .save(nested_path)
    )
    df_nested = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .option("attributePrefix", "_")
        .load(nested_path)
    )
    df_nested.printSchema()
    df_nested.show(truncate=False)

    # ── 5. Cast types before writing ────────────────────────────────
    print("\n=== 5. Type casting (doubles → formatted strings) ===")
    cast_path = (out_dir / "products_formatted").as_posix()
    (
        df.select(
            col("product_id"),
            col("name"),
            format_number(col("price"), 2).alias("price_formatted"),
            col("price").cast("string").alias("price_raw"),
            col("weight_kg").cast("string").alias("weight"),
        )
        .write.format("xml")
        .mode("overwrite")
        .option("rootTag", "catalog")
        .option("rowTag", "product")
        .save(cast_path)
    )
    spark.read.format("xml").option("rowTag", "product").load(cast_path).show(truncate=False)

    spark.stop()
