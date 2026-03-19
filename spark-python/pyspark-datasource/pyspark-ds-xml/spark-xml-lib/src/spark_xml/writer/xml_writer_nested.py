"""XML writer with nested structs, arrays, and maps.

Demonstrates how spark-xml serialises complex PySpark types
(StructType, ArrayType, MapType) into nested XML elements.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def create_orders_df(spark: SparkSession):
    """Create a DataFrame with nested structs and arrays."""
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer", StructType([
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("address", StructType([
                StructField("street", StringType()),
                StructField("city", StringType()),
                StructField("zip", StringType()),
            ])),
        ])),
        StructField("items", ArrayType(StructType([
            StructField("product", StringType()),
            StructField("quantity", IntegerType()),
            StructField("price", DoubleType()),
        ]))),
    ])
    data = [
        (
            "ORD-001",
            ("Alice Johnson", "alice@example.com", ("123 Main St", "New York", "10001")),
            [("Laptop", 1, 1299.99), ("Mouse", 2, 29.99)],
        ),
        (
            "ORD-002",
            ("Bob Smith", "bob@example.com", ("456 Oak Ave", "Chicago", "60601")),
            [("Keyboard", 1, 79.99), ("Monitor", 1, 499.99), ("USB Hub", 3, 19.99)],
        ),
        (
            "ORD-003",
            ("Carol Williams", "carol@example.com", ("789 Pine Rd", "Austin", "73301")),
            [("Headphones", 2, 149.99)],
        ),
    ]
    return spark.createDataFrame(data, schema)


def create_map_df(spark: SparkSession):
    """Create a DataFrame with MapType columns."""
    schema = StructType([
        StructField("product_id", StringType()),
        StructField("name", StringType()),
        StructField("attributes", MapType(StringType(), StringType())),
        StructField("prices", MapType(StringType(), DoubleType())),
    ])
    data = [
        ("P001", "Laptop", {"brand": "Dell", "color": "Silver", "ram": "16GB"}, {"USD": 1299.99, "EUR": 1199.99}),
        ("P002", "Phone", {"brand": "Samsung", "color": "Black", "storage": "256GB"}, {"USD": 899.99, "EUR": 829.99}),
        ("P003", "Tablet", {"brand": "Apple", "color": "Space Gray"}, {"USD": 599.99}),
    ]
    return spark.createDataFrame(data, schema)


if __name__ == "__main__":
    out_dir = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "writer_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session(
        app_name="xml-writer-nested",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ── 1. Nested structs and arrays ────────────────────────────────
    print("\n=== 1. Write nested structs + arrays ===")
    df_orders = create_orders_df(spark)
    df_orders.printSchema()
    df_orders.show(truncate=False)

    nested_path = (out_dir / "nested_orders").as_posix()
    (
        df_orders.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "orders")
        .option("rowTag", "order")
        .save(nested_path)
    )
    print(f"Written nested orders to {nested_path}")

    # Read back and verify structure preserved
    df_back = (
        spark.read.format("xml")
        .option("rowTag", "order")
        .load(nested_path)
    )
    print("\nRound-trip nested read:")
    df_back.printSchema()
    df_back.show(truncate=False)

    # ── 2. MapType columns ──────────────────────────────────────────
    print("\n=== 2. Write MapType columns ===")
    df_map = create_map_df(spark)
    df_map.printSchema()
    df_map.show(truncate=False)

    map_path = (out_dir / "map_products").as_posix()
    (
        df_map.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "products")
        .option("rowTag", "product")
        .save(map_path)
    )
    print(f"Written map columns to {map_path}")

    df_map_back = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .load(map_path)
    )
    print("\nRound-trip map read:")
    df_map_back.printSchema()
    df_map_back.show(truncate=False)

    # ── 3. Array of arrays (deeply nested) ──────────────────────────
    print("\n=== 3. Deeply nested array of arrays ===")
    deep_schema = StructType([
        StructField("class_name", StringType()),
        StructField("students", ArrayType(StructType([
            StructField("name", StringType()),
            StructField("grades", ArrayType(IntegerType())),
        ]))),
    ])
    deep_data = [
        ("Math 101", [("Alice", [95, 88, 92]), ("Bob", [78, 85, 90])]),
        ("Science 201", [("Carol", [100, 97, 95]), ("Dave", [88, 82])]),
    ]
    df_deep = spark.createDataFrame(deep_data, deep_schema)
    deep_path = (out_dir / "deep_nested").as_posix()
    (
        df_deep.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "classes")
        .option("rowTag", "class")
        .save(deep_path)
    )
    df_deep_back = (
        spark.read.format("xml")
        .option("rowTag", "class")
        .load(deep_path)
    )
    print("Deep nested round-trip:")
    df_deep_back.printSchema()
    df_deep_back.show(truncate=False)

    spark.stop()
