"""
Nested Row objects — Row containing other Rows as field values,
mirroring StructType columns (struct fields) in a DataFrame schema.

Patterns covered:
  1. Row with a nested Row field    — struct column in a schema
  2. StructType with nested StructField
  3. asDict(recursive=True)         — deeply convert nested Rows to dicts
  4. Accessing nested fields        — row.address.city, row["address"]["city"]
  5. Array of Rows                  — ArrayType(StructType) column
  6. Selecting and exploding structs
  7. Constructing nested data from plain tuples
"""

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_frame.spark_utils import get_spark


def demo_nested_row_object(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 1. Row containing another Row as a field value
    # ------------------------------------------------------------------
    address = Row(street="10 Downing St", city="London", country="UK")
    person = Row(id=1, name="Alice", address=address)

    print("=== Nested Row object ===")
    print(f"  person          : {person}")
    print(f"  person.address  : {person.address}")
    print(f"  city via attr   : {person.address.city}")
    print(f"  city via dict   : {person['address']['city']}")

    df = spark.createDataFrame([person])
    df.printSchema()
    df.show(truncate=False)


def demo_structtype_schema(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 2. Explicit StructType with nested StructField
    # ------------------------------------------------------------------
    address_schema = StructType(
        [
            StructField("street", StringType(), nullable=True),
            StructField("city", StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
            StructField("postcode", StringType(), nullable=True),
        ]
    )
    person_schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("age", IntegerType(), nullable=True),
            StructField("address", address_schema, nullable=True),
        ]
    )

    # Data as nested tuples (address is a tuple matching address_schema)
    data = [
        (1, "Alice", 30, ("Baker St 1", "London", "UK", "NW1 6XE")),
        (2, "Bob", 45, ("Unter d. Linden", "Berlin", "DE", "10117")),
        (3, "Carol", 28, None),  # NULL address
    ]
    df = spark.createDataFrame(data, person_schema)

    print("\n=== StructType with nested StructField ===")
    df.printSchema()
    df.show(truncate=False)

    # Access nested field via dot notation in SQL / column expressions
    df.select(
        "id",
        "name",
        F.col("address.city").alias("city"),
        F.col("address.country").alias("country"),
    ).show(truncate=False)


def demo_as_dict_recursive(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 3. asDict(recursive=True) — nested Rows become nested dicts
    # ------------------------------------------------------------------
    address_schema = StructType(
        [
            StructField("city", StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
        ]
    )
    person_schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("address", address_schema, nullable=True),
        ]
    )
    data = [(1, "Alice", ("London", "UK")), (2, "Bob", ("Berlin", "DE"))]
    df = spark.createDataFrame(data, person_schema)

    print("\n=== asDict(recursive=True) ===")
    for row in df.collect():
        shallow = row.asDict(recursive=False)  # address is still a Row
        deep = row.asDict(recursive=True)  # address becomes a dict
        print(f"  shallow address type : {type(shallow['address']).__name__}")
        print(f"  deep    address      : {deep['address']}")
        print(f"  deep    full         : {deep}")


def demo_array_of_structs(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 4. ArrayType(StructType) — a column containing a list of structs
    # ------------------------------------------------------------------
    tag_schema = StructType(
        [
            StructField("name", StringType(), nullable=True),
            StructField("score", DoubleType(), nullable=True),
        ]
    )
    product_schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("tags", ArrayType(tag_schema), nullable=True),
        ]
    )

    data = [
        (1, "Widget", [("electronics", 0.9), ("popular", 0.8)]),
        (2, "Gadget", [("electronics", 0.7), ("sale", 1.0)]),
        (3, "Book", [("education", 0.6)]),
    ]
    df = spark.createDataFrame(data, product_schema)

    print("\n=== ArrayType(StructType) ===")
    df.printSchema()
    df.show(truncate=False)

    # Explode array of structs into separate rows
    df.select(
        "id",
        "name",
        F.explode("tags").alias("tag"),
    ).select(
        "id",
        "name",
        F.col("tag.name").alias("tag_name"),
        F.col("tag.score").alias("tag_score"),
    ).show(truncate=False)


def demo_nested_row_selection(spark: SparkSession) -> None:
    # ------------------------------------------------------------------
    # 5. withColumn to add / update a nested struct field
    # ------------------------------------------------------------------
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField(
                "loc",
                StructType(
                    [
                        StructField("lat", DoubleType(), nullable=True),
                        StructField("lon", DoubleType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )
    data = [
        (1, "Store A", (51.5074, -0.1278)),  # London
        (2, "Store B", (52.5200, 13.4050)),  # Berlin
        (3, "Store C", (43.6532, -79.3832)),  # Toronto
    ]
    df = spark.createDataFrame(data, schema)

    print("\n=== Selecting struct sub-fields ===")
    df.select(
        "id",
        "name",
        F.col("loc.lat").alias("latitude"),
        F.col("loc.lon").alias("longitude"),
        F.struct(
            F.round("loc.lat", 1).alias("lat"),
            F.round("loc.lon", 1).alias("lon"),
        ).alias("loc_rounded"),
    ).show(truncate=False)


def main(spark: SparkSession) -> None:
    demo_nested_row_object(spark)
    demo_structtype_schema(spark)
    demo_as_dict_recursive(spark)
    demo_array_of_structs(spark)
    demo_nested_row_selection(spark)


if __name__ == "__main__":
    spark = get_spark("row-nested")
    main(spark)
    spark.stop()
