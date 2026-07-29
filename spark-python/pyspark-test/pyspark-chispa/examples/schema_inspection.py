"""Example: Schema inspection and comparison with chispa.

Demonstrates schema_to_dict, get_column_names_by_type, add_nullable_fields,
and chispa's assert_schema_equality.

Run:
    PYTHONPATH=src uv run python examples/schema_inspection.py
"""

import os

from chispa.schema_comparer import assert_schema_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from data_frame.schema.schema_utils import add_nullable_fields, get_column_names_by_type, schema_to_dict


def main() -> None:
    """Demonstrate schema inspection and comparison utilities."""
    spark = (
        SparkSession.builder.appName("example-schema").master(os.environ.get("SPARK_MASTER", "local[*]")).getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Create a typed DataFrame
    df = spark.createDataFrame(
        [(1, "Alice", 95000.50), (2, "Bob", 72000.00)],
        schema=StructType(
            [
                StructField("id", LongType(), nullable=False),
                StructField("name", StringType(), nullable=False),
                StructField("salary", DoubleType(), nullable=True),
            ]
        ),
    )

    print("=== Schema ===")
    df.printSchema()

    # Schema to dictionary
    schema_dict = schema_to_dict(df.schema)
    print(f"Schema as dict: {schema_dict}")

    # Get columns by type
    string_cols = get_column_names_by_type(df, "string")
    numeric_cols = get_column_names_by_type(df, "bigint")
    print(f"String columns: {string_cols}")
    print(f"Long columns: {numeric_cols}")

    # Make all fields nullable
    strict_schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("name", StringType(), nullable=False),
        ]
    )
    relaxed_schema = add_nullable_fields(strict_schema)

    print(f"\nStrict nullable: {[f.nullable for f in strict_schema.fields]}")
    print(f"Relaxed nullable: {[f.nullable for f in relaxed_schema.fields]}")

    # Verify schema equality with chispa
    expected = StructType(
        [
            StructField("id", LongType(), nullable=True),
            StructField("name", StringType(), nullable=True),
        ]
    )
    assert_schema_equality(relaxed_schema, expected)
    print("\n✅ chispa schema assertion passed")

    spark.stop()


if __name__ == "__main__":
    main()
