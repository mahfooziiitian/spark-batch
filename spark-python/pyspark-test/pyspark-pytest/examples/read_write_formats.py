"""Example: Reading and writing data in multiple formats.

Demonstrates using the reader utilities for CSV, JSON, and Parquet,
including schema enforcement and format conversion.

Run:
    uv run python examples/read_write_formats.py
"""

import os
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def main() -> None:
    """Demonstrate reading and writing in CSV, JSON, and Parquet formats."""
    spark = (
        SparkSession.builder.appName("example-read-write-formats")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Create sample data
    data = [
        ("Alice", 30, "Engineering"),
        ("Bob", 25, "Marketing"),
        ("Charlie", 35, "Engineering"),
        ("Diana", 28, "Sales"),
        ("Eve", 32, "Marketing"),
    ]
    schema = StructType(
        [
            StructField("name", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=False),
            StructField("department", StringType(), nullable=False),
        ]
    )
    df = spark.createDataFrame(data, schema)

    with tempfile.TemporaryDirectory() as tmp_dir:
        base_path = Path(tmp_dir)

        # Write as CSV
        csv_path = str(base_path / "output.csv")
        df.write.mode("overwrite").option("header", "true").csv(csv_path)
        print(f"Written CSV to: {csv_path}")

        # Write as JSON
        json_path = str(base_path / "output.json")
        df.write.mode("overwrite").json(json_path)
        print(f"Written JSON to: {json_path}")

        # Write as Parquet (preferred format)
        parquet_path = str(base_path / "output.parquet")
        df.write.mode("overwrite").parquet(parquet_path)
        print(f"Written Parquet to: {parquet_path}")

        # Read back from Parquet and show
        print("\n=== Read Back from Parquet ===")
        parquet_df = spark.read.parquet(parquet_path)
        parquet_df.show()
        parquet_df.printSchema()

        # Read CSV with explicit schema (no inference overhead)
        print("=== Read CSV with Schema ===")
        csv_df = spark.read.format("csv").option("header", "true").schema(schema).load(csv_path)
        csv_df.show()

        # Demonstrate schema enforcement catches type mismatches
        print("=== Schema Details ===")
        print(f"Columns: {csv_df.columns}")
        print(f"Row count: {csv_df.count()}")
        print(f"Partitions: {csv_df.rdd.getNumPartitions()}")

    spark.stop()


if __name__ == "__main__":
    main()
