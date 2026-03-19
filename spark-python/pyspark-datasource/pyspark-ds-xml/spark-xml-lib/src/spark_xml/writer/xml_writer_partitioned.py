"""XML writer with partitioning — partitionBy columns.

Demonstrates writing partitioned XML output (directory-per-partition)
and reading it back with partition discovery.
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


def create_sales_df(spark: SparkSession):
    """Create a sales DataFrame suitable for partitioning demos."""
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("product", StringType()),
        StructField("amount", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("region", StringType()),
        StructField("year", IntegerType()),
        StructField("quarter", StringType()),
    ])
    data = [
        ("T001", "Laptop", 1299.99, 1, "North", 2024, "Q1"),
        ("T002", "Phone", 899.99, 2, "South", 2024, "Q1"),
        ("T003", "Tablet", 499.99, 3, "North", 2024, "Q2"),
        ("T004", "Laptop", 1399.99, 1, "East", 2024, "Q2"),
        ("T005", "Monitor", 349.99, 2, "South", 2024, "Q3"),
        ("T006", "Phone", 799.99, 1, "West", 2024, "Q3"),
        ("T007", "Keyboard", 129.99, 5, "North", 2024, "Q4"),
        ("T008", "Laptop", 1199.99, 1, "East", 2024, "Q4"),
        ("T009", "Tablet", 599.99, 2, "West", 2025, "Q1"),
        ("T010", "Phone", 949.99, 1, "North", 2025, "Q1"),
        ("T011", "Monitor", 449.99, 3, "South", 2025, "Q1"),
        ("T012", "Laptop", 1499.99, 1, "East", 2025, "Q2"),
    ]
    return spark.createDataFrame(data, schema)


if __name__ == "__main__":
    out_dir = (
        Path(os.environ["DATA_HOME"])
        / "file_data"
        / "xml"
        / "writer_output"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session(
        app_name="xml-writer-partitioned",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    df = create_sales_df(spark)
    df.show(truncate=False)

    # ── 1. Partition by single column ───────────────────────────────
    print("\n=== 1. Partition by region ===")
    single_path = (out_dir / "sales_by_region").as_posix()
    (
        df.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "sales")
        .option("rowTag", "transaction")
        .partitionBy("region")
        .save(single_path)
    )
    print(f"Written to {single_path}")

    # Show partition directory structure
    for p in sorted(Path(single_path).rglob("*.xml")):
        print(f"  {p.relative_to(single_path)}")

    # Read back with partition discovery
    df_region = (
        spark.read.format("xml")
        .option("rowTag", "transaction")
        .load(single_path)
    )
    print("\nRead back (region is discovered as partition column):")
    df_region.printSchema()
    df_region.show(truncate=False)

    # ── 2. Partition by multiple columns ────────────────────────────
    print("\n=== 2. Partition by year + quarter ===")
    multi_path = (out_dir / "sales_by_year_quarter").as_posix()
    (
        df.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "sales")
        .option("rowTag", "transaction")
        .partitionBy("year", "quarter")
        .save(multi_path)
    )
    print(f"Written to {multi_path}")

    for p in sorted(Path(multi_path).rglob("*.xml")):
        print(f"  {p.relative_to(multi_path)}")

    # Read back a single partition via filter (safer than
    # hardcoding a partition path that may not exist)
    print("\nRead only 2024/Q1:")
    df_2024_q1 = (
        spark.read.format("xml")
        .option("rowTag", "transaction")
        .load(multi_path)
        .filter("year = 2024 AND quarter = 'Q1'")
    )
    df_2024_q1.show(truncate=False)

    # Read all with filter pushdown on partition columns
    print("\nRead all, filter year=2025:")
    df_all = (
        spark.read.format("xml")
        .option("rowTag", "transaction")
        .load(multi_path)
    )
    df_all.filter("year = 2025").show(truncate=False)

    # ── 3. Repartition before writing ───────────────────────────────
    print("\n=== 3. Coalesce to single file per partition ===")
    coalesce_path = (out_dir / "sales_coalesced").as_posix()
    (
        df.coalesce(1)
        .write.format("xml")
        .mode("overwrite")
        .option("rootTag", "sales")
        .option("rowTag", "transaction")
        .partitionBy("region")
        .save(coalesce_path)
    )
    for p in sorted(Path(coalesce_path).rglob("*.xml")):
        size = p.stat().st_size
        print(f"  {p.relative_to(coalesce_path)}  ({size} bytes)")

    spark.stop()
