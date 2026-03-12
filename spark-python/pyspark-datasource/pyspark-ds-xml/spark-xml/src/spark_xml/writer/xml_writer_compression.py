"""XML writer with compression — gzip, bzip2, deflate, lz4, snappy.

Demonstrates writing XML with various compression codecs and
compares file sizes vs uncompressed output.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand
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


def create_large_df(spark: SparkSession, num_rows: int = 500):
    """Create a larger DataFrame to make compression differences visible."""
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("department", StringType()),
        StructField("salary", DoubleType()),
        StructField("description", StringType()),
    ])

    departments = ["Engineering", "Marketing", "Finance", "HR", "Sales", "Operations"]
    names = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank"]

    data = []
    for i in range(1, num_rows + 1):
        data.append((
            i,
            f"{names[i % len(names)]} {names[(i * 3) % len(names)]}son",
            departments[i % len(departments)],
            50000.0 + (i * 137.5) % 80000,
            f"Employee {i} works in the {departments[i % len(departments)]} department "
            f"and has been with the company for {i % 20} years. "
            f"Performance rating: {'Excellent' if i % 5 == 0 else 'Good'}.",
        ))
    return spark.createDataFrame(data, schema)


def dir_size_kb(path: str) -> float:
    """Calculate total size of files in a directory in KB."""
    total = 0
    p = Path(path)
    if p.exists():
        for f in p.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
    return total / 1024.0


if __name__ == "__main__":
    out_dir = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "writer_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session(
        app_name="xml-writer-compression",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    df = create_large_df(spark, num_rows=500)
    print(f"DataFrame: {df.count()} rows")
    df.show(5, truncate=False)

    codecs = {
        "none": None,
        "gzip": "gzip",
        "bzip2": "bzip2",
        "deflate": "deflate",
    }

    results = []

    for label, codec in codecs.items():
        print(f"\n=== Writing with compression: {label} ===")
        path = (out_dir / f"compressed_{label}").as_posix()

        writer = (
            df.coalesce(1)
            .write.format("xml")
            .mode("overwrite")
            .option("rootTag", "employees")
            .option("rowTag", "employee")
        )
        if codec:
            writer = writer.option("compression", codec)
        writer.save(path)

        size = dir_size_kb(path)
        results.append((label, size))
        print(f"  Output size: {size:.1f} KB")

        # Verify read-back
        reader = spark.read.format("xml").option("rowTag", "employee")
        if codec:
            reader = reader.option("compression", codec)
        count = reader.load(path).count()
        print(f"  Read-back count: {count} rows ✓")

    # ── Summary ─────────────────────────────────────────────────────
    print("\n=== Compression Summary ===")
    print(f"{'Codec':<12} {'Size (KB)':>10} {'Ratio':>8}")
    print("-" * 32)
    base_size = results[0][1] if results[0][1] > 0 else 1
    for label, size in results:
        ratio = size / base_size
        print(f"{label:<12} {size:>10.1f} {ratio:>7.1%}")

    spark.stop()
