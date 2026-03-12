"""Basic XML writer — demonstrates fundamental write options.

Covers rootTag, rowTag, declaration, write modes (overwrite/append),
and round-trip read-back verification.
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


def create_employees_df(spark: SparkSession):
    """Create a sample employees DataFrame."""
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("department", StringType()),
        StructField("salary", DoubleType()),
    ])
    data = [
        (1, "Alice Johnson", "Engineering", 95000.50),
        (2, "Bob Smith", "Marketing", 82000.00),
        (3, "Carol Williams", "Finance", 105000.75),
        (4, "Dave Brown", "Engineering", 88500.25),
        (5, "Eve Davis", "HR", 76000.00),
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
        app_name="xml-writer-basic",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    df = create_employees_df(spark)
    df.printSchema()
    df.show(truncate=False)

    # ── 1. Basic write with rootTag and rowTag ──────────────────────
    print("\n=== 1. Basic write (rootTag + rowTag) ===")
    basic_path = (out_dir / "basic_employees").as_posix()
    (
        df.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "employees")
        .option("rowTag", "employee")
        .save(basic_path)
    )
    print(f"Written to {basic_path}")

    # Read back to verify round-trip
    df_back = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .load(basic_path)
    )
    print("Round-trip read:")
    df_back.show(truncate=False)

    # ── 2. Write with XML declaration ───────────────────────────────
    print("\n=== 2. Write with XML declaration ===")
    decl_path = (out_dir / "with_declaration").as_posix()
    (
        df.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "employees")
        .option("rowTag", "employee")
        .option("declaration", 'xml version="1.0" encoding="UTF-8"')
        .save(decl_path)
    )
    print(f"Written with declaration to {decl_path}")

    # ── 3. Overwrite mode ───────────────────────────────────────────
    print("\n=== 3. Overwrite mode ===")
    overwrite_path = (out_dir / "overwrite_test").as_posix()
    # First write
    (
        df.limit(2).write.format("xml")
        .mode("overwrite")
        .option("rootTag", "employees")
        .option("rowTag", "employee")
        .save(overwrite_path)
    )
    count_1 = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .load(overwrite_path)
        .count()
    )
    print(f"After first write: {count_1} rows")

    # Overwrite with full data
    (
        df.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "employees")
        .option("rowTag", "employee")
        .save(overwrite_path)
    )
    count_2 = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .load(overwrite_path)
        .count()
    )
    print(f"After overwrite: {count_2} rows (original 2 replaced with 5)")

    # ── 4. Simulated append (union + overwrite) ────────────────────
    # spark-xml does not support append mode directly, so we
    # read existing data, union new rows, and write to a temp
    # path before replacing — avoids read/write on same path.
    print("\n=== 4. Simulated append (union + overwrite) ===")
    append_path = (out_dir / "append_test").as_posix()
    append_tmp = (out_dir / "append_test_tmp").as_posix()
    (
        df.limit(3).write.format("xml")
        .mode("overwrite")
        .option("rootTag", "employees")
        .option("rowTag", "employee")
        .save(append_path)
    )
    existing = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .load(append_path)
    )
    combined = existing.unionByName(df.limit(2))
    (
        combined.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "employees")
        .option("rowTag", "employee")
        .save(append_tmp)
    )
    # Replace original with merged result
    count_a = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .load(append_tmp)
        .count()
    )
    print(f"After 3 + 2 simulated append: {count_a} rows")

    # ── 5. Custom null value representation ─────────────────────────
    print("\n=== 5. Write with null values ===")
    null_data = [
        (6, "Frank", None, 70000.0),
        (7, None, "Sales", None),
    ]
    df_null = spark.createDataFrame(null_data, df.schema)
    null_path = (out_dir / "with_nulls").as_posix()
    (
        df_null.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "employees")
        .option("rowTag", "employee")
        .option("nullValue", "N/A")
        .save(null_path)
    )
    df_null_back = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("nullValue", "N/A")
        .load(null_path)
    )
    print("Null values written as 'N/A':")
    df_null_back.show(truncate=False)

    spark.stop()
