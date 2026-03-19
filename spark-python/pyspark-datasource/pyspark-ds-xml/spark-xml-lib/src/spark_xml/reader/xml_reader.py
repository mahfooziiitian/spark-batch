"""Basic XML reader demonstrating core spark-xml read options.

Generates a sample XML file and reads it multiple ways:
  1. Schema inference (default)
  2. Explicit schema
  3. With attribute and value-tag options
  4. With row validation (PERMISSIVE / DROPMALFORMED / FAILFAST)
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

SAMPLE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <catalog>
      <book id="bk101" lang="en">
        <author>Alice Johnson</author>
        <title>Learning PySpark</title>
        <genre>Technology</genre>
        <price>45.99</price>
        <publish_date>2024-03-01</publish_date>
        <description>A beginner-friendly guide to PySpark.</description>
      </book>
      <book id="bk102" lang="en">
        <author>Bob Smith</author>
        <title>XML Processing with Spark</title>
        <genre>Data Engineering</genre>
        <price>39.50</price>
        <publish_date>2023-11-20</publish_date>
        <description>End-to-end XML pipelines using spark-xml.</description>
      </book>
      <book id="bk103" lang="de">
        <author>Carol Lee</author>
        <title>Big Data Patterns</title>
        <genre>Computer Science</genre>
        <price>52.00</price>
        <publish_date>2024-06-10</publish_date>
        <description>Design patterns for large-scale data systems.</description>
      </book>
      <book id="bk104" lang="en">
        <author>David Chen</author>
        <title>Streaming Analytics</title>
        <genre>Data Engineering</genre>
        <price>48.75</price>
        <publish_date>2025-01-15</publish_date>
        <description>Real-time analytics with Spark Structured Streaming.</description>
      </book>
      <book id="bk105" lang="fr">
        <author>Eve Martin</author>
        <title>Graph Databases</title>
        <genre>Database</genre>
        <price>41.00</price>
        <publish_date>2025-03-05</publish_date>
        <description>Introduction to graph storage and querying.</description>
      </book>
    </catalog>
""")


def generate_sample_xml(file_path: Path) -> None:
    """Write the book catalog sample XML to *file_path*."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(SAMPLE_XML, encoding="utf-8")
    print(f"Generated sample XML → {file_path}")


if __name__ == "__main__":
    data_path = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "catalog.xml"
    generate_sample_xml(data_path)

    spark = get_spark_session(
        app_name="spark-xml-reader",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    xml_file = data_path.as_posix()

    # ── 1. Schema inference (default) ────────────────────────────────
    print("\n=== 1. Schema Inference ===")
    df_inferred = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .load(xml_file)
    )
    df_inferred.printSchema()
    df_inferred.show(truncate=False)

    # ── 2. Explicit schema ───────────────────────────────────────────
    print("\n=== 2. Explicit Schema ===")
    schema = StructType(
        [
            StructField("_id", StringType(), True),
            StructField("_lang", StringType(), True),
            StructField("author", StringType(), True),
            StructField("title", StringType(), True),
            StructField("genre", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("publish_date", StringType(), True),
            StructField("description", StringType(), True),
        ]
    )
    df_explicit = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .schema(schema)
        .load(xml_file)
    )
    df_explicit.printSchema()
    df_explicit.show(truncate=False)

    # ── 3. Custom attribute prefix & excludeAttribute ────────────────
    print("\n=== 3. Custom Attribute Prefix (attr_) ===")
    df_prefix = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .option("attributePrefix", "attr_")
        .load(xml_file)
    )
    df_prefix.select("attr_id", "attr_lang", "title", "author", "price").show(
        truncate=False
    )

    print("\n=== 3b. Exclude Attributes ===")
    df_no_attr = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .option("excludeAttribute", True)
        .load(xml_file)
    )
    df_no_attr.printSchema()
    df_no_attr.show(truncate=False)

    # ── 4. Row validation (PERMISSIVE mode with corrupt record) ──────
    print("\n=== 4. PERMISSIVE Mode ===")
    strict_schema = StructType(
        [
            StructField("_id", StringType(), True),
            StructField("author", StringType(), True),
            StructField("title", StringType(), True),
            StructField("price", LongType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )
    df_permissive = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(strict_schema)
        .load(xml_file)
    )
    df_permissive.show(truncate=False)

    # ── 5. Filter and select ─────────────────────────────────────────
    print("\n=== 5. Filter: Data Engineering books ===")
    df_inferred.filter(df_inferred.genre == "Data Engineering").select(
        "_id", "title", "author", "price"
    ).show(truncate=False)

    spark.stop()
