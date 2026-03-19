"""Demonstrate handling schema evolution in XML data.

When XML sources change over time (columns added, removed, or renamed),
spark-xml can still read the data. This script shows:
- Reading XML v1 (original columns) and v2 (new/removed columns)
- Using a union-compatible schema for both versions
- Handling missing columns gracefully (null values)
- Column aliasing for renamed fields
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql import functions as F
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

# Version 1: original schema
USERS_V1_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <users>
      <user>
        <id>1</id>
        <name>Alice</name>
        <email>alice@example.com</email>
        <age>30</age>
      </user>
      <user>
        <id>2</id>
        <name>Bob</name>
        <email>bob@example.com</email>
        <age>25</age>
      </user>
    </users>
""")

# Version 2: "age" removed, "phone" and "score" added, "name" → "full_name"
USERS_V2_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <users>
      <user>
        <id>3</id>
        <full_name>Carol Lee</full_name>
        <email>carol@example.com</email>
        <phone>555-0103</phone>
        <score>92.5</score>
      </user>
      <user>
        <id>4</id>
        <full_name>David Chen</full_name>
        <email>david@example.com</email>
        <phone>555-0104</phone>
        <score>87.0</score>
      </user>
    </users>
""")

# Unified schema covering both versions
UNIFIED_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", LongType(), True),
    StructField("phone", StringType(), True),
    StructField("score", DoubleType(), True),
])


def write_xml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "schema_demo"

    v1_path = base / "users_v1.xml"
    v2_path = base / "users_v2.xml"
    write_xml(v1_path, USERS_V1_XML)
    write_xml(v2_path, USERS_V2_XML)

    spark = get_spark_session(
        app_name="spark-xml-schema-evolution",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # --- 1. Read each version with its own inferred schema ---
    print("=== V1 Schema (inferred) ===")
    df_v1_inferred = (
        spark.read.format("xml")
        .option("rowTag", "user")
        .load(v1_path.as_posix())
    )
    df_v1_inferred.printSchema()
    df_v1_inferred.show(truncate=False)

    print("=== V2 Schema (inferred) ===")
    df_v2_inferred = (
        spark.read.format("xml")
        .option("rowTag", "user")
        .load(v2_path.as_posix())
    )
    df_v2_inferred.printSchema()
    df_v2_inferred.show(truncate=False)

    # --- 2. Read both versions with the unified schema ---
    print("=== V1 with Unified Schema (missing columns become null) ===")
    df_v1 = (
        spark.read.format("xml")
        .option("rowTag", "user")
        .schema(UNIFIED_SCHEMA)
        .load(v1_path.as_posix())
    )
    df_v1.show(truncate=False)

    print("=== V2 with Unified Schema (missing columns become null) ===")
    df_v2 = (
        spark.read.format("xml")
        .option("rowTag", "user")
        .schema(UNIFIED_SCHEMA)
        .load(v2_path.as_posix())
    )
    df_v2.show(truncate=False)

    # --- 3. Union both versions into a single DataFrame ---
    print("=== Combined (union of both versions) ===")
    df_combined = df_v1.unionByName(df_v2)
    df_combined.show(truncate=False)

    # --- 4. Normalize the renamed column ---
    print("=== Normalized: coalesce(full_name, name) ===")
    df_normalized = (
        df_combined
        .withColumn(
            "display_name", F.coalesce(F.col("full_name"), F.col("name"))
        )
        .select("id", "display_name", "email", "age", "phone", "score")
    )
    df_normalized.show(truncate=False)

    spark.stop()
