"""Demonstrate reading XML with an explicit StructType schema.

Providing an explicit schema avoids the overhead of schema inference and
guarantees consistent column names and data types regardless of the data.
This script shows:
- Flat StructType with all common PySpark data types
- How explicit schema overrides inferred types
- Handling nullable vs non-nullable fields
- Type coercion when schema type differs from XML text
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

EMPLOYEES_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <company>
      <employee>
        <id>1</id>
        <name>Alice Johnson</name>
        <department>Engineering</department>
        <salary>95000.50</salary>
        <age>32</age>
        <level>5</level>
        <active>true</active>
        <hire_date>2020-03-15</hire_date>
        <last_login>2024-11-20T09:30:00</last_login>
        <rating>4.7</rating>
      </employee>
      <employee>
        <id>2</id>
        <name>Bob Smith</name>
        <department>Marketing</department>
        <salary>72000.00</salary>
        <age>28</age>
        <level>3</level>
        <active>true</active>
        <hire_date>2022-07-01</hire_date>
        <last_login>2024-11-19T14:15:00</last_login>
        <rating>4.2</rating>
      </employee>
      <employee>
        <id>3</id>
        <name>Carol Lee</name>
        <department>Finance</department>
        <salary>88500.75</salary>
        <age>45</age>
        <level>7</level>
        <active>false</active>
        <hire_date>2015-01-10</hire_date>
        <last_login>2024-10-05T11:00:00</last_login>
        <rating>4.9</rating>
      </employee>
    </company>
""")

EMPLOYEE_SCHEMA = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("department", StringType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("level", ShortType(), nullable=True),
    StructField("active", BooleanType(), nullable=True),
    StructField("hire_date", DateType(), nullable=True),
    StructField("last_login", TimestampType(), nullable=True),
    StructField("rating", FloatType(), nullable=True),
])


def write_xml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    xml_path = Path(data_home) / "file_data" / "xml" / "schema_demo" / "employees.xml"
    write_xml(xml_path, EMPLOYEES_XML)

    spark = get_spark_session(
        app_name="spark-xml-explicit-schema",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # --- 1. Read with explicit schema ---
    print("=== Explicit StructType Schema ===")
    df = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .schema(EMPLOYEE_SCHEMA)
        .load(xml_path.as_posix())
    )
    df.printSchema()
    df.show(truncate=False)

    # --- 2. Compare with inferred schema ---
    print("\n=== Inferred Schema (for comparison) ===")
    df_inferred = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .load(xml_path.as_posix())
    )
    df_inferred.printSchema()

    print("\n=== Type Differences ===")
    for explicit, inferred in zip(
        EMPLOYEE_SCHEMA.fields, df_inferred.schema.fields
    ):
        marker = "✓" if explicit.dataType == inferred.dataType else "≠"
        print(
            f"  {explicit.name:15s}  explicit={explicit.dataType.simpleString():12s}  "
            f"inferred={inferred.dataType.simpleString():12s}  {marker}"
        )

    # --- 3. Demonstrate type coercion ---
    print("\n=== Type Coercion: salary as StringType ===")
    coerced_schema = StructType([
        StructField("name", StringType(), True),
        StructField("salary", StringType(), True),
    ])
    df_coerced = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .schema(coerced_schema)
        .load(xml_path.as_posix())
    )
    df_coerced.show(truncate=False)

    spark.stop()
