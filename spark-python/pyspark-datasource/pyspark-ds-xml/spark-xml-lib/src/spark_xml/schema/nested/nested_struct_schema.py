"""Demonstrate complex nested schema definitions for XML.

Real-world XML often contains deeply nested structures with arrays of
structs, nested objects, and mixed content. This script shows:
- StructType with nested StructType (object within object)
- ArrayType of StructType (repeating child elements)
- Multi-level nesting (3+ levels deep)
- Accessing nested fields with dot notation and explode
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

COMPANY_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <companies>
      <company>
        <name>TechCorp</name>
        <founded>2010</founded>
        <headquarters>
          <city>San Francisco</city>
          <state>CA</state>
          <country>USA</country>
        </headquarters>
        <departments>
          <department>
            <dept_name>Engineering</dept_name>
            <head_count>150</head_count>
            <budget>5000000</budget>
            <teams>
              <team>
                <team_name>Backend</team_name>
                <members>45</members>
              </team>
              <team>
                <team_name>Frontend</team_name>
                <members>30</members>
              </team>
              <team>
                <team_name>DevOps</team_name>
                <members>15</members>
              </team>
            </teams>
          </department>
          <department>
            <dept_name>Marketing</dept_name>
            <head_count>40</head_count>
            <budget>2000000</budget>
            <teams>
              <team>
                <team_name>Digital</team_name>
                <members>20</members>
              </team>
              <team>
                <team_name>Content</team_name>
                <members>12</members>
              </team>
            </teams>
          </department>
        </departments>
      </company>
      <company>
        <name>DataInc</name>
        <founded>2015</founded>
        <headquarters>
          <city>Austin</city>
          <state>TX</state>
          <country>USA</country>
        </headquarters>
        <departments>
          <department>
            <dept_name>Data Science</dept_name>
            <head_count>80</head_count>
            <budget>3500000</budget>
            <teams>
              <team>
                <team_name>ML</team_name>
                <members>35</members>
              </team>
              <team>
                <team_name>Analytics</team_name>
                <members>25</members>
              </team>
            </teams>
          </department>
        </departments>
      </company>
    </companies>
""")

TEAM_SCHEMA = StructType([
    StructField("team_name", StringType(), True),
    StructField("members", LongType(), True),
])

DEPARTMENT_SCHEMA = StructType([
    StructField("dept_name", StringType(), True),
    StructField("head_count", LongType(), True),
    StructField("budget", DoubleType(), True),
    StructField("teams", StructType([
        StructField("team", ArrayType(TEAM_SCHEMA), True),
    ]), True),
])

HEADQUARTERS_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
])

COMPANY_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("founded", LongType(), True),
    StructField("headquarters", HEADQUARTERS_SCHEMA, True),
    StructField("departments", StructType([
        StructField("department", ArrayType(DEPARTMENT_SCHEMA), True),
    ]), True),
])


def write_xml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    xml_path = (
        Path(data_home) / "file_data" / "xml" / "schema_demo" / "companies.xml"
    )
    write_xml(xml_path, COMPANY_XML)

    spark = get_spark_session(
        app_name="spark-xml-nested-schema",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # --- 1. Read with explicit nested schema ---
    print("=== Nested Schema (3 levels deep) ===")
    df = (
        spark.read.format("xml")
        .option("rowTag", "company")
        .schema(COMPANY_SCHEMA)
        .load(xml_path.as_posix())
    )
    df.printSchema()
    df.show(truncate=False)

    # --- 2. Access nested fields with dot notation ---
    print("\n=== Dot Notation Access ===")
    df.select(
        "name",
        "headquarters.city",
        "headquarters.state",
    ).show(truncate=False)

    # --- 3. Explode departments array ---
    print("\n=== Explode Departments ===")
    df_depts = df.select(
        "name",
        F.explode("departments.department").alias("dept"),
    ).select(
        "name",
        "dept.dept_name",
        "dept.head_count",
        "dept.budget",
    )
    df_depts.show(truncate=False)

    # --- 4. Explode down to team level (2-level explode) ---
    print("\n=== Explode to Team Level ===")
    df_teams = (
        df.select(
            "name",
            F.explode("departments.department").alias("dept"),
        )
        .select(
            F.col("name").alias("company"),
            "dept.dept_name",
            F.explode("dept.teams.team").alias("team"),
        )
        .select(
            "company",
            "dept_name",
            "team.team_name",
            "team.members",
        )
    )
    df_teams.show(truncate=False)

    # --- 5. Compare with inferred schema ---
    print("\n=== Inferred Schema (for comparison) ===")
    df_inferred = (
        spark.read.format("xml")
        .option("rowTag", "company")
        .load(xml_path.as_posix())
    )
    df_inferred.printSchema()

    spark.stop()
