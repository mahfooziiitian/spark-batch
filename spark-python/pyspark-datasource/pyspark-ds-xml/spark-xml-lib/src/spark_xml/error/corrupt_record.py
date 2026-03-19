"""Corrupt record handling — all parse-mode scenarios.

Demonstrates PERMISSIVE, DROPMALFORMED, and FAILFAST modes
for handling type mismatches, malformed XML, missing fields,
extra columns, null elements, and bad date formats.
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_xml.util.session.spark_session_util import (
    get_spark_session,
)

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable


# ── Schemas ──────────────────────────────────────────────────────

EMPLOYEE_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("age", LongType(), True),
    StructField("salary", DoubleType(), True),
    StructField("department", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("date", DateType(), True),
    StructField("_corrupt_record", StringType(), True),
])


# ── Scenario helpers ─────────────────────────────────────────────

def scenario_permissive_type_mismatch(
    spark: SparkSession, xml_file: str
) -> None:
    """PERMISSIVE mode: type mismatches go to _corrupt_record.

    Rows where age/salary/id cannot be cast to the schema
    type are captured as corrupt records while valid rows
    are parsed normally.
    """
    print("\n" + "=" * 60)
    print("1. PERMISSIVE — type mismatch (age, salary, id)")
    print("=" * 60)

    df = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("mode", "PERMISSIVE")
        .option(
            "columnNameOfCorruptRecord", "_corrupt_record"
        )
        .schema(EMPLOYEE_SCHEMA)
        .load(xml_file)
    )
    df.show(truncate=False)

    print("Corrupt records only:")
    df.filter(
        df._corrupt_record.isNotNull()
    ).show(truncate=False)

    print("Valid records only:")
    df.filter(
        df._corrupt_record.isNull()
    ).show(truncate=False)


def scenario_permissive_missing_fields(
    spark: SparkSession, xml_file: str
) -> None:
    """PERMISSIVE mode: empty / self-closing elements.

    Empty tags (<name></name>) and self-closing tags
    (<name/>) become nulls — these are NOT corrupt records
    because the XML is structurally valid.
    """
    print("\n" + "=" * 60)
    print("2. PERMISSIVE — empty & self-closing elements")
    print("=" * 60)

    df = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("mode", "PERMISSIVE")
        .option(
            "columnNameOfCorruptRecord", "_corrupt_record"
        )
        .schema(EMPLOYEE_SCHEMA)
        .load(xml_file)
    )

    print("Rows with null name or salary:")
    df.filter(
        df.name.isNull() | df.salary.isNull()
    ).show(truncate=False)


def scenario_permissive_bad_dates(
    spark: SparkSession, xml_file: str
) -> None:
    """PERMISSIVE mode: unparseable date values.

    Date strings that don't match the expected format are
    captured as corrupt records.
    """
    print("\n" + "=" * 60)
    print("3. PERMISSIVE — bad date format")
    print("=" * 60)

    df = (
        spark.read.format("xml")
        .option("rowTag", "order")
        .option("mode", "PERMISSIVE")
        .option("dateFormat", "yyyy-MM-dd")
        .option(
            "columnNameOfCorruptRecord", "_corrupt_record"
        )
        .schema(ORDER_SCHEMA)
        .load(xml_file)
    )
    df.show(truncate=False)

    print("Corrupt records (bad dates / malformed XML):")
    df.filter(
        df._corrupt_record.isNotNull()
    ).show(truncate=False)


def scenario_permissive_extra_columns(
    spark: SparkSession, xml_file: str
) -> None:
    """PERMISSIVE mode: extra fields not in schema.

    When a strict schema is provided, XML elements not
    listed in the schema are silently ignored — they do NOT
    cause a corrupt record.
    """
    print("\n" + "=" * 60)
    print("4. PERMISSIVE — extra fields ignored by schema")
    print("=" * 60)

    strict_schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
    ])

    df = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("mode", "PERMISSIVE")
        .schema(strict_schema)
        .load(xml_file)
    )
    print("Schema has only id/name/age — salary, dept, "
          "bonus are dropped:")
    df.show(truncate=False)


def scenario_permissive_nullvalue(
    spark: SparkSession, xml_file: str
) -> None:
    """PERMISSIVE mode: custom nullValue mapping.

    Map specific sentinel strings to null during read.
    """
    print("\n" + "=" * 60)
    print("5. PERMISSIVE — nullValue mapping")
    print("=" * 60)

    schema_no_corrupt = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("salary", DoubleType(), True),
        StructField("department", StringType(), True),
    ])

    df = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .schema(schema_no_corrupt)
        .load(xml_file)
    )
    print("Empty strings mapped to null via nullValue='':")
    df.show(truncate=False)


def scenario_dropmalformed(
    spark: SparkSession, xml_file: str
) -> None:
    """DROPMALFORMED mode: silently drop unparseable rows.

    Rows that fail schema validation are removed from the
    result without error.
    """
    print("\n" + "=" * 60)
    print("6. DROPMALFORMED — drop bad rows silently")
    print("=" * 60)

    schema_no_corrupt = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("salary", DoubleType(), True),
        StructField("department", StringType(), True),
    ])

    df = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("mode", "DROPMALFORMED")
        .schema(schema_no_corrupt)
        .load(xml_file)
    )
    print(f"Row count (malformed rows dropped): {df.count()}")
    df.show(truncate=False)


def scenario_failfast(
    spark: SparkSession, xml_file: str
) -> None:
    """FAILFAST mode: raise exception on first bad row.

    Immediately throws an exception when a row cannot be
    parsed, useful for strict data pipelines.
    """
    print("\n" + "=" * 60)
    print("7. FAILFAST — exception on first bad row")
    print("=" * 60)

    schema_no_corrupt = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("salary", DoubleType(), True),
        StructField("department", StringType(), True),
    ])

    try:
        df = (
            spark.read.format("xml")
            .option("rowTag", "employee")
            .option("mode", "FAILFAST")
            .schema(schema_no_corrupt)
            .load(xml_file)
        )
        df.show()
    except Exception as e:
        print(f"FAILFAST raised: {type(e).__name__}")
        msg = str(e).split("\n")[0]
        print(f"  {msg[:80]}")


def scenario_infer_vs_explicit_schema(
    spark: SparkSession, xml_file: str
) -> None:
    """Compare inferred schema (lenient) vs explicit schema.

    Without a schema, spark-xml infers all ambiguous columns
    as StringType so nothing appears corrupt. With an
    explicit numeric schema, mismatches become visible.
    """
    print("\n" + "=" * 60)
    print("8. Inferred schema vs explicit schema")
    print("=" * 60)

    df_inferred = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .load(xml_file)
    )
    print("Inferred schema (all strings — no corrupt):")
    df_inferred.printSchema()
    df_inferred.show(truncate=False)

    df_explicit = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("mode", "PERMISSIVE")
        .option(
            "columnNameOfCorruptRecord", "_corrupt_record"
        )
        .schema(EMPLOYEE_SCHEMA)
        .load(xml_file)
    )
    print("Explicit schema (numeric types — corrupt visible):")
    df_explicit.printSchema()
    df_explicit.show(truncate=False)


def scenario_malformed_xml_tags(
    spark: SparkSession, xml_file: str
) -> None:
    """PERMISSIVE mode: structurally broken XML (unclosed tags).

    Rows with malformed XML structure (e.g. unclosed
    <customer> tag) are captured as corrupt records.
    """
    print("\n" + "=" * 60)
    print("9. PERMISSIVE — malformed XML (unclosed tags)")
    print("=" * 60)

    df = (
        spark.read.format("xml")
        .option("rowTag", "order")
        .option("mode", "PERMISSIVE")
        .option(
            "columnNameOfCorruptRecord", "_corrupt_record"
        )
        .schema(ORDER_SCHEMA)
        .load(xml_file)
    )
    df.show(truncate=False)

    print("Malformed XML rows:")
    df.filter(
        df._corrupt_record.isNotNull()
    ).show(truncate=False)


# ── Main ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    data_home = os.environ["DATA_HOME"]
    emp_file = os.path.join(
        data_home, "file_data", "xml", "error",
        "employees_corrupt.xml",
    )
    order_file = os.path.join(
        data_home, "file_data", "xml", "error",
        "orders_malformed.xml",
    )

    spark = get_spark_session(
        app_name="spark-xml-corrupt-records",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # Employee scenarios (type mismatch, nulls, extras)
    scenario_permissive_type_mismatch(spark, emp_file)
    scenario_permissive_missing_fields(spark, emp_file)
    scenario_permissive_extra_columns(spark, emp_file)
    scenario_permissive_nullvalue(spark, emp_file)
    scenario_dropmalformed(spark, emp_file)
    scenario_failfast(spark, emp_file)
    scenario_infer_vs_explicit_schema(spark, emp_file)

    # Order scenarios (bad dates, malformed XML)
    scenario_permissive_bad_dates(spark, order_file)
    scenario_malformed_xml_tags(spark, order_file)

    spark.stop()
