"""Inferred schema vs explicit schema comparison.

Without a schema, spark-xml infers all ambiguous columns
as StringType so nothing appears corrupt. With an explicit
numeric schema, type mismatches become visible as corrupt
records.
"""

import os
import sys

from pyspark.sql.types import (
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

if __name__ == "__main__":
    data_home = os.environ["DATA_HOME"]
    xml_file = os.path.join(
        data_home, "file_data", "xml", "error",
        "employees_corrupt.xml",
    )

    spark = get_spark_session(
        app_name="corrupt-infer-vs-explicit",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ── Inferred schema ──────────────────────────────────────
    print("=== Inferred schema (lenient — no corrupt) ===")
    df_inferred = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .load(xml_file)
    )
    df_inferred.printSchema()
    df_inferred.show(truncate=False)

    # ── Explicit schema ──────────────────────────────────────
    print("=== Explicit schema (strict — corrupt visible) ===")
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("salary", DoubleType(), True),
        StructField("department", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])

    df_explicit = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("mode", "PERMISSIVE")
        .option(
            "columnNameOfCorruptRecord", "_corrupt_record"
        )
        .schema(schema)
        .load(xml_file)
    )
    df_explicit.printSchema()
    df_explicit.show(truncate=False)

    spark.stop()
