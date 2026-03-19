"""PERMISSIVE mode — extra fields ignored by schema.

When a strict schema is provided, XML elements not listed
in the schema are silently ignored. They do NOT cause a
corrupt record.
"""

import os
import sys

from pyspark.sql.types import (
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
        app_name="corrupt-extra-columns",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # Only id, name, age — salary, department, bonus
    # are silently dropped
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

    print("=== Schema has only id/name/age ===")
    print("salary, department, bonus are dropped:")
    df.printSchema()
    df.show(truncate=False)

    spark.stop()
