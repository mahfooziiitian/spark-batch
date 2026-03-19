"""DROPMALFORMED mode — silently drop unparseable rows.

Rows that fail schema validation are removed from the
result without raising an error. Useful when you want
only clean data and can tolerate data loss.
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
        app_name="corrupt-dropmalformed",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    schema = StructType([
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
        .schema(schema)
        .load(xml_file)
    )

    print("=== DROPMALFORMED results ===")
    print(f"Row count (malformed dropped): {df.count()}")
    df.show(truncate=False)

    spark.stop()
