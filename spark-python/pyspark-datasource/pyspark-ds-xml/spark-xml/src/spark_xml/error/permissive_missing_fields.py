"""PERMISSIVE mode — empty and self-closing elements.

Empty tags (<name></name>) and self-closing tags (<name/>)
become nulls. These are NOT corrupt records because the XML
is structurally valid.
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
        app_name="corrupt-missing-fields",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("salary", DoubleType(), True),
        StructField("department", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])

    df = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("mode", "PERMISSIVE")
        .option(
            "columnNameOfCorruptRecord", "_corrupt_record"
        )
        .schema(schema)
        .load(xml_file)
    )

    print("=== Rows with null name or salary ===")
    df.filter(
        df.name.isNull() | df.salary.isNull()
    ).show(truncate=False)

    spark.stop()
