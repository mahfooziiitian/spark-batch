"""FAILFAST mode — exception on first bad row.

Immediately throws an exception when a row cannot be
parsed against the schema. Useful for strict data
pipelines that should not tolerate any bad data.
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
        app_name="corrupt-failfast",
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

    print("=== FAILFAST — expects exception ===")
    try:
        df = (
            spark.read.format("xml")
            .option("rowTag", "employee")
            .option("mode", "FAILFAST")
            .schema(schema)
            .load(xml_file)
        )
        df.show()
    except Exception as e:
        print(f"FAILFAST raised: {type(e).__name__}")
        msg = str(e).split("\n")[0]
        print(f"  {msg[:80]}")

    spark.stop()
