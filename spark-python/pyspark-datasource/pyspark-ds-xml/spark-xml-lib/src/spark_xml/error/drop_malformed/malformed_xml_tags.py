"""PERMISSIVE mode — malformed XML (unclosed tags).

Rows with structurally broken XML (e.g. unclosed <customer>
tag) are captured as corrupt records. This is different from
type mismatches — the XML itself is invalid.
"""

import os
import sys

from pyspark.sql.types import (
    DateType,
    DoubleType,
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
        "orders_malformed.xml",
    )

    spark = get_spark_session(
        app_name="corrupt-malformed-xml",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("date", DateType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])

    df = (
        spark.read.format("xml")
        .option("rowTag", "order")
        .option("mode", "PERMISSIVE")
        .option(
            "columnNameOfCorruptRecord", "_corrupt_record"
        )
        .schema(schema)
        .load(xml_file)
    )

    print("=== All rows ===")
    df.show(truncate=False)

    print("=== Malformed XML rows ===")
    df.filter(
        df._corrupt_record.isNotNull()
    ).show(truncate=False)

    print("=== Valid rows ===")
    df.filter(
        df._corrupt_record.isNull()
    ).show(truncate=False)

    spark.stop()
