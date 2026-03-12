"""Demonstrate reading XML with a non-ISO timestamp format using the
dateFormat option *before* loading — spark-xml parses the custom format
directly into TimestampType, with PERMISSIVE corrupt-record handling.

Generates sample XML with dd-MM-yyyy HH:mm:ss timestamps.
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

NON_ISO_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <records>
      <record>
        <id>1</id>
        <created_at>15-03-2024 09:30:00</created_at>
      </record>
      <record>
        <id>2</id>
        <created_at>22-07-2024 14:45:30</created_at>
      </record>
      <record>
        <id>3</id>
        <created_at>INVALID-TIMESTAMP</created_at>
      </record>
      <record>
        <id>4</id>
        <created_at>30-01-2025 17:00:00</created_at>
      </record>
    </records>
""")


def generate_non_iso_xml(file_path: Path) -> None:
    """Write the non-ISO timestamp sample XML to *file_path*."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(NON_ISO_XML, encoding="utf-8")
    print(f"Generated sample XML → {file_path}")


if __name__ == "__main__":
    data_home = os.environ["DATA_HOME"]
    data_path = (
        Path(data_home) / "file_data" / "xml" / "date_time" / "non_iso_timestamp.xml"
    )
    generate_non_iso_xml(data_path)

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("_corrupt_record", StringType()),
        ]
    )

    spark = get_spark_session(
        app_name="spark-xml-custom-datetime-before",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    df = (
        spark.read.format("xml")
        .option("rowTag", "record")
        .option("dateFormat", "dd-MM-yyyy")
        .option("timestampFormat", "dd-MM-yyyy HH:mm:ss")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(data_path.as_posix())
    )

    print("\n=== Parsed with dateFormat (PERMISSIVE) ===")
    df.show(truncate=False)
    df.printSchema()

    spark.stop()
