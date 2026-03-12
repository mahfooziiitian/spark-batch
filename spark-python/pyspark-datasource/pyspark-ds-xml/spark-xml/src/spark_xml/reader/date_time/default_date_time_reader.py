"""Demonstrate reading XML with default (ISO-8601) date and timestamp formats.

spark-xml automatically parses yyyy-MM-dd dates and
yyyy-MM-dd'T'HH:mm:ss timestamps when no custom format options are set.
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

SAMPLE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <records>
      <record>
        <id>1</id>
        <created_at>2024-01-10T09:30:00</created_at>
        <birth_date>1990-03-15</birth_date>
      </record>
      <record>
        <id>2</id>
        <created_at>2024-02-14T14:45:30</created_at>
        <birth_date>1985-07-22</birth_date>
      </record>
      <record>
        <id>3</id>
        <created_at>2024-03-20T08:15:00</created_at>
        <birth_date>1992-11-08</birth_date>
      </record>
      <record>
        <id>4</id>
        <created_at>2024-04-05T17:00:00</created_at>
        <birth_date>1988-01-30</birth_date>
      </record>
      <record>
        <id>5</id>
        <created_at>2024-05-18T11:20:45</created_at>
        <birth_date>1995-12-03</birth_date>
      </record>
    </records>
""")


def generate_sample_xml(file_path: Path) -> None:
    """Write the ISO date/time sample XML to *file_path*."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(SAMPLE_XML, encoding="utf-8")
    print(f"Generated sample XML → {file_path}")


if __name__ == "__main__":
    data_path = (
        Path(os.environ["DATA_HOME"])
        / "file_data"
        / "xml"
        / "date_time"
        / "default_datetime.xml"
    )
    generate_sample_xml(data_path)

    spark = get_spark_session(
        app_name="spark-xml-default-datetime",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # Define schema with Date and Timestamp fields
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("birth_date", DateType(), True),
        ]
    )

    df = (
        spark.read.format("xml")
        .option("rowTag", "record")
        .schema(schema)
        .load(data_path.as_posix())
    )

    print("\n=== Default ISO Date/Timestamp Parsing ===")
    df.show(truncate=False)
    df.printSchema()

    spark.stop()
