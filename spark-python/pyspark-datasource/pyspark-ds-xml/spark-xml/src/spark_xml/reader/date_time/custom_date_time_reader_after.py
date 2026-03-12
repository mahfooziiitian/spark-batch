"""Demonstrate reading XML with custom date/time formats and converting
them using to_date / to_timestamp after loading as strings.

Generates sample XML with non-ISO date and timestamp values, reads them
as strings, then converts to proper Date and Timestamp types.
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql.functions import to_date, to_timestamp

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

SAMPLE_XML = textwrap.dedent(
    """\
    <?xml version="1.0" encoding="UTF-8"?>
    <records>
      <record>
        <id>1</id>
        <name>Alice Johnson</name>
        <birth_date>15-03-1990</birth_date>
        <created_at>01/10/2024 09:30</created_at>
        <status>active</status>
      </record>
      <record>
        <id>2</id>
        <name>Bob Smith</name>
        <birth_date>22-07-1985</birth_date>
        <created_at>02/14/2024 14:45</created_at>
        <status>active</status>
      </record>
      <record>
        <id>3</id>
        <name>Carol Lee</name>
        <birth_date>08-11-1992</birth_date>
        <created_at>03/20/2024 08:15</created_at>
        <status>inactive</status>
      </record>
      <record>
        <id>4</id>
        <name>David Chen</name>
        <birth_date>30-01-1988</birth_date>
        <created_at>04/05/2024 17:00</created_at>
        <status>active</status>
      </record>
      <record>
        <id>5</id>
        <name>Eve Martin</name>
        <birth_date>03-12-1995</birth_date>
        <created_at>05/18/2024 11:20</created_at>
        <status>pending</status>
      </record>
    </records>
"""
)


def generate_sample_xml(file_path: Path) -> None:
    """Write the date/time sample XML to *file_path*."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(SAMPLE_XML, encoding="utf-8")
    print(f"Generated sample XML → {file_path}")


if __name__ == "__main__":
    data_home = os.environ["DATA_HOME"]
    data_path = Path(data_home) / "file_data" / "xml" / "date_time" / "sample_data.xml"
    generate_sample_xml(data_path)

    spark = get_spark_session(
        app_name="spark-xml-custom-datetime-after",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    df_raw = (
        spark.read.format("xml").option("rowTag", "record").load(data_path.as_posix())
    )

    print("\n=== Raw (strings) ===")
    df_raw.show(truncate=False)
    df_raw.printSchema()

    # Convert string fields into proper Date and Timestamp
    df = df_raw.withColumn(
        "birth_date", to_date("birth_date", "dd-MM-yyyy")
    ).withColumn("created_at", to_timestamp("created_at", "MM/dd/yyyy HH:mm"))

    print("\n=== Converted (Date / Timestamp) ===")
    df.show(truncate=False)
    df.printSchema()

    spark.stop()
