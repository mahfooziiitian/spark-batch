"""Demonstrate reading XML with a schema loaded from a JSON file.

Generates a sample XML file and a corresponding Spark StructType JSON
schema file, then reads the XML using that external schema definition.
"""

import json
import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

SAMPLE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <notes>
      <note id="1">
        <to>Alice</to>
        <from>Bob</from>
        <heading>Reminder</heading>
        <body>Don't forget the meeting at 10am.</body>
        <priority>1</priority>
        <score>9.5</score>
      </note>
      <note id="2">
        <to>Carol</to>
        <from>David</from>
        <heading>Lunch</heading>
        <body>Are you free for lunch tomorrow?</body>
        <priority>2</priority>
        <score>7.0</score>
      </note>
      <note id="3">
        <to>Eve</to>
        <from>Alice</from>
        <heading>Project Update</heading>
        <body>The sprint review is scheduled for Friday.</body>
        <priority>1</priority>
        <score>8.8</score>
      </note>
      <note id="4">
        <to>Bob</to>
        <from>Carol</from>
        <heading>Feedback</heading>
        <body>Great job on the presentation!</body>
        <priority>3</priority>
        <score>6.5</score>
      </note>
      <note id="5">
        <to>David</to>
        <from>Eve</from>
        <heading>Deadline</heading>
        <body>Please submit the report by end of day.</body>
        <priority>1</priority>
        <score>9.0</score>
      </note>
    </notes>
""")

NOTES_SCHEMA = StructType(
    [
        StructField("to", StringType(), True),
        StructField("from", StringType(), True),
        StructField("heading", StringType(), True),
        StructField("body", StringType(), True),
        StructField("priority", LongType(), True),
        StructField("score", DoubleType(), True),
    ]
)


def generate_data_files(xml_path: Path, schema_path: Path) -> None:
    """Write the sample XML and its JSON schema to disk."""
    xml_path.parent.mkdir(parents=True, exist_ok=True)
    xml_path.write_text(SAMPLE_XML, encoding="utf-8")
    print(f"Generated sample XML  → {xml_path}")

    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(
        json.dumps(NOTES_SCHEMA.jsonValue(), indent=2), encoding="utf-8"
    )
    print(f"Generated JSON schema → {schema_path}")


if __name__ == "__main__":
    data_home = os.environ["DATA_HOME"]
    xml_path = Path(data_home) / "file_data" / "xml" / "notes.xml"
    schema_path = Path(data_home) / "file_data" / "xml" / "schema" / "notes_schema.json"

    generate_data_files(xml_path, schema_path)

    spark = get_spark_session(
        app_name="spark-xml-json-schema",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    with open(schema_path) as f:
        d = json.load(f)
        schema = StructType.fromJson(d)

    print(f"\n=== Loaded Schema ===\n{schema.simpleString()}")

    json_df = (
        spark.read.format("xml")
        .option("rowTag", "note")
        .option("excludeAttribute", "true")
        .option("ignoreNamespace", "true")
        .schema(schema)
        .load(xml_path.as_posix())
    )

    print("\n=== Data ===")
    json_df.show(truncate=False)
    json_df.printSchema()

    spark.stop()
