"""Demonstrate reading XML with arrays of structs and exploding them.

Generates sample XML containing <arrayOfStructs> rows, each holding
a nested array of <struct> elements with field1/field2 pairs, then
explodes and concatenates the fields.
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql import functions as F

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

SAMPLE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <root>
      <arrayOfStructs id="row1">
        <struct>
          <field1>alpha</field1>
          <field2>100</field2>
        </struct>
        <struct>
          <field1>beta</field1>
          <field2>200</field2>
        </struct>
        <struct>
          <field1>gamma</field1>
          <field2>300</field2>
        </struct>
      </arrayOfStructs>
      <arrayOfStructs id="row2">
        <struct>
          <field1>delta</field1>
          <field2>400</field2>
        </struct>
        <struct>
          <field1>epsilon</field1>
          <field2>500</field2>
        </struct>
      </arrayOfStructs>
      <arrayOfStructs id="row3">
        <struct>
          <field1>zeta</field1>
          <field2>600</field2>
        </struct>
      </arrayOfStructs>
    </root>
""")


def generate_sample_xml(file_path: Path) -> None:
    """Write the array-of-structs sample XML to *file_path*."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(SAMPLE_XML, encoding="utf-8")
    print(f"Generated sample XML → {file_path}")


if __name__ == "__main__":
    data_path = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "data.xml"
    generate_sample_xml(data_path)

    spark = get_spark_session(
        app_name="spark-xml-array-of-structs",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    df = (
        spark.read.format("com.databricks.spark.xml")
        .option("rootTag", "root")
        .option("rowTag", "arrayOfStructs")
        .load(data_path.as_posix())
    )

    print("\n=== Raw Schema ===")
    df.printSchema()
    print(df.schema.json())

    print("\n=== Raw Data ===")
    df.show(truncate=False)

    # Explode the nested struct array into individual rows
    array_of_structs_df = df.select(
        F.col("_id").alias("row_id"),
        F.explode(F.col("struct")).alias("struct"),
    )

    print("\n=== Exploded Schema ===")
    array_of_structs_df.printSchema()

    print("\n=== Exploded Data ===")
    array_of_structs_df.show(truncate=False)

    # Concatenate struct fields into a single string column
    array_of_strings_df = array_of_structs_df.select(
        "row_id",
        F.concat_ws(",", F.col("struct.field1"), F.col("struct.field2")).alias(
            "concatenated"
        ),
    )

    print("\n=== Concatenated Fields ===")
    array_of_strings_df.show(truncate=False)

    spark.stop()
