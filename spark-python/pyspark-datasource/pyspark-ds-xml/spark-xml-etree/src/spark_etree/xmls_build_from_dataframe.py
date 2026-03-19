"""Build XML strings from DataFrame rows using ElementTree and collect back."""

import os
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

SAMPLE_DATA = [
    {"emp_id": 101, "name": "Alice", "dept": "Engineering", "salary": 95000},
    {"emp_id": 102, "name": "Bob", "dept": "Marketing", "salary": 72000},
    {"emp_id": 103, "name": "Charlie", "dept": "Engineering", "salary": 110000},
    {"emp_id": 104, "name": "Diana", "dept": "Sales", "salary": 68000},
    {"emp_id": 105, "name": "Eve", "dept": "Engineering", "salary": 102000},
]


def row_to_xml(emp_id: int, name: str, dept: str, salary: int) -> str | None:
    """Serialize a single employee row into an XML element string."""
    if name is None:
        return None

    emp = ET.Element("employee", id=str(emp_id))
    ET.SubElement(emp, "name").text = name
    ET.SubElement(emp, "department").text = dept
    ET.SubElement(emp, "salary").text = str(salary)
    return ET.tostring(emp, encoding="unicode")


def wrap_in_root(xml_fragments: list[str], root_tag: str = "employees") -> str:
    """Combine XML fragment strings under a single root element."""
    root = ET.Element(root_tag)
    for fragment in xml_fragments:
        root.append(ET.fromstring(fragment))

    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("xml-etree-build-from-df")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(SAMPLE_DATA)

    print("=== Source DataFrame ===")
    df.show(truncate=False)

    # Convert each row to an XML string via UDF
    row_to_xml_udf = udf(
        lambda eid, n, d, s: row_to_xml(eid, n, d, s),
        StringType(),
    )

    xml_df = df.withColumn(
        "xml",
        row_to_xml_udf(F.col("emp_id"), F.col("name"), F.col("dept"), F.col("salary")),
    )

    print("=== DataFrame with XML column ===")
    xml_df.select("emp_id", "xml").show(truncate=False)

    # Filter and generate XML for engineering department only
    eng_xml_df = xml_df.filter(F.col("dept") == "Engineering")

    print("=== Engineering dept XML column ===")
    eng_xml_df.select("emp_id", "xml").show(truncate=False)

    # Collect fragments and assemble a full XML document
    fragments = [row["xml"] for row in eng_xml_df.select("xml").collect()]
    full_xml = wrap_in_root(fragments)

    print("=== Assembled XML document ===")
    print(full_xml)

    # Round-trip: parse the assembled document back into a DataFrame
    root = ET.fromstring(full_xml)
    round_trip_rows = []
    for emp in root.findall("employee"):
        round_trip_rows.append(
            {
                "emp_id": int(emp.attrib["id"]),
                "name": emp.findtext("name"),
                "department": emp.findtext("department"),
                "salary": int(emp.findtext("salary", default="0")),
            }
        )

    round_trip_df = spark.createDataFrame(round_trip_rows)

    print("=== Round-trip DataFrame ===")
    round_trip_df.show(truncate=False)

    spark.stop()
