"""Demonstrate spark-xml rowValidationXSDPath with corrupt records.

Generates a sample XML file containing both valid and intentionally
corrupt Order elements, plus a matching XSD. Reads with FAILFAST mode
to show XSD-based row validation catching schema violations.
"""

import os
import sys
import textwrap
from pathlib import Path

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

# XSD that defines a valid <Root> element structure
ORDERS_XSD = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="Root">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="OrderID" type="xs:string"/>
            <xs:element name="CustomerName" type="xs:string"/>
            <xs:element name="OrderDate" type="xs:date"/>
            <xs:element name="Amount" type="xs:decimal"/>
            <xs:element name="Status" type="xs:string"/>
          </xs:sequence>
        </xs:complexType>
      </xs:element>
    </xs:schema>
""")

# XML with a mix of valid and corrupt rows
CORRUPT_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <Orders>
      <Root>
        <OrderID>ORD-001</OrderID>
        <CustomerName>Alice Johnson</CustomerName>
        <OrderDate>2024-03-15</OrderDate>
        <Amount>250.00</Amount>
        <Status>Completed</Status>
      </Root>
      <Root>
        <OrderID>ORD-002</OrderID>
        <CustomerName>Bob Smith</CustomerName>
        <OrderDate>INVALID-DATE</OrderDate>
        <Amount>125.50</Amount>
        <Status>Pending</Status>
      </Root>
      <Root>
        <OrderID>ORD-003</OrderID>
        <CustomerName>Carol Lee</CustomerName>
        <OrderDate>2024-05-20</OrderDate>
        <Amount>NOT-A-NUMBER</Amount>
        <Status>Shipped</Status>
      </Root>
      <Root>
        <OrderID>ORD-004</OrderID>
        <CustomerName>David Chen</CustomerName>
        <OrderDate>2024-07-10</OrderDate>
        <Amount>480.00</Amount>
        <Status>Completed</Status>
      </Root>
      <Root>
        <OrderID>ORD-005</OrderID>
        <ExtraField>unexpected</ExtraField>
        <CustomerName>Eve Martin</CustomerName>
        <OrderDate>2024-08-01</OrderDate>
        <Amount>310.25</Amount>
        <Status>Cancelled</Status>
      </Root>
    </Orders>
""")


def generate_data_files(xml_path: Path, xsd_path: Path) -> None:
    """Write the corrupt XML and its XSD to disk."""
    xml_path.parent.mkdir(parents=True, exist_ok=True)
    xml_path.write_text(CORRUPT_XML, encoding="utf-8")
    print(f"Generated corrupt XML → {xml_path}")

    xsd_path.parent.mkdir(parents=True, exist_ok=True)
    xsd_path.write_text(ORDERS_XSD, encoding="utf-8")
    print(f"Generated XSD         → {xsd_path}")


if __name__ == "__main__":
    data_home = os.environ["DATA_HOME"]
    xml_path = Path(data_home) / "file_data" / "xml" / "orders_corrupt.xml"
    xsd_path = Path(data_home) / "file_data" / "xml" / "orders_corrupt.xsd"

    generate_data_files(xml_path, xsd_path)

    spark = get_spark_session(
        app_name="spark-xml-xsd-validation-corrupt",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # Distribute XSD to executors
    spark.sparkContext.addFile(xsd_path.as_posix())

    # --- PERMISSIVE first to see which rows are corrupt ---
    print("\n=== PERMISSIVE mode (shows corrupt records) ===")
    orders_permissive = (
        spark.read.format("com.databricks.spark.xml")
        .option("rowTag", "Root")
        .option("mode", "PERMISSIVE")
        .option("rowValidationXSDPath", xsd_path.name)
        .load(xml_path.as_posix())
    )
    orders_permissive.show(truncate=False)
    orders_permissive.printSchema()

    # --- DROPMALFORMED to skip corrupt rows silently ---
    print("\n=== DROPMALFORMED mode (drops invalid rows) ===")
    orders_drop = (
        spark.read.format("com.databricks.spark.xml")
        .option("rowTag", "Root")
        .option("mode", "DROPMALFORMED")
        .option("rowValidationXSDPath", xsd_path.name)
        .load(xml_path.as_posix())
    )
    orders_drop.show(truncate=False)

    # --- FAILFAST would throw on corrupt rows ---
    print("\n=== FAILFAST mode (will throw on first invalid row) ===")
    try:
        orders_fail = (
            spark.read.format("com.databricks.spark.xml")
            .option("rowTag", "Root")
            .option("mode", "FAILFAST")
            .option("rowValidationXSDPath", xsd_path.name)
            .load(xml_path.as_posix())
        )
        orders_fail.show(truncate=False)
    except Exception as e:
        print(f"FAILFAST caught expected error:\n  {e.__class__.__name__}: {str(e)[:200]}")

    spark.stop()
