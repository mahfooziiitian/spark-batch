"""Demonstrate XSD-validated XML reading via Spark SQL with corrupt records.

Creates a self-contained XML file containing both valid and intentionally
corrupt Order rows together with a matching XSD schema. Uses Spark SQL
``CREATE TABLE USING xml`` with ``rowValidationXSDPath`` to show how each
parse mode (PERMISSIVE, DROPMALFORMED, FAILFAST) handles schema violations.
Also demonstrates querying valid/corrupt rows via SQL and using temp views.
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql import SparkSession
from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

ORDERS_XSD = textwrap.dedent(
    """\
    <?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="Order">
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
"""
)

CORRUPT_XML = textwrap.dedent(
    """\
    <?xml version="1.0" encoding="UTF-8"?>
    <Orders>
      <Order>
        <OrderID>ORD-001</OrderID>
        <CustomerName>Alice Johnson</CustomerName>
        <OrderDate>2024-03-15</OrderDate>
        <Amount>250.00</Amount>
        <Status>Completed</Status>
      </Order>
      <Order>
        <OrderID>ORD-002</OrderID>
        <CustomerName>Bob Smith</CustomerName>
        <OrderDate>INVALID-DATE</OrderDate>
        <Amount>125.50</Amount>
        <Status>Pending</Status>
      </Order>
      <Order>
        <OrderID>ORD-003</OrderID>
        <CustomerName>Carol Lee</CustomerName>
        <OrderDate>2024-05-20</OrderDate>
        <Amount>NOT-A-NUMBER</Amount>
        <Status>Shipped</Status>
      </Order>
      <Order>
        <OrderID>ORD-004</OrderID>
        <CustomerName>David Chen</CustomerName>
        <OrderDate>2024-07-10</OrderDate>
        <Amount>480.00</Amount>
        <Status>Completed</Status>
      </Order>
      <Order>
        <OrderID>ORD-005</OrderID>
        <ExtraField>unexpected</ExtraField>
        <CustomerName>Eve Martin</CustomerName>
        <OrderDate>2024-08-01</OrderDate>
        <Amount>310.25</Amount>
        <Status>Cancelled</Status>
      </Order>
    </Orders>
"""
)


def generate_data_files(xml_path: Path, xsd_path: Path) -> None:
    """Write the corrupt XML and its XSD to disk."""
    for p in (xml_path, xsd_path):
        p.parent.mkdir(parents=True, exist_ok=True)

    xml_path.write_text(CORRUPT_XML, encoding="utf-8")
    xsd_path.write_text(ORDERS_XSD, encoding="utf-8")
    print(f"Generated corrupt XML → {xml_path}")
    print(f"Generated XSD         → {xsd_path}")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    spark_warehouse = os.environ.get(
        "SPARK_WAREHOUSE", str(Path(data_home) / "spark-warehouse")
    )

    xml_path = Path(data_home) / "file_data" / "xml" / "orders_corrupt_sql.xml"
    xsd_path = Path(data_home) / "file_data" / "xml" / "orders_corrupt_sql.xsd"

    generate_data_files(xml_path, xsd_path)

    spark = get_spark_session(
        app_name="spark-xml-xsd-sql-corrupt",
        scala_version="2.12",
        spark_xml_version="0.18.0",
        master="local[*]",
        warehouse_dir=spark_warehouse,
        log_level="WARN",
        enable_ui=False,
    )

    # Distribute the XSD so every executor can access it by filename
    spark.sparkContext.addFile(xsd_path.as_posix())

    xml_uri = xml_path.as_posix()
    xsd_name = xsd_path.name

    # ------------------------------------------------------------------
    # 1. PERMISSIVE mode — keeps all rows; corrupt ones have null fields
    # ------------------------------------------------------------------
    print("\n=== 1. CREATE TABLE — PERMISSIVE mode ===")
    spark.sql("DROP TABLE IF EXISTS orders_permissive")
    spark.sql(
        f"""
        CREATE TABLE orders_permissive
        USING xml
        OPTIONS (
            path         '{xml_uri}',
            rowTag        'Order',
            rowValidationXSDPath '{xsd_name}',
            mode          'PERMISSIVE'
        )
    """
    )
    spark.sql("SELECT * FROM orders_permissive").show(truncate=False)
    spark.sql("SELECT * FROM orders_permissive").printSchema()

    print("--- Rows where OrderDate is null (XSD violation) ---")
    spark.sql(
        """
        SELECT OrderID, CustomerName, Status
        FROM orders_permissive
        WHERE OrderDate IS NULL
    """
    ).show(truncate=False)

    print("--- Only valid rows ---")
    spark.sql(
        """
        SELECT OrderID, CustomerName, OrderDate, Amount, Status
        FROM orders_permissive
        WHERE OrderDate IS NOT NULL AND Amount IS NOT NULL
    """
    ).show(truncate=False)

    # ------------------------------------------------------------------
    # 2. DROPMALFORMED mode — silently drops rows that fail XSD
    # ------------------------------------------------------------------
    print("\n=== 2. CREATE TABLE — DROPMALFORMED mode ===")
    spark.sql("DROP TABLE IF EXISTS orders_drop")
    spark.sql(
        f"""
        CREATE TABLE orders_drop
        USING xml
        OPTIONS (
            path         '{xml_uri}',
            rowTag        'Order',
            rowValidationXSDPath '{xsd_name}',
            mode          'DROPMALFORMED'
        )
    """
    )
    spark.sql("SELECT * FROM orders_drop").show(truncate=False)

    row_count = spark.sql("SELECT count(*) AS cnt FROM orders_drop").first()["cnt"]
    print(f"Rows after DROPMALFORMED: {row_count} (corrupt rows silently removed)")

    # ------------------------------------------------------------------
    # 3. FAILFAST mode — raises an exception on the first corrupt row
    # ------------------------------------------------------------------
    print("\n=== 3. CREATE TABLE — FAILFAST mode ===")
    spark.sql("DROP TABLE IF EXISTS orders_fail")
    spark.sql(
        f"""
        CREATE TABLE orders_fail
        USING xml
        OPTIONS (
            path         '{xml_uri}',
            rowTag        'Order',
            rowValidationXSDPath '{xsd_name}',
            mode          'FAILFAST'
        )
    """
    )
    try:
        spark.sql("SELECT * FROM orders_fail").show(truncate=False)
    except Exception as e:
        print(
            f"FAILFAST raised expected error:\n"
            f"  {e.__class__.__name__}: {str(e)[:200]}"
        )

    # ------------------------------------------------------------------
    # 4. Temp view approach (no managed table)
    # ------------------------------------------------------------------
    print("\n=== 4. Temp View — PERMISSIVE via DataFrame ===")
    df_permissive = (
        spark.read.format("xml")
        .option("rowTag", "Order")
        .option("rowValidationXSDPath", xsd_name)
        .option("mode", "PERMISSIVE")
        .load(xml_uri)
    )
    df_permissive.createOrReplaceTempView("orders_view")

    spark.sql(
        """
        SELECT OrderID, CustomerName, Status,
               CASE
                 WHEN OrderDate IS NULL OR Amount IS NULL
                 THEN 'INVALID'
                 ELSE 'VALID'
               END AS validation_status
        FROM orders_view
    """
    ).show(truncate=False)

    # ------------------------------------------------------------------
    # 5. Aggregate stats on corrupt vs valid
    # ------------------------------------------------------------------
    print("=== 5. Validation Summary ===")
    spark.sql(
        """
        SELECT
            count(*)                                                  AS total_rows,
            count(CASE WHEN OrderDate IS NOT NULL
                         AND Amount IS NOT NULL THEN 1 END)           AS valid_rows,
            count(CASE WHEN OrderDate IS NULL
                         OR  Amount IS NULL THEN 1 END)               AS invalid_rows
        FROM orders_view
    """
    ).show(truncate=False)

    # ------------------------------------------------------------------
    # Cleanup managed tables
    # ------------------------------------------------------------------
    for tbl in ("orders_permissive", "orders_drop", "orders_fail"):
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")

    spark.stop()
