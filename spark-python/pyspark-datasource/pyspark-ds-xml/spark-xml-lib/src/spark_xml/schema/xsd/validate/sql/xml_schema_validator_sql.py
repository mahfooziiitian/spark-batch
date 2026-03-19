"""Demonstrate XSD-validated XML reading via Spark SQL (valid data).

Creates a self-contained XML file of valid Order rows and a matching XSD,
then uses Spark SQL ``CREATE TABLE USING xml`` with ``rowValidationXSDPath``
to register the data. Shows schema inspection, SQL queries, temporary views,
and the difference between ``inferSchema true`` vs ``false`` with XSD.

See ``xml_schema_validator_sql_corrupt.py`` for error-handling modes
(PERMISSIVE / DROPMALFORMED / FAILFAST) with corrupt records.
"""

import os
import sys
import textwrap
from pathlib import Path

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

ORDERS_XSD = textwrap.dedent("""\
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
""")

ORDERS_XML = textwrap.dedent("""\
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
        <OrderDate>2024-04-22</OrderDate>
        <Amount>125.50</Amount>
        <Status>Pending</Status>
      </Order>
      <Order>
        <OrderID>ORD-003</OrderID>
        <CustomerName>Carol Lee</CustomerName>
        <OrderDate>2024-05-20</OrderDate>
        <Amount>480.75</Amount>
        <Status>Shipped</Status>
      </Order>
      <Order>
        <OrderID>ORD-004</OrderID>
        <CustomerName>David Chen</CustomerName>
        <OrderDate>2024-07-10</OrderDate>
        <Amount>310.25</Amount>
        <Status>Completed</Status>
      </Order>
      <Order>
        <OrderID>ORD-005</OrderID>
        <CustomerName>Eve Martin</CustomerName>
        <OrderDate>2024-08-01</OrderDate>
        <Amount>89.99</Amount>
        <Status>Cancelled</Status>
      </Order>
    </Orders>
""")


def generate_data_files(xml_path: Path, xsd_path: Path) -> None:
    """Write the sample XML and its XSD to disk."""
    for p in (xml_path, xsd_path):
        p.parent.mkdir(parents=True, exist_ok=True)

    xml_path.write_text(ORDERS_XML, encoding="utf-8")
    xsd_path.write_text(ORDERS_XSD, encoding="utf-8")
    print(f"Generated XML → {xml_path}")
    print(f"Generated XSD → {xsd_path}")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    spark_warehouse = os.environ.get(
        "SPARK_WAREHOUSE", str(Path(data_home) / "spark-warehouse")
    )

    xml_path = Path(data_home) / "file_data" / "xml" / "orders_valid_sql.xml"
    xsd_path = Path(data_home) / "file_data" / "xml" / "orders_valid_sql.xsd"

    generate_data_files(xml_path, xsd_path)

    spark = get_spark_session(
        app_name="spark-xml-xsd-sql-valid",
        scala_version="2.12",
        spark_xml_version="0.18.0",
        warehouse_dir=spark_warehouse,
    )

    # Distribute the XSD so every executor can access it by filename
    spark.sparkContext.addFile(xsd_path.as_posix())

    xml_uri = xml_path.as_posix()
    xsd_name = xsd_path.name

    # ------------------------------------------------------------------
    # 1. CREATE TABLE with XSD validation + inferSchema true
    # ------------------------------------------------------------------
    print("\n=== 1. CREATE TABLE — inferSchema true (default) ===")
    spark.sql("DROP TABLE IF EXISTS orders_infer")
    spark.sql(f"""
        CREATE TABLE orders_infer
        USING xml
        OPTIONS (
            path                  '{xml_uri}',
            rowTag                'Order',
            rowValidationXSDPath  '{xsd_name}'
        )
    """)
    spark.sql("DESCRIBE orders_infer").show(truncate=False)
    spark.sql("SELECT * FROM orders_infer").show(truncate=False)

    # ------------------------------------------------------------------
    # 2. CREATE TABLE with inferSchema false (all columns become STRING)
    # ------------------------------------------------------------------
    print("\n=== 2. CREATE TABLE — inferSchema false ===")
    spark.sql("DROP TABLE IF EXISTS orders_no_infer")
    spark.sql(f"""
        CREATE TABLE orders_no_infer
        USING xml
        OPTIONS (
            path                  '{xml_uri}',
            rowTag                'Order',
            rowValidationXSDPath  '{xsd_name}',
            inferSchema           'false'
        )
    """)
    spark.sql("DESCRIBE orders_no_infer").show(truncate=False)
    spark.sql("SELECT * FROM orders_no_infer").show(truncate=False)

    # ------------------------------------------------------------------
    # 3. SQL queries on validated data
    # ------------------------------------------------------------------
    print("\n=== 3. Filter — Completed orders ===")
    spark.sql("""
        SELECT OrderID, CustomerName, Amount
        FROM orders_infer
        WHERE Status = 'Completed'
        ORDER BY Amount DESC
    """).show(truncate=False)

    print("=== 4. Aggregation — totals by Status ===")
    spark.sql("""
        SELECT Status,
               count(*)       AS order_count,
               sum(Amount)    AS total_amount,
               avg(Amount)    AS avg_amount,
               min(OrderDate) AS earliest,
               max(OrderDate) AS latest
        FROM orders_infer
        GROUP BY Status
        ORDER BY total_amount DESC
    """).show(truncate=False)

    print("=== 5. Grand total ===")
    spark.sql("""
        SELECT count(*)    AS total_orders,
               sum(Amount) AS grand_total,
               avg(Amount) AS avg_order
        FROM orders_infer
    """).show(truncate=False)

    # ------------------------------------------------------------------
    # 4. Temporary view approach (no managed table on disk)
    # ------------------------------------------------------------------
    print("\n=== 6. Temporary View via DataFrame ===")
    df = (
        spark.read.format("xml")
        .option("rowTag", "Order")
        .option("rowValidationXSDPath", xsd_name)
        .load(xml_uri)
    )
    df.createOrReplaceTempView("orders_view")

    spark.sql("""
        SELECT OrderID,
               CustomerName,
               Amount,
               CASE
                 WHEN Amount >= 300 THEN 'HIGH'
                 WHEN Amount >= 100 THEN 'MEDIUM'
                 ELSE 'LOW'
               END AS tier
        FROM orders_view
        ORDER BY Amount DESC
    """).show(truncate=False)

    # ------------------------------------------------------------------
    # 5. Compare inferred schema vs no-infer schema
    # ------------------------------------------------------------------
    print("\n=== 7. Schema Comparison: inferSchema true vs false ===")
    infer_cols = {
        r["col_name"]: r["data_type"]
        for r in spark.sql("DESCRIBE orders_infer").collect()
    }
    no_infer_cols = {
        r["col_name"]: r["data_type"]
        for r in spark.sql("DESCRIBE orders_no_infer").collect()
    }
    print(f"{'Column':<20s} {'infer=true':<15s} {'infer=false':<15s}")
    print("-" * 50)
    for col_name in infer_cols:
        print(
            f"{col_name:<20s} {infer_cols[col_name]:<15s} "
            f"{no_infer_cols.get(col_name, 'N/A'):<15s}"
        )

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    for tbl in ("orders_infer", "orders_no_infer"):
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")

    spark.stop()
