"""Demonstrate reading complex XML with an explicit StructType + XSD validation.

Generates a self-contained Customers-and-Orders XML file (nested arrays,
structs, attributes) together with a matching XSD. Reads the data using
both an explicit StructType schema and XSD-based row validation, then
queries the nested structures with explode and dot notation.
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

# ── Inline test data ────────────────────────────────────────────────────

ORDERS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <Root>
      <Customers>
        <Customer CustomerID="ALFKI">
          <CompanyName>Alfreds Futterkiste</CompanyName>
          <ContactName>Maria Anders</ContactName>
          <ContactTitle>Sales Representative</ContactTitle>
          <Phone>030-0074321</Phone>
          <Fax>030-0076545</Fax>
          <FullAddress>
            <Address>Obere Str. 57</Address>
            <City>Berlin</City>
            <PostalCode>12209</PostalCode>
            <Country>Germany</Country>
          </FullAddress>
        </Customer>
        <Customer CustomerID="BERGS">
          <CompanyName>Berglunds snabbköp</CompanyName>
          <ContactName>Christina Berglund</ContactName>
          <ContactTitle>Order Administrator</ContactTitle>
          <Phone>0921-12 34 65</Phone>
          <FullAddress>
            <Address>Berguvsvägen  8</Address>
            <City>Luleå</City>
            <Region>Northern</Region>
            <PostalCode>17100</PostalCode>
            <Country>Sweden</Country>
          </FullAddress>
        </Customer>
      </Customers>
      <Orders>
        <Order>
          <CustomerID>ALFKI</CustomerID>
          <EmployeeID>4</EmployeeID>
          <OrderDate>1997-08-25T00:00:00</OrderDate>
          <RequiredDate>1997-09-22T00:00:00</RequiredDate>
          <ShipInfo ShippedDate="1997-09-02T00:00:00">
            <ShipVia>2</ShipVia>
            <Freight>29.46</Freight>
            <ShipName>Alfreds Futterkiste</ShipName>
            <ShipAddress>Obere Str. 57</ShipAddress>
            <ShipCity>Berlin</ShipCity>
            <ShipPostalCode>12209</ShipPostalCode>
            <ShipCountry>Germany</ShipCountry>
          </ShipInfo>
        </Order>
        <Order>
          <CustomerID>ALFKI</CustomerID>
          <EmployeeID>1</EmployeeID>
          <OrderDate>1997-10-03T00:00:00</OrderDate>
          <RequiredDate>1997-10-31T00:00:00</RequiredDate>
          <ShipInfo ShippedDate="1997-10-13T00:00:00">
            <ShipVia>1</ShipVia>
            <Freight>61.02</Freight>
            <ShipName>Alfred's Futterkiste</ShipName>
            <ShipAddress>Obere Str. 57</ShipAddress>
            <ShipCity>Berlin</ShipCity>
            <ShipPostalCode>12209</ShipPostalCode>
            <ShipCountry>Germany</ShipCountry>
          </ShipInfo>
        </Order>
        <Order>
          <CustomerID>BERGS</CustomerID>
          <EmployeeID>3</EmployeeID>
          <OrderDate>1997-11-18T00:00:00</OrderDate>
          <RequiredDate>1997-12-16T00:00:00</RequiredDate>
          <ShipInfo ShippedDate="1997-11-25T00:00:00">
            <ShipVia>1</ShipVia>
            <Freight>151.52</Freight>
            <ShipName>Berglunds snabbköp</ShipName>
            <ShipAddress>Berguvsvägen  8</ShipAddress>
            <ShipCity>Luleå</ShipCity>
            <ShipRegion>Northern</ShipRegion>
            <ShipPostalCode>17100</ShipPostalCode>
            <ShipCountry>Sweden</ShipCountry>
          </ShipInfo>
        </Order>
      </Orders>
    </Root>
""")

ORDERS_XSD = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="Root">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="Customers">
              <xs:complexType>
                <xs:sequence>
                  <xs:element name="Customer" maxOccurs="unbounded">
                    <xs:complexType>
                      <xs:sequence>
                        <xs:element name="CompanyName" type="xs:string"/>
                        <xs:element name="ContactName" type="xs:string"/>
                        <xs:element name="ContactTitle" type="xs:string"/>
                        <xs:element name="Phone" type="xs:string"/>
                        <xs:element name="Fax" type="xs:string" minOccurs="0"/>
                        <xs:element name="FullAddress">
                          <xs:complexType>
                            <xs:sequence>
                              <xs:element name="Address" type="xs:string"/>
                              <xs:element name="City" type="xs:string"/>
                              <xs:element name="Region" type="xs:string" minOccurs="0"/>
                              <xs:element name="PostalCode" type="xs:integer"/>
                              <xs:element name="Country" type="xs:string"/>
                            </xs:sequence>
                          </xs:complexType>
                        </xs:element>
                      </xs:sequence>
                      <xs:attribute name="CustomerID" type="xs:string" use="required"/>
                    </xs:complexType>
                  </xs:element>
                </xs:sequence>
              </xs:complexType>
            </xs:element>
            <xs:element name="Orders">
              <xs:complexType>
                <xs:sequence>
                  <xs:element name="Order" maxOccurs="unbounded">
                    <xs:complexType>
                      <xs:sequence>
                        <xs:element name="CustomerID" type="xs:string"/>
                        <xs:element name="EmployeeID" type="xs:integer"/>
                        <xs:element name="OrderDate" type="xs:dateTime"/>
                        <xs:element name="RequiredDate" type="xs:dateTime"/>
                        <xs:element name="ShipInfo">
                          <xs:complexType>
                            <xs:sequence>
                              <xs:element name="ShipVia" type="xs:integer"/>
                              <xs:element name="Freight" type="xs:decimal"/>
                              <xs:element name="ShipName" type="xs:string"/>
                              <xs:element name="ShipAddress" type="xs:string"/>
                              <xs:element name="ShipCity" type="xs:string"/>
                              <xs:element name="ShipRegion" type="xs:string" minOccurs="0"/>
                              <xs:element name="ShipPostalCode" type="xs:integer"/>
                              <xs:element name="ShipCountry" type="xs:string"/>
                            </xs:sequence>
                            <xs:attribute name="ShippedDate" type="xs:dateTime"/>
                          </xs:complexType>
                        </xs:element>
                      </xs:sequence>
                    </xs:complexType>
                  </xs:element>
                </xs:sequence>
              </xs:complexType>
            </xs:element>
          </xs:sequence>
        </xs:complexType>
      </xs:element>
    </xs:schema>
""")

# ── Explicit StructType (mirrors the XSD) ───────────────────────────────

ADDRESS_SCHEMA = StructType([
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("PostalCode", LongType(), True),
    StructField("Region", StringType(), True),
])

CUSTOMER_SCHEMA = StructType([
    StructField("CompanyName", StringType(), True),
    StructField("ContactName", StringType(), True),
    StructField("ContactTitle", StringType(), True),
    StructField("Fax", StringType(), True),
    StructField("FullAddress", ADDRESS_SCHEMA, True),
    StructField("Phone", StringType(), True),
    StructField("_CustomerID", StringType(), True),
])

SHIP_INFO_SCHEMA = StructType([
    StructField("Freight", DoubleType(), True),
    StructField("ShipAddress", StringType(), True),
    StructField("ShipCity", StringType(), True),
    StructField("ShipCountry", StringType(), True),
    StructField("ShipName", StringType(), True),
    StructField("ShipPostalCode", LongType(), True),
    StructField("ShipRegion", StringType(), True),
    StructField("ShipVia", LongType(), True),
    StructField("_ShippedDate", TimestampType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("CustomerID", StringType(), True),
    StructField("EmployeeID", LongType(), True),
    StructField("OrderDate", TimestampType(), True),
    StructField("RequiredDate", TimestampType(), True),
    StructField("ShipInfo", SHIP_INFO_SCHEMA, True),
])

ROOT_SCHEMA = StructType([
    StructField("Customers", StructType([
        StructField("Customer", ArrayType(CUSTOMER_SCHEMA), True),
    ]), True),
    StructField("Orders", StructType([
        StructField("Order", ArrayType(ORDER_SCHEMA), True),
    ]), True),
])


def generate_data_files(xml_path: Path, xsd_path: Path) -> None:
    """Write sample XML and matching XSD to disk."""
    for p in (xml_path, xsd_path):
        p.parent.mkdir(parents=True, exist_ok=True)

    xml_path.write_text(ORDERS_XML, encoding="utf-8")
    xsd_path.write_text(ORDERS_XSD, encoding="utf-8")
    print(f"Generated XML → {xml_path}")
    print(f"Generated XSD → {xsd_path}")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    xml_path = Path(data_home) / "file_data" / "xml" / "orders_schema.xml"
    xsd_path = Path(data_home) / "file_data" / "xml" / "orders_schema.xsd"

    generate_data_files(xml_path, xsd_path)

    spark = get_spark_session(
        app_name="spark-xml-explicit-schema-xsd",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )
    spark.sparkContext.addFile(xsd_path.as_posix())

    # ------------------------------------------------------------------
    # 1. Read with explicit StructType schema
    # ------------------------------------------------------------------
    print("\n=== 1. Explicit StructType Schema ===")
    df = (
        spark.read.format("xml")
        .option("rowTag", "Root")
        .schema(ROOT_SCHEMA)
        .load(xml_path.as_posix())
    )
    df.printSchema()
    df.show(truncate=False)

    # ------------------------------------------------------------------
    # 2. Read with XSD validation (inferred schema)
    # ------------------------------------------------------------------
    print("\n=== 2. XSD Validation (inferred schema) ===")
    df_xsd = (
        spark.read.format("xml")
        .option("rowTag", "Root")
        .option("rowValidationXSDPath", xsd_path.name)
        .load(xml_path.as_posix())
    )
    df_xsd.printSchema()

    # ------------------------------------------------------------------
    # 3. Read with explicit schema + XSD validation combined
    # ------------------------------------------------------------------
    print("\n=== 3. Explicit Schema + XSD Validation ===")
    df_both = (
        spark.read.format("xml")
        .option("rowTag", "Root")
        .option("rowValidationXSDPath", xsd_path.name)
        .schema(ROOT_SCHEMA)
        .load(xml_path.as_posix())
    )
    df_both.printSchema()
    df_both.show(truncate=False)

    # ------------------------------------------------------------------
    # 4. Explode and query customers
    # ------------------------------------------------------------------
    print("\n=== 4. Customers (exploded) ===")
    df_customers = (
        df.select(F.explode("Customers.Customer").alias("c"))
        .select(
            F.col("c._CustomerID").alias("customer_id"),
            F.col("c.CompanyName").alias("company"),
            F.col("c.ContactName").alias("contact"),
            F.col("c.FullAddress.City").alias("city"),
            F.col("c.FullAddress.Country").alias("country"),
        )
    )
    df_customers.show(truncate=False)

    # ------------------------------------------------------------------
    # 5. Explode and query orders with shipping details
    # ------------------------------------------------------------------
    print("=== 5. Orders with Shipping (exploded) ===")
    df_orders = (
        df.select(F.explode("Orders.Order").alias("o"))
        .select(
            F.col("o.CustomerID").alias("customer_id"),
            F.col("o.EmployeeID").alias("employee_id"),
            F.col("o.OrderDate").alias("order_date"),
            F.col("o.ShipInfo.Freight").alias("freight"),
            F.col("o.ShipInfo.ShipCity").alias("ship_city"),
            F.col("o.ShipInfo.ShipCountry").alias("ship_country"),
            F.col("o.ShipInfo._ShippedDate").alias("shipped_date"),
        )
    )
    df_orders.show(truncate=False)

    # ------------------------------------------------------------------
    # 6. Join customers and orders
    # ------------------------------------------------------------------
    print("=== 6. Customers ⟕ Orders ===")
    df_joined = df_customers.join(df_orders, on="customer_id", how="inner")
    df_joined.select(
        "customer_id", "company", "order_date", "freight", "ship_city",
    ).show(truncate=False)

    # ------------------------------------------------------------------
    # 7. Freight aggregation per customer
    # ------------------------------------------------------------------
    print("=== 7. Freight Summary per Customer ===")
    df_orders.groupBy("customer_id").agg(
        F.count("*").alias("order_count"),
        F.round(F.sum("freight"), 2).alias("total_freight"),
        F.round(F.avg("freight"), 2).alias("avg_freight"),
    ).show(truncate=False)

    # ------------------------------------------------------------------
    # 8. Compare explicit vs inferred schema
    # ------------------------------------------------------------------
    print("=== 8. Schema Comparison: explicit vs inferred ===")
    def _flat_fields(schema, prefix=""):
        for f in schema.fields:
            path = f"{prefix}.{f.name}" if prefix else f.name
            if isinstance(f.dataType, StructType):
                yield from _flat_fields(f.dataType, path)
            elif isinstance(f.dataType, ArrayType) and isinstance(
                f.dataType.elementType, StructType
            ):
                yield from _flat_fields(f.dataType.elementType, f"{path}[]")
            else:
                yield path, f.dataType.simpleString()

    explicit_fields = dict(_flat_fields(ROOT_SCHEMA))
    inferred_fields = dict(_flat_fields(df_xsd.schema))

    all_names = sorted(set(explicit_fields) | set(inferred_fields))
    print(f"{'Field':<45s} {'Explicit':<12s} {'Inferred':<12s} Match")
    print("-" * 80)
    for name in all_names:
        e = explicit_fields.get(name, "—")
        i = inferred_fields.get(name, "—")
        marker = "✓" if e == i else "≠"
        print(f"{name:<45s} {e:<12s} {i:<12s} {marker}")

    spark.stop()
