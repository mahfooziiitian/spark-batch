"""Comprehensive tests for all schema handling patterns in spark-xml."""

import json

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_xml(tmp_path, filename, content):
    """Write XML content to a file and return the path string."""
    p = tmp_path / filename
    p.write_text(content, encoding="utf-8")
    return str(p)


# ---------------------------------------------------------------------------
# 1. Schema Inference
# ---------------------------------------------------------------------------

class TestSchemaInference:
    """Tests for automatic schema inference."""

    PRODUCTS_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<catalog>
  <product>
    <id>1</id>
    <name>Laptop</name>
    <price>999.99</price>
    <quantity>50</quantity>
    <available>true</available>
  </product>
  <product>
    <id>2</id>
    <name>Mouse</name>
    <price>29.99</price>
    <quantity>500</quantity>
    <available>false</available>
  </product>
</catalog>"""

    def test_infer_schema_column_names(self, spark, tmp_path):
        path = _write_xml(tmp_path, "products.xml", self.PRODUCTS_XML)
        df = spark.read.format("xml").option("rowTag", "product").load(path)
        assert set(df.columns) == {"id", "name", "price", "quantity", "available"}

    def test_infer_schema_row_count(self, spark, tmp_path):
        path = _write_xml(tmp_path, "products.xml", self.PRODUCTS_XML)
        df = spark.read.format("xml").option("rowTag", "product").load(path)
        assert df.count() == 2

    def test_infer_schema_numeric_types(self, spark, tmp_path):
        path = _write_xml(tmp_path, "products.xml", self.PRODUCTS_XML)
        df = spark.read.format("xml").option("rowTag", "product").load(path)
        type_map = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(type_map["id"], LongType)
        assert isinstance(type_map["price"], DoubleType)
        assert isinstance(type_map["quantity"], LongType)

    def test_mixed_type_infers_string(self, spark, tmp_path):
        xml = """\
<records>
  <record><value>42</value></record>
  <record><value>hello</value></record>
</records>"""
        path = _write_xml(tmp_path, "mixed.xml", xml)
        df = spark.read.format("xml").option("rowTag", "record").load(path)
        assert isinstance(df.schema["value"].dataType, StringType)

    def test_exclude_attribute_inference(self, spark, tmp_path):
        xml = """\
<items>
  <item id="1"><name>A</name></item>
  <item id="2"><name>B</name></item>
</items>"""
        path = _write_xml(tmp_path, "attrs.xml", xml)
        df = (
            spark.read.format("xml")
            .option("rowTag", "item")
            .option("excludeAttribute", "true")
            .load(path)
        )
        assert "_id" not in df.columns
        assert "name" in df.columns


# ---------------------------------------------------------------------------
# 2. Explicit StructType Schema
# ---------------------------------------------------------------------------

class TestExplicitSchema:
    """Tests for explicit StructType schema definition."""

    EMPLOYEES_XML = """\
<company>
  <employee>
    <id>1</id>
    <name>Alice</name>
    <salary>95000.50</salary>
    <age>32</age>
    <active>true</active>
    <hire_date>2020-03-15</hire_date>
    <last_login>2024-11-20T09:30:00</last_login>
    <rating>4.7</rating>
  </employee>
  <employee>
    <id>2</id>
    <name>Bob</name>
    <salary>72000.00</salary>
    <age>28</age>
    <active>true</active>
    <hire_date>2022-07-01</hire_date>
    <last_login>2024-11-19T14:15:00</last_login>
    <rating>4.2</rating>
  </employee>
</company>"""

    SCHEMA = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("age", IntegerType(), True),
        StructField("active", BooleanType(), True),
        StructField("hire_date", DateType(), True),
        StructField("last_login", TimestampType(), True),
        StructField("rating", FloatType(), True),
    ])

    def test_explicit_schema_types(self, spark, tmp_path):
        path = _write_xml(tmp_path, "emp.xml", self.EMPLOYEES_XML)
        df = (
            spark.read.format("xml")
            .option("rowTag", "employee")
            .schema(self.SCHEMA)
            .load(path)
        )
        assert df.schema == self.SCHEMA

    def test_explicit_schema_values(self, spark, tmp_path):
        path = _write_xml(tmp_path, "emp.xml", self.EMPLOYEES_XML)
        df = (
            spark.read.format("xml")
            .option("rowTag", "employee")
            .schema(self.SCHEMA)
            .load(path)
        )
        row = df.filter(F.col("id") == 1).first()
        assert row["name"] == "Alice"
        assert row["salary"] == pytest.approx(95000.50)
        assert row["active"] is True

    def test_type_coercion_to_string(self, spark, tmp_path):
        path = _write_xml(tmp_path, "emp.xml", self.EMPLOYEES_XML)
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("salary", StringType(), True),
        ])
        df = (
            spark.read.format("xml")
            .option("rowTag", "employee")
            .schema(schema)
            .load(path)
        )
        row = df.first()
        assert isinstance(row["salary"], str)


# ---------------------------------------------------------------------------
# 3. DDL String Schema
# ---------------------------------------------------------------------------

class TestDDLSchema:
    """Tests for DDL string schema definition."""

    BOOKS_XML = """\
<library>
  <book>
    <title>Effective Java</title>
    <author>Joshua Bloch</author>
    <year>2018</year>
    <price>45.00</price>
    <in_stock>true</in_stock>
  </book>
  <book>
    <title>Clean Code</title>
    <author>Robert C. Martin</author>
    <year>2008</year>
    <price>37.50</price>
    <in_stock>true</in_stock>
  </book>
</library>"""

    def test_flat_ddl_schema(self, spark, tmp_path):
        path = _write_xml(tmp_path, "books.xml", self.BOOKS_XML)
        ddl = "title STRING, author STRING, year INT, price DOUBLE, in_stock BOOLEAN"
        df = (
            spark.read.format("xml")
            .option("rowTag", "book")
            .schema(ddl)
            .load(path)
        )
        assert df.count() == 2
        type_map = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(type_map["year"], IntegerType)
        assert isinstance(type_map["price"], DoubleType)
        assert isinstance(type_map["in_stock"], BooleanType)

    def test_nested_ddl_schema(self, spark, tmp_path):
        xml = """\
<orders>
  <order>
    <id>1</id>
    <shipping><city>Seattle</city><zip>98101</zip></shipping>
  </order>
</orders>"""
        path = _write_xml(tmp_path, "orders.xml", xml)
        ddl = "id LONG, shipping STRUCT<city: STRING, zip: STRING>"
        df = (
            spark.read.format("xml")
            .option("rowTag", "order")
            .schema(ddl)
            .load(path)
        )
        row = df.first()
        assert row["shipping"]["city"] == "Seattle"

    def test_ddl_structtype_roundtrip(self, spark):
        """Parse a DDL string via a dummy DataFrame schema."""
        ddl = "name STRING, age INT, score DOUBLE"
        df = spark.createDataFrame([], ddl)
        struct = df.schema
        assert len(struct.fields) == 3
        assert struct["name"].dataType == StringType()
        assert struct["age"].dataType == IntegerType()


# ---------------------------------------------------------------------------
# 4. JSON Schema Export / Reload
# ---------------------------------------------------------------------------

class TestSchemaExport:
    """Tests for exporting and reloading schemas."""

    XML = """\
<sensors>
  <reading>
    <sensor_id>S1</sensor_id>
    <temperature>22.5</temperature>
    <active>true</active>
  </reading>
</sensors>"""

    def test_json_schema_roundtrip(self, spark, tmp_path):
        path = _write_xml(tmp_path, "sensors.xml", self.XML)
        df = spark.read.format("xml").option("rowTag", "reading").load(path)
        original = df.schema

        schema_path = tmp_path / "schema.json"
        schema_path.write_text(
            json.dumps(original.jsonValue(), indent=2), encoding="utf-8"
        )

        with open(schema_path) as f:
            reloaded = StructType.fromJson(json.load(f))
        assert reloaded == original

    def test_ddl_schema_export(self, spark, tmp_path):
        path = _write_xml(tmp_path, "sensors.xml", self.XML)
        df = spark.read.format("xml").option("rowTag", "reading").load(path)
        ddl = df.schema.simpleString()
        assert "sensor_id" in ddl
        assert "temperature" in ddl

    def test_reloaded_json_schema_reads_data(self, spark, tmp_path):
        path = _write_xml(tmp_path, "sensors.xml", self.XML)
        df = spark.read.format("xml").option("rowTag", "reading").load(path)

        schema_path = tmp_path / "schema.json"
        schema_path.write_text(
            json.dumps(df.schema.jsonValue()), encoding="utf-8"
        )

        with open(schema_path) as f:
            schema = StructType.fromJson(json.load(f))

        df2 = (
            spark.read.format("xml")
            .option("rowTag", "reading")
            .schema(schema)
            .load(path)
        )
        assert df2.count() == 1
        assert df2.first()["sensor_id"] == "S1"


# ---------------------------------------------------------------------------
# 5. Schema Evolution
# ---------------------------------------------------------------------------

class TestSchemaEvolution:
    """Tests for handling schema changes across XML versions."""

    V1_XML = """\
<users>
  <user><id>1</id><name>Alice</name><age>30</age></user>
  <user><id>2</id><name>Bob</name><age>25</age></user>
</users>"""

    V2_XML = """\
<users>
  <user><id>3</id><full_name>Carol</full_name><phone>555-0103</phone></user>
  <user><id>4</id><full_name>David</full_name><phone>555-0104</phone></user>
</users>"""

    def test_unified_schema_fills_nulls(self, spark, tmp_path):
        v1_path = _write_xml(tmp_path, "v1.xml", self.V1_XML)
        v2_path = _write_xml(tmp_path, "v2.xml", self.V2_XML)

        schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("age", LongType(), True),
            StructField("phone", StringType(), True),
        ])

        df_v1 = (
            spark.read.format("xml")
            .option("rowTag", "user").schema(schema).load(v1_path)
        )
        df_v2 = (
            spark.read.format("xml")
            .option("rowTag", "user").schema(schema).load(v2_path)
        )

        # V1 should have null for full_name and phone
        row_v1 = df_v1.first()
        assert row_v1["full_name"] is None
        assert row_v1["phone"] is None

        # V2 should have null for name and age
        row_v2 = df_v2.first()
        assert row_v2["name"] is None
        assert row_v2["age"] is None

    def test_union_by_name(self, spark, tmp_path):
        v1_path = _write_xml(tmp_path, "v1.xml", self.V1_XML)
        v2_path = _write_xml(tmp_path, "v2.xml", self.V2_XML)

        df_v1 = spark.read.format("xml").option("rowTag", "user").load(v1_path)
        df_v2 = spark.read.format("xml").option("rowTag", "user").load(v2_path)

        df = df_v1.unionByName(df_v2, allowMissingColumns=True)
        assert df.count() == 4

    def test_coalesce_renamed_column(self, spark, tmp_path):
        v1_path = _write_xml(tmp_path, "v1.xml", self.V1_XML)
        v2_path = _write_xml(tmp_path, "v2.xml", self.V2_XML)

        schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("full_name", StringType(), True),
        ])

        df_v1 = (
            spark.read.format("xml")
            .option("rowTag", "user").schema(schema).load(v1_path)
        )
        df_v2 = (
            spark.read.format("xml")
            .option("rowTag", "user").schema(schema).load(v2_path)
        )
        df = df_v1.unionByName(df_v2).withColumn(
            "display_name", F.coalesce("full_name", "name")
        )
        names = {r["display_name"] for r in df.collect()}
        assert names == {"Alice", "Bob", "Carol", "David"}


# ---------------------------------------------------------------------------
# 6. Schema Merge
# ---------------------------------------------------------------------------

class TestSchemaMerge:
    """Tests for reading multiple XML files with different schemas."""

    FILE_A = """\
<products>
  <item><name>Widget</name><price>10.99</price><color>red</color></item>
</products>"""

    FILE_B = """\
<products>
  <item><name>Gadget</name><price>25.00</price><weight>0.5</weight></item>
</products>"""

    def test_directory_read_merges_schemas(self, spark, tmp_path):
        merge_dir = tmp_path / "merge"
        merge_dir.mkdir()
        (merge_dir / "a.xml").write_text(self.FILE_A, encoding="utf-8")
        (merge_dir / "b.xml").write_text(self.FILE_B, encoding="utf-8")

        df = (
            spark.read.format("xml")
            .option("rowTag", "item")
            .load(str(merge_dir / "*.xml"))
        )
        assert df.count() == 2
        assert "color" in df.columns
        assert "weight" in df.columns

    def test_explicit_merged_schema(self, spark, tmp_path):
        merge_dir = tmp_path / "merge2"
        merge_dir.mkdir()
        (merge_dir / "a.xml").write_text(self.FILE_A, encoding="utf-8")
        (merge_dir / "b.xml").write_text(self.FILE_B, encoding="utf-8")

        schema = StructType([
            StructField("name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("color", StringType(), True),
            StructField("weight", DoubleType(), True),
        ])
        df = (
            spark.read.format("xml")
            .option("rowTag", "item")
            .schema(schema)
            .load(str(merge_dir / "*.xml"))
        )
        assert df.schema == schema
        assert df.count() == 2


# ---------------------------------------------------------------------------
# 7. Partial Schema (Column Pruning)
# ---------------------------------------------------------------------------

class TestPartialSchema:
    """Tests for reading with a subset of columns."""

    INVENTORY_XML = """\
<inventory>
  <product>
    <sku>SKU-001</sku>
    <name>Laptop</name>
    <price>1299.99</price>
    <cost>850.00</cost>
    <quantity>45</quantity>
    <warehouse>WH-EAST</warehouse>
  </product>
  <product>
    <sku>SKU-002</sku>
    <name>Mouse</name>
    <price>29.99</price>
    <cost>12.50</cost>
    <quantity>500</quantity>
    <warehouse>WH-WEST</warehouse>
  </product>
</inventory>"""

    def test_partial_schema_fewer_columns(self, spark, tmp_path):
        path = _write_xml(tmp_path, "inv.xml", self.INVENTORY_XML)
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("price", DoubleType(), True),
        ])
        df = (
            spark.read.format("xml")
            .option("rowTag", "product")
            .schema(schema)
            .load(path)
        )
        assert set(df.columns) == {"name", "price"}
        assert df.count() == 2

    def test_partial_schema_values_correct(self, spark, tmp_path):
        path = _write_xml(tmp_path, "inv.xml", self.INVENTORY_XML)
        schema = StructType([
            StructField("sku", StringType(), True),
            StructField("price", DoubleType(), True),
        ])
        df = (
            spark.read.format("xml")
            .option("rowTag", "product")
            .schema(schema)
            .load(path)
        )
        row = df.filter(F.col("sku") == "SKU-001").first()
        assert row["price"] == pytest.approx(1299.99)

    def test_partial_ddl_schema(self, spark, tmp_path):
        path = _write_xml(tmp_path, "inv.xml", self.INVENTORY_XML)
        df = (
            spark.read.format("xml")
            .option("rowTag", "product")
            .schema("sku STRING, warehouse STRING")
            .load(path)
        )
        assert set(df.columns) == {"sku", "warehouse"}
        warehouses = {r["warehouse"] for r in df.collect()}
        assert warehouses == {"WH-EAST", "WH-WEST"}


# ---------------------------------------------------------------------------
# 8. Nested Struct Schema
# ---------------------------------------------------------------------------

class TestNestedSchema:
    """Tests for complex nested schema definitions."""

    COMPANY_XML = """\
<companies>
  <company>
    <name>TechCorp</name>
    <headquarters>
      <city>San Francisco</city>
      <state>CA</state>
    </headquarters>
    <departments>
      <department>
        <dept_name>Engineering</dept_name>
        <head_count>150</head_count>
      </department>
      <department>
        <dept_name>Marketing</dept_name>
        <head_count>40</head_count>
      </department>
    </departments>
  </company>
</companies>"""

    def test_nested_struct_access(self, spark, tmp_path):
        path = _write_xml(tmp_path, "company.xml", self.COMPANY_XML)
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("headquarters", StructType([
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
            ]), True),
        ])
        df = (
            spark.read.format("xml")
            .option("rowTag", "company")
            .schema(schema)
            .load(path)
        )
        row = df.first()
        assert row["headquarters"]["city"] == "San Francisco"

    def test_explode_nested_array(self, spark, tmp_path):
        path = _write_xml(tmp_path, "company.xml", self.COMPANY_XML)
        df = (
            spark.read.format("xml")
            .option("rowTag", "company")
            .load(path)
        )
        df_depts = df.select(
            "name",
            F.explode("departments.department").alias("dept"),
        ).select("name", "dept.dept_name", "dept.head_count")

        assert df_depts.count() == 2
        dept_names = {r["dept_name"] for r in df_depts.collect()}
        assert dept_names == {"Engineering", "Marketing"}

    def test_explicit_array_schema(self, spark, tmp_path):
        path = _write_xml(tmp_path, "company.xml", self.COMPANY_XML)
        dept_schema = StructType([
            StructField("dept_name", StringType(), True),
            StructField("head_count", LongType(), True),
        ])
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("departments", StructType([
                StructField("department", ArrayType(dept_schema), True),
            ]), True),
        ])
        df = (
            spark.read.format("xml")
            .option("rowTag", "company")
            .schema(schema)
            .load(path)
        )
        row = df.first()
        depts = row["departments"]["department"]
        assert len(depts) == 2


# ---------------------------------------------------------------------------
# 9. Schema with Attributes
# ---------------------------------------------------------------------------

class TestSchemaWithAttributes:
    """Tests for XML attribute handling in schema."""

    BOOKSTORE_XML = """\
<bookstore>
  <book id="bk101" category="fiction">
    <title>The Great Gatsby</title>
    <price currency="USD">10.99</price>
  </book>
  <book id="bk102" category="science">
    <title>A Brief History of Time</title>
    <price currency="GBP">12.50</price>
  </book>
</bookstore>"""

    def test_default_attribute_prefix(self, spark, tmp_path):
        path = _write_xml(tmp_path, "books.xml", self.BOOKSTORE_XML)
        df = (
            spark.read.format("xml")
            .option("rowTag", "book")
            .load(path)
        )
        assert "_id" in df.columns
        assert "_category" in df.columns

    def test_custom_attribute_prefix(self, spark, tmp_path):
        path = _write_xml(tmp_path, "books.xml", self.BOOKSTORE_XML)
        df = (
            spark.read.format("xml")
            .option("rowTag", "book")
            .option("attributePrefix", "attr_")
            .load(path)
        )
        assert "attr_id" in df.columns
        assert "attr_category" in df.columns

    def test_value_tag_access(self, spark, tmp_path):
        path = _write_xml(tmp_path, "books.xml", self.BOOKSTORE_XML)
        schema = StructType([
            StructField("_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("price", StructType([
                StructField("_currency", StringType(), True),
                StructField("_VALUE", DoubleType(), True),
            ]), True),
        ])
        df = (
            spark.read.format("xml")
            .option("rowTag", "book")
            .schema(schema)
            .load(path)
        )
        row = df.filter(F.col("_id") == "bk101").first()
        assert row["price"]["_VALUE"] == pytest.approx(10.99)
        assert row["price"]["_currency"] == "USD"

    def test_exclude_attributes(self, spark, tmp_path):
        path = _write_xml(tmp_path, "books.xml", self.BOOKSTORE_XML)
        df = (
            spark.read.format("xml")
            .option("rowTag", "book")
            .option("excludeAttribute", "true")
            .load(path)
        )
        assert "_id" not in df.columns
        assert "_category" not in df.columns
        assert "title" in df.columns


# ---------------------------------------------------------------------------
# 10. XSD Validation
# ---------------------------------------------------------------------------

class TestXsdValidation:
    """Tests for XSD-based row validation."""

    VALID_XML = """\
<orders>
  <order>
    <order_id>ORD-001</order_id>
    <customer>Alice</customer>
    <amount>250.00</amount>
  </order>
  <order>
    <order_id>ORD-002</order_id>
    <customer>Bob</customer>
    <amount>125.50</amount>
  </order>
</orders>"""

    ORDER_XSD = """\
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="order">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="order_id" type="xs:string"/>
        <xs:element name="customer" type="xs:string"/>
        <xs:element name="amount" type="xs:decimal"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

    def test_xsd_validation_valid_data(self, spark, tmp_path):
        xml_path = _write_xml(tmp_path, "orders.xml", self.VALID_XML)
        xsd_path = _write_xml(tmp_path, "order.xsd", self.ORDER_XSD)

        spark.sparkContext.addFile(xsd_path)

        df = (
            spark.read.format("com.databricks.spark.xml")
            .option("rowTag", "order")
            .option("rowValidationXSDPath", "order.xsd")
            .load(xml_path)
        )
        assert df.count() == 2
        assert "order_id" in df.columns

    def test_xsd_dropmalformed(self, spark, tmp_path):
        corrupt_xml = """\
<orders>
  <order>
    <order_id>ORD-001</order_id>
    <customer>Alice</customer>
    <amount>250.00</amount>
  </order>
  <order>
    <order_id>ORD-002</order_id>
    <extra>unexpected</extra>
    <customer>Bob</customer>
    <amount>125.50</amount>
  </order>
</orders>"""
        xml_path = _write_xml(tmp_path, "corrupt.xml", corrupt_xml)
        xsd_path = _write_xml(tmp_path, "order.xsd", self.ORDER_XSD)

        spark.sparkContext.addFile(xsd_path)

        df = (
            spark.read.format("com.databricks.spark.xml")
            .option("rowTag", "order")
            .option("rowValidationXSDPath", "order.xsd")
            .option("mode", "DROPMALFORMED")
            .load(xml_path)
        )
        assert df.count() == 1
        assert df.first()["order_id"] == "ORD-001"


# ---------------------------------------------------------------------------
# 11. Write + Read Round-Trip Schema Preservation
# ---------------------------------------------------------------------------

class TestSchemaRoundTrip:
    """Tests that schema is preserved through write → read cycles."""

    def test_roundtrip_preserves_data(self, spark, tmp_path):
        data = [("Alice", 30, 95.5), ("Bob", 25, 87.0)]
        df_write = spark.createDataFrame(data, ["name", "age", "score"])

        output = str(tmp_path / "roundtrip")
        (
            df_write.write.format("xml")
            .mode("overwrite")
            .option("rootTag", "people")
            .option("rowTag", "person")
            .save(output)
        )

        df_read = (
            spark.read.format("xml")
            .option("rowTag", "person")
            .load(output)
        )
        assert df_read.count() == 2
        names = {r["name"] for r in df_read.collect()}
        assert names == {"Alice", "Bob"}

    def test_roundtrip_with_explicit_schema(self, spark, tmp_path):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
        ])
        data = [("X", 1.1), ("Y", 2.2)]
        df_write = spark.createDataFrame(data, schema)

        output = str(tmp_path / "rt_schema")
        (
            df_write.write.format("xml")
            .mode("overwrite")
            .option("rootTag", "root")
            .option("rowTag", "row")
            .save(output)
        )

        df_read = (
            spark.read.format("xml")
            .option("rowTag", "row")
            .schema(schema)
            .load(output)
        )
        assert df_read.schema == schema
        assert df_read.count() == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
