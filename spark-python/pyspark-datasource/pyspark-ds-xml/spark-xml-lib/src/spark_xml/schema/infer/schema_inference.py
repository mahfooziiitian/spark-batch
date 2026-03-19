"""Demonstrate automatic schema inference when reading XML with spark-xml.

When no explicit schema is provided, spark-xml scans the XML data and
infers column names and data types. This script shows:
- Default inference behaviour
- Comparing inferred types against expected types
- How inference handles mixed-type elements (falls back to StringType)
- Using samplingRatio to speed up inference on large files
"""

import os
import sys
import textwrap
from pathlib import Path


from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

PRODUCTS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <catalog>
      <product>
        <id>1</id>
        <name>Laptop</name>
        <price>999.99</price>
        <quantity>50</quantity>
        <available>true</available>
        <release_date>2024-03-15</release_date>
      </product>
      <product>
        <id>2</id>
        <name>Keyboard</name>
        <price>79.50</price>
        <quantity>200</quantity>
        <available>true</available>
        <release_date>2023-11-01</release_date>
      </product>
      <product>
        <id>3</id>
        <name>Monitor</name>
        <price>449.00</price>
        <quantity>0</quantity>
        <available>false</available>
        <release_date>2024-06-20</release_date>
      </product>
      <product>
        <id>4</id>
        <name>Mouse</name>
        <price>29.99</price>
        <quantity>500</quantity>
        <available>true</available>
        <release_date>2022-08-10</release_date>
      </product>
    </catalog>
""")

# XML where the same element has mixed types across rows
MIXED_TYPES_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <records>
      <record>
        <id>1</id>
        <value>42</value>
      </record>
      <record>
        <id>2</id>
        <value>hello</value>
      </record>
      <record>
        <id>3</id>
        <value>3.14</value>
      </record>
    </records>
""")


def write_xml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "schema_demo"

    products_path = base / "products.xml"
    mixed_path = base / "mixed_types.xml"
    write_xml(products_path, PRODUCTS_XML)
    write_xml(mixed_path, MIXED_TYPES_XML)

    spark = get_spark_session(
        app_name="spark-xml-schema-inference",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # --- 1. Default schema inference ---
    print("=== Default Schema Inference ===")
    df = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .load(products_path.as_posix())
    )
    df.printSchema()
    df.show(truncate=False)

    for field in df.schema.fields:
        print(f"  {field.name:20s} → {field.dataType.simpleString()}")

    # --- 2. Inference with samplingRatio ---
    print("\n=== Schema Inference with samplingRatio ===")
    df_sampled = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .option("samplingRatio", "0.5")
        .load(products_path.as_posix())
    )
    df_sampled.printSchema()
    df_sampled.show(truncate=False)

    # --- 3. Mixed-type inference (falls back to string) ---
    print("\n=== Mixed-Type Inference ===")
    df_mixed = (
        spark.read.format("xml")
        .option("rowTag", "record")
        .load(mixed_path.as_posix())
    )
    df_mixed.printSchema()
    df_mixed.show(truncate=False)
    print(
        "Note: 'value' column is inferred as StringType because rows "
        "contain both numbers and text."
    )

    # --- 4. Exclude attributes during inference ---
    print("\n=== Inference with excludeAttribute ===")
    df_no_attrs = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .option("excludeAttribute", "true")
        .load(products_path.as_posix())
    )
    df_no_attrs.printSchema()
    df_no_attrs.show(truncate=False)

    spark.stop()
