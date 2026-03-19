"""Demonstrate schema merging when reading multiple XML files.

When a directory contains XML files with slightly different structures,
spark-xml must reconcile the schemas. This script shows:
- Reading a directory of heterogeneous XML files
- How spark-xml merges schemas by union of all columns
- Using an explicit schema to control the merged result
"""

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

# File 1: has "color" column
PRODUCTS_A_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <products>
      <item>
        <name>Widget A</name>
        <price>10.99</price>
        <color>red</color>
        <quantity>100</quantity>
      </item>
      <item>
        <name>Widget B</name>
        <price>15.50</price>
        <color>blue</color>
        <quantity>200</quantity>
      </item>
    </products>
""")

# File 2: has "weight" column instead of "color"
PRODUCTS_B_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <products>
      <item>
        <name>Gadget X</name>
        <price>25.00</price>
        <weight>0.5</weight>
        <quantity>50</quantity>
      </item>
      <item>
        <name>Gadget Y</name>
        <price>30.75</price>
        <weight>1.2</weight>
        <quantity>75</quantity>
      </item>
    </products>
""")

# File 3: has both "color" and "weight", plus "category"
PRODUCTS_C_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <products>
      <item>
        <name>Premium Z</name>
        <price>99.99</price>
        <color>silver</color>
        <weight>2.0</weight>
        <quantity>10</quantity>
        <category>premium</category>
      </item>
    </products>
""")

MERGED_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("color", StringType(), True),
    StructField("weight", DoubleType(), True),
    StructField("quantity", LongType(), True),
    StructField("category", StringType(), True),
])


def write_xml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    merge_dir = Path(data_home) / "file_data" / "xml" / "schema_demo" / "merge"

    write_xml(merge_dir / "products_a.xml", PRODUCTS_A_XML)
    write_xml(merge_dir / "products_b.xml", PRODUCTS_B_XML)
    write_xml(merge_dir / "products_c.xml", PRODUCTS_C_XML)

    spark = get_spark_session(
        app_name="spark-xml-schema-merge",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # --- 1. Read individual files (different schemas) ---
    print("=== File A schema ===")
    df_a = (
        spark.read.format("xml")
        .option("rowTag", "item")
        .load((merge_dir / "products_a.xml").as_posix())
    )
    df_a.printSchema()

    print("=== File B schema ===")
    df_b = (
        spark.read.format("xml")
        .option("rowTag", "item")
        .load((merge_dir / "products_b.xml").as_posix())
    )
    df_b.printSchema()

    print("=== File C schema ===")
    df_c = (
        spark.read.format("xml")
        .option("rowTag", "item")
        .load((merge_dir / "products_c.xml").as_posix())
    )
    df_c.printSchema()

    # --- 2. Read entire directory (spark-xml infers a merged schema) ---
    print("\n=== Merged Schema (read whole directory) ===")
    df_merged = (
        spark.read.format("xml")
        .option("rowTag", "item")
        .load((merge_dir / "*.xml").as_posix())
    )
    df_merged.printSchema()
    df_merged.show(truncate=False)

    # --- 3. Read with explicit merged schema (consistent column order) ---
    print("=== Read with Explicit Merged Schema ===")
    df_explicit = (
        spark.read.format("xml")
        .option("rowTag", "item")
        .schema(MERGED_SCHEMA)
        .load((merge_dir / "*.xml").as_posix())
    )
    df_explicit.printSchema()
    df_explicit.show(truncate=False)

    # --- 4. Union individual DataFrames with allowMissingColumns ---
    print("=== Union with allowMissingColumns ===")
    df_union = df_a.unionByName(df_b, allowMissingColumns=True)
    df_union = df_union.unionByName(df_c, allowMissingColumns=True)
    df_union.show(truncate=False)

    spark.stop()
