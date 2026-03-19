"""Demonstrate reading and writing XML containing CDATA sections.

CDATA (Character Data) sections let XML carry unescaped text — HTML
snippets, code fragments, special characters — without breaking the
parser.  spark-xml transparently reads CDATA content as regular string
values.  This script shows:

1. Reading XML with CDATA elements (text is extracted automatically)
2. CDATA mixed with regular elements and attributes
3. CDATA containing HTML, JSON, and special characters
4. Writing DataFrames back as XML (Spark writes escaped text, not CDATA)
5. Round-trip verification (content preserved despite encoding change)
6. Explicit schema with CDATA columns
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql import functions as F
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

# ── Sample XML with various CDATA patterns ──────────────────────────────

ARTICLES_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <articles>
      <article id="1">
        <title>Introduction to XML</title>
        <author>Alice</author>
        <content><![CDATA[
          <p>XML is a <b>markup language</b> used for storing data.</p>
          <p>Special characters like <, >, & are allowed inside CDATA.</p>
        ]]></content>
        <views>1500</views>
      </article>
      <article id="2">
        <title>Working with JSON</title>
        <author>Bob</author>
        <content><![CDATA[
          {"tip": "Use CDATA to embed JSON inside XML without escaping quotes"}
        ]]></content>
        <views>2300</views>
      </article>
      <article id="3">
        <title>SQL & Special Characters</title>
        <author>Carol</author>
        <content><![CDATA[
          SELECT * FROM users WHERE age > 18 AND name <> 'O''Brien';
          -- The <, >, & characters don't need escaping in CDATA
        ]]></content>
        <views>980</views>
      </article>
    </articles>
""")

# CDATA mixed with attributes and nested elements
PRODUCTS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <catalog>
      <product sku="PRD-001" category="electronics">
        <name>Smart TV 55"</name>
        <price>699.99</price>
        <description><![CDATA[
          55-inch 4K Ultra HD Smart TV with HDR10+.
          Dimensions: 48.5" x 28.1" x 2.4" (without stand).
          Features: Built-in Alexa & Google Assistant.
        ]]></description>
        <specs><![CDATA[{"resolution":"3840x2160","refresh":"120Hz","ports":{"hdmi":4,"usb":2}}]]></specs>
      </product>
      <product sku="PRD-002" category="audio">
        <name>Wireless Headphones</name>
        <price>149.50</price>
        <description><![CDATA[
          Active noise cancellation with 30-hour battery life.
          Bluetooth 5.2 with multipoint connection.
        ]]></description>
        <specs><![CDATA[{"driver":"40mm","impedance":"32Ω","weight":"250g"}]]></specs>
      </product>
      <product sku="PRD-003" category="electronics">
        <name>USB-C Hub</name>
        <price>49.99</price>
        <description><![CDATA[7-in-1 hub: 2×USB-A, 1×USB-C, HDMI, SD, microSD, Ethernet]]></description>
        <specs><![CDATA[{"ports":7,"power_delivery":"100W","data_rate":"10Gbps"}]]></specs>
      </product>
    </catalog>
""")

# CDATA with code / script content
TEMPLATES_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <templates>
      <template name="greeting">
        <language>Python</language>
        <code><![CDATA[
def greet(name: str) -> str:
    return f"Hello, {name}! Welcome to <AppName>."

if __name__ == "__main__":
    print(greet("World"))
        ]]></code>
      </template>
      <template name="query">
        <language>SQL</language>
        <code><![CDATA[
SELECT u.name, COUNT(o.id) AS order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at > '2024-01-01'
  AND o.total > 100.00
GROUP BY u.name
HAVING COUNT(o.id) >= 3;
        ]]></code>
      </template>
      <template name="config">
        <language>YAML</language>
        <code><![CDATA[
server:
  host: 0.0.0.0
  port: 8080
  workers: 4
logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s"
        ]]></code>
      </template>
    </templates>
""")


def write_xml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "cdata_demo"

    articles_path = base / "articles.xml"
    products_path = base / "products.xml"
    templates_path = base / "templates.xml"
    write_xml(articles_path, ARTICLES_XML)
    write_xml(products_path, PRODUCTS_XML)
    write_xml(templates_path, TEMPLATES_XML)

    spark = get_spark_session(
        app_name="spark-xml-cdata",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ------------------------------------------------------------------
    # 1. Basic CDATA reading — content extracted as plain string
    # ------------------------------------------------------------------
    print("=== 1. Read CDATA Articles ===")
    df_articles = (
        spark.read.format("xml")
        .option("rowTag", "article")
        .load(articles_path.as_posix())
    )
    df_articles.printSchema()
    df_articles.show(truncate=False)

    print("--- CDATA content is a normal string column ---")
    df_articles.select(
        "_id", "title", F.length("content").alias("content_length")
    ).show(truncate=False)

    # ------------------------------------------------------------------
    # 2. CDATA with attributes and nested elements
    # ------------------------------------------------------------------
    print("\n=== 2. Products with CDATA Description + JSON Specs ===")
    product_schema = StructType([
        StructField("_sku", StringType(), True),
        StructField("_category", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("description", StringType(), True),
        StructField("specs", StringType(), True),
    ])

    df_products = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .schema(product_schema)
        .load(products_path.as_posix())
    )
    df_products.printSchema()
    df_products.show(truncate=False)

    # Parse the embedded JSON specs string
    print("--- Extract fields from CDATA JSON specs ---")
    df_products.select(
        "_sku",
        "name",
        "price",
        F.get_json_object("specs", "$.resolution").alias("resolution"),
        F.get_json_object("specs", "$.weight").alias("weight"),
        F.get_json_object("specs", "$.ports").alias("ports"),
    ).show(truncate=False)

    # ------------------------------------------------------------------
    # 3. CDATA with code/script content
    # ------------------------------------------------------------------
    print("\n=== 3. Code Templates (CDATA preserves formatting) ===")
    df_templates = (
        spark.read.format("xml")
        .option("rowTag", "template")
        .load(templates_path.as_posix())
    )
    df_templates.printSchema()
    df_templates.show(truncate=False)

    print("--- Code content by language ---")
    for row in df_templates.collect():
        print(f"\n[{row['_name']}] ({row['language']})")
        print(row["code"].strip())
        print("-" * 40)

    # ------------------------------------------------------------------
    # 4. Write DataFrame as XML (CDATA becomes escaped text)
    # ------------------------------------------------------------------
    print("\n=== 4. Write → Read Round-Trip ===")
    output_path = base / "articles_output"

    (
        df_articles.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "articles")
        .option("rowTag", "article")
        .save(output_path.as_posix())
    )

    df_roundtrip = (
        spark.read.format("xml")
        .option("rowTag", "article")
        .load(output_path.as_posix())
    )
    df_roundtrip.show(truncate=False)

    # Verify content is preserved
    original = {
        r["_id"]: r["content"].strip()
        for r in df_articles.collect()
    }
    reloaded = {
        r["_id"]: r["content"].strip()
        for r in df_roundtrip.collect()
    }
    for aid in sorted(original):
        match = original[aid] == reloaded[aid]
        print(f"  Article {aid}: content {'✓ preserved' if match else '✗ changed'}")

    # ------------------------------------------------------------------
    # 5. Filter and aggregate on CDATA content
    # ------------------------------------------------------------------
    print("\n=== 5. Search Inside CDATA Content ===")
    df_articles.filter(
        F.col("content").contains("special characters")
        | F.col("content").contains("Special characters")
    ).select("_id", "title", "author").show(truncate=False)

    print("--- Articles with HTML tags in CDATA ---")
    df_articles.filter(
        F.col("content").rlike("<[a-z]+>")
    ).select("_id", "title").show(truncate=False)

    print("--- Products by category with description length ---")
    df_products.groupBy("_category").agg(
        F.count("*").alias("count"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(F.avg(F.length("description")), 0).alias("avg_desc_length"),
    ).show(truncate=False)

    spark.stop()
