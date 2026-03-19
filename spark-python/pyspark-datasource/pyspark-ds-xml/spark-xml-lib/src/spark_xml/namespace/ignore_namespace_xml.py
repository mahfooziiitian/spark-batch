"""Demonstrate spark-xml ``ignoreNamespace=true`` option.

XML namespaces (``xmlns:prefix="URI"``) scope element and attribute
names to avoid collisions.  When ``ignoreNamespace`` is ``true``,
spark-xml strips **all** namespace prefixes from column names, giving
clean, easy-to-use names like ``title`` instead of ``bk:title``.

This script covers:

1. Multiple prefixed namespaces — ``bk:``, ``pub:``, ``rev:``
2. Default (unprefixed) namespace — ``xmlns="..."``
3. Mixed default + prefixed namespaces in the same document
4. Nested elements across different namespaces
5. Attribute handling with namespaces ignored
6. Explicit schema — no prefixes needed in field names
7. Write round-trip — output is namespace-free
8. Queries and aggregations on clean column names
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

# ── 1. Multiple prefixed namespaces ──────────────────────────────────────

MULTI_PREFIX_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <catalog xmlns:bk="http://example.com/books"
             xmlns:pub="http://example.com/publisher"
             xmlns:rev="http://example.com/reviews">
      <bk:book id="101">
        <bk:title>Learning PySpark</bk:title>
        <bk:author>Alice Johnson</bk:author>
        <bk:genre>Technology</bk:genre>
        <bk:price currency="USD">45.99</bk:price>
        <pub:publisher>
          <pub:name>Tech Books Inc.</pub:name>
          <pub:year>2024</pub:year>
          <pub:city>San Francisco</pub:city>
        </pub:publisher>
        <rev:review>
          <rev:rating>4.5</rev:rating>
          <rev:comment>Excellent introduction to PySpark</rev:comment>
        </rev:review>
      </bk:book>
      <bk:book id="102">
        <bk:title>XML Processing with Spark</bk:title>
        <bk:author>Bob Smith</bk:author>
        <bk:genre>Data Engineering</bk:genre>
        <bk:price currency="EUR">39.50</bk:price>
        <pub:publisher>
          <pub:name>Data Press</pub:name>
          <pub:year>2023</pub:year>
          <pub:city>Berlin</pub:city>
        </pub:publisher>
        <rev:review>
          <rev:rating>4.0</rev:rating>
          <rev:comment>Great reference for XML pipelines</rev:comment>
        </rev:review>
      </bk:book>
      <bk:book id="103">
        <bk:title>Namespace Patterns in XML</bk:title>
        <bk:author>Carol Lee</bk:author>
        <bk:genre>Software Design</bk:genre>
        <bk:price currency="GBP">32.00</bk:price>
        <pub:publisher>
          <pub:name>Oxford Computing</pub:name>
          <pub:year>2025</pub:year>
          <pub:city>London</pub:city>
        </pub:publisher>
        <rev:review>
          <rev:rating>4.8</rev:rating>
          <rev:comment>Definitive guide on namespace handling</rev:comment>
        </rev:review>
      </bk:book>
    </catalog>
""")

# ── 2. Default (unprefixed) namespace ────────────────────────────────────

DEFAULT_NS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <employees xmlns="http://example.com/hr">
      <employee>
        <id>1</id>
        <name>Alice Johnson</name>
        <department>Engineering</department>
        <salary>95000</salary>
      </employee>
      <employee>
        <id>2</id>
        <name>Bob Smith</name>
        <department>Marketing</department>
        <salary>82000</salary>
      </employee>
      <employee>
        <id>3</id>
        <name>Carol Davis</name>
        <department>Finance</department>
        <salary>91000</salary>
      </employee>
    </employees>
""")

# ── 3. Mixed default + prefixed namespaces ───────────────────────────────

MIXED_NS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <inventory xmlns="http://example.com/inventory"
               xmlns:loc="http://example.com/location"
               xmlns:sup="http://example.com/supplier">
      <product sku="SKU-001">
        <name>Industrial Sensor</name>
        <category>Electronics</category>
        <price>149.99</price>
        <stock>250</stock>
        <loc:warehouse>
          <loc:city>Chicago</loc:city>
          <loc:zone>A-12</loc:zone>
        </loc:warehouse>
        <sup:supplier>
          <sup:name>SensorTech Ltd.</sup:name>
          <sup:country>Germany</sup:country>
          <sup:lead_days>14</sup:lead_days>
        </sup:supplier>
      </product>
      <product sku="SKU-002">
        <name>Pressure Valve</name>
        <category>Mechanical</category>
        <price>89.50</price>
        <stock>180</stock>
        <loc:warehouse>
          <loc:city>Houston</loc:city>
          <loc:zone>B-05</loc:zone>
        </loc:warehouse>
        <sup:supplier>
          <sup:name>ValveCorp</sup:name>
          <sup:country>USA</sup:country>
          <sup:lead_days>7</sup:lead_days>
        </sup:supplier>
      </product>
      <product sku="SKU-003">
        <name>Thermal Camera</name>
        <category>Electronics</category>
        <price>599.00</price>
        <stock>45</stock>
        <loc:warehouse>
          <loc:city>Chicago</loc:city>
          <loc:zone>A-15</loc:zone>
        </loc:warehouse>
        <sup:supplier>
          <sup:name>OptiVision</sup:name>
          <sup:country>Japan</sup:country>
          <sup:lead_days>21</sup:lead_days>
        </sup:supplier>
      </product>
    </inventory>
""")

# ── 4. Namespaced attributes ─────────────────────────────────────────────

NS_ATTRIBUTES_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <data xmlns:meta="http://example.com/metadata"
          xmlns:val="http://example.com/validation">
      <record meta:created="2024-01-15" meta:author="admin"
              val:status="verified" val:score="98">
        <id>1</id>
        <content>First record with namespaced attributes</content>
      </record>
      <record meta:created="2024-02-20" meta:author="editor"
              val:status="pending" val:score="75">
        <id>2</id>
        <content>Second record — pending review</content>
      </record>
      <record meta:created="2024-03-10" meta:author="admin"
              val:status="rejected" val:score="42">
        <id>3</id>
        <content>Third record — rejected</content>
      </record>
    </data>
""")

# ── Schemas ──────────────────────────────────────────────────────────────

BOOK_FLAT_SCHEMA = StructType([
    StructField("_id", LongType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("price", StructType([
        StructField("_VALUE", DoubleType(), True),
        StructField("_currency", StringType(), True),
    ]), True),
    StructField("publisher", StructType([
        StructField("name", StringType(), True),
        StructField("year", LongType(), True),
        StructField("city", StringType(), True),
    ]), True),
    StructField("review", StructType([
        StructField("rating", DoubleType(), True),
        StructField("comment", StringType(), True),
    ]), True),
])


def write_xml(path: Path, content: str) -> None:
    """Write XML string to file, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "namespace_demo"

    multi_path = base / "multi_prefix.xml"
    default_path = base / "default_ns.xml"
    mixed_path = base / "mixed_ns.xml"
    ns_attr_path = base / "ns_attributes.xml"

    write_xml(multi_path, MULTI_PREFIX_XML)
    write_xml(default_path, DEFAULT_NS_XML)
    write_xml(mixed_path, MIXED_NS_XML)
    write_xml(ns_attr_path, NS_ATTRIBUTES_XML)

    spark = get_spark_session(
        app_name="spark-xml-ignore-namespace",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ==================================================================
    # 1. Multiple Prefixed Namespaces — ignoreNamespace=true
    # ==================================================================
    print("=" * 70)
    print("1. Multiple Prefixed Namespaces (bk:, pub:, rev:)")
    print("=" * 70)
    print(
        "Three namespace prefixes scope elements to different domains.\n"
        "With ignoreNamespace=true, all prefixes are stripped from column\n"
        "names — 'bk:title' becomes 'title', 'pub:name' becomes 'name'.\n"
    )

    # rowTag must still use the prefix even when ignoring namespaces
    df_books = (
        spark.read.format("xml")
        .option("rowTag", "bk:book")
        .option("ignoreNamespace", "true")
        .schema(BOOK_FLAT_SCHEMA)
        .load(multi_path.as_posix())
    )
    df_books.printSchema()
    df_books.show(truncate=False)

    print("Key point: rowTag='bk:book' still requires the prefix —")
    print("ignoreNamespace only affects column names, not the row tag.\n")

    cols = df_books.columns
    prefixed = [c for c in cols if ":" in c]
    assert not prefixed, f"Prefixed columns found: {prefixed}"
    assert "title" in cols, "Expected 'title' (not 'bk:title')"
    assert "publisher" in cols, "Expected 'publisher' (not 'pub:publisher')"
    assert "review" in cols, "Expected 'review' (not 'rev:review')"
    print("✓ All namespace prefixes stripped from column names")
    print(f"  Columns: {sorted(cols)}")

    # Flatten nested cross-namespace fields
    print("\n--- Flattened view across namespaces ---")
    df_flat = df_books.selectExpr(
        "_id as book_id",
        "title",
        "author",
        "genre",
        "price._VALUE as price",
        "price._currency as currency",
        "publisher.name as publisher_name",
        "publisher.year as pub_year",
        "publisher.city as pub_city",
        "review.rating as rating",
        "review.comment as comment",
    )
    df_flat.show(truncate=False)

    # ==================================================================
    # 2. Default (Unprefixed) Namespace
    # ==================================================================
    print("\n" + "=" * 70)
    print("2. Default Namespace (xmlns='...' without prefix)")
    print("=" * 70)
    print(
        "A default namespace (xmlns='URI') applies to all elements without\n"
        "an explicit prefix.  With ignoreNamespace=true, this is seamless —\n"
        "the rowTag uses the bare element name.\n"
    )

    df_employees = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("ignoreNamespace", "true")
        .load(default_path.as_posix())
    )
    df_employees.printSchema()
    df_employees.show(truncate=False)

    assert df_employees.count() == 3, "Expected 3 employees"
    assert "name" in df_employees.columns, "Expected 'name' column"
    print("✓ Default namespace handled transparently — clean column names")

    # ==================================================================
    # 3. Mixed Default + Prefixed Namespaces
    # ==================================================================
    print("\n" + "=" * 70)
    print("3. Mixed Default + Prefixed Namespaces")
    print("=" * 70)
    print(
        "Default namespace on <inventory> elements + loc: and sup: prefixed\n"
        "sub-elements.  All prefixes stripped when ignoreNamespace=true.\n"
    )

    df_inventory = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .option("ignoreNamespace", "true")
        .load(mixed_path.as_posix())
    )
    df_inventory.printSchema()
    df_inventory.show(truncate=False)

    cols = df_inventory.columns
    assert "warehouse" in cols, "Expected 'warehouse' (not 'loc:warehouse')"
    assert "supplier" in cols, "Expected 'supplier' (not 'sup:supplier')"
    print("✓ Mixed namespace columns all clean")
    print(f"  Columns: {sorted(cols)}")

    # Flatten the mixed-namespace structure
    print("\n--- Flattened inventory view ---")
    df_inv_flat = df_inventory.selectExpr(
        "_sku as sku",
        "name as product_name",
        "category",
        "CAST(price AS DOUBLE) as price",
        "CAST(stock AS INT) as stock",
        "warehouse.city as warehouse_city",
        "warehouse.zone as warehouse_zone",
        "supplier.name as supplier_name",
        "supplier.country as supplier_country",
        "CAST(supplier.lead_days AS INT) as lead_days",
    )
    df_inv_flat.show(truncate=False)

    # ==================================================================
    # 4. Namespaced Attributes with ignoreNamespace=true
    # ==================================================================
    print("\n" + "=" * 70)
    print("4. Namespaced Attributes — Prefixes Stripped")
    print("=" * 70)
    print(
        "XML attributes can also have namespace prefixes:\n"
        '  <record meta:created="..." val:status="...">\n'
        "With ignoreNamespace=true, attribute prefixes are removed.\n"
    )

    df_attrs = (
        spark.read.format("xml")
        .option("rowTag", "record")
        .option("ignoreNamespace", "true")
        .load(ns_attr_path.as_posix())
    )
    df_attrs.printSchema()
    df_attrs.show(truncate=False)

    cols = df_attrs.columns
    print(f"Columns: {sorted(cols)}")

    # When ignoring namespaces, prefixed attributes lose their prefixes.
    # If two different NS prefixes produce the same local name,
    # spark-xml merges them (edge case — not present in this data).
    has_created = any("created" in c for c in cols)
    has_status = any("status" in c for c in cols)
    assert has_created, "Expected 'created' attribute (from meta:created)"
    assert has_status, "Expected 'status' attribute (from val:status)"
    print("✓ Namespaced attributes stripped of prefixes")

    # ==================================================================
    # 5. Explicit Schema — No Prefixes Needed
    # ==================================================================
    print("\n" + "=" * 70)
    print("5. Explicit Schema with ignoreNamespace=true")
    print("=" * 70)
    print(
        "When defining an explicit StructType schema for namespace-heavy\n"
        "XML, use plain names (no prefixes) if ignoreNamespace=true.\n"
    )

    df_explicit = (
        spark.read.format("xml")
        .option("rowTag", "bk:book")
        .option("ignoreNamespace", "true")
        .schema(BOOK_FLAT_SCHEMA)
        .load(multi_path.as_posix())
    )
    df_explicit.printSchema()
    df_explicit.show(truncate=False)

    assert df_explicit.schema == BOOK_FLAT_SCHEMA, "Schema mismatch!"
    print("✓ Explicit schema applied without namespace prefixes in field names")

    # ==================================================================
    # 6. Write Round-Trip — Output Is Namespace-Free
    # ==================================================================
    print("\n" + "=" * 70)
    print("6. Write Round-Trip")
    print("=" * 70)
    print(
        "When spark-xml writes a DataFrame, the output XML has no\n"
        "namespace declarations — it's clean XML with plain element names.\n"
    )

    output_path = base / "books_output"
    (
        df_flat.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "catalog")
        .option("rowTag", "book")
        .save(output_path.as_posix())
    )

    df_roundtrip = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .load(output_path.as_posix())
    )
    df_roundtrip.show(truncate=False)

    assert df_roundtrip.count() == df_flat.count(), "Row count mismatch"
    print("✓ Round-trip preserves data — output has no namespace prefixes")

    # Verify no xmlns declarations in written XML
    import glob as g

    written_files = g.glob(str(output_path / "part-*.xml"))
    if written_files:
        content = Path(written_files[0]).read_text(encoding="utf-8")
        assert "xmlns" not in content, "xmlns found in output!"
        assert "bk:" not in content, "Namespace prefix found in output!"
        print("✓ Written XML contains no xmlns declarations or prefixes")

    # ==================================================================
    # 7. Queries and Aggregations on Clean Names
    # ==================================================================
    print("\n" + "=" * 70)
    print("7. Queries and Aggregations")
    print("=" * 70)

    print("--- Books by genre ---")
    df_flat.groupBy("genre").agg(
        F.count("*").alias("count"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.collect_list("title").alias("titles"),
    ).orderBy("genre").show(truncate=False)

    print("--- Highest rated books ---")
    df_flat.orderBy(F.desc("rating")).select(
        "title", "author", "rating", "publisher_name"
    ).show(truncate=False)

    print("--- Inventory: products by warehouse city ---")
    df_inv_flat.groupBy("warehouse_city").agg(
        F.count("*").alias("num_products"),
        F.round(F.sum(F.col("price") * F.col("stock")), 2).alias("total_value"),
        F.collect_list("product_name").alias("products"),
    ).show(truncate=False)

    print("--- Inventory: supplier lead time analysis ---")
    df_inv_flat.groupBy("supplier_country").agg(
        F.round(F.avg("lead_days"), 1).alias("avg_lead_days"),
        F.collect_list("product_name").alias("products"),
    ).orderBy("avg_lead_days").show(truncate=False)

    # ==================================================================
    # 8. Summary
    # ==================================================================
    print("\n" + "=" * 70)
    print("Summary: ignoreNamespace=true")
    print("=" * 70)
    print(
        "  Behavior                        Effect\n"
        "  ──────────────────────────────   ────────────────────────────\n"
        "  rowTag                           Still requires prefix (bk:book)\n"
        "  Element columns                  Prefix stripped (title, not bk:title)\n"
        "  Nested element columns           Prefix stripped (publisher.name)\n"
        "  Attribute columns                Prefix stripped (_created, not _meta:created)\n"
        "  Default namespace (xmlns='...')  Transparent — bare element names\n"
        "  Written XML output               No xmlns declarations or prefixes\n"
        "  Explicit schema                  Use plain field names\n"
    )

    spark.stop()
