"""Demonstrate fully-qualified namespace handling in spark-xml.

A **fully-qualified** XML element has both a namespace prefix and a
URI binding (``xmlns:prefix="URI"``), creating a globally unique
identity: ``{URI}localName``.  This is how XML avoids name collisions
when combining vocabularies from different standards or organisations.

spark-xml works with **prefix:localName** strings — it does NOT
expose the underlying namespace URIs as column data (except as
``_xmlns:prefix`` pseudo-attributes in certain cases).

This script covers:

1. Fully-qualified elements — prefix:localName pattern
2. Same local name in different namespaces — the disambiguation use case
3. Qualified attributes — ``xsi:type``, ``xml:lang``, ``xlink:href``
4. Industry-standard namespaces — SOAP, Atom, XHTML, SVG
5. Qualified elements with ``attributePrefix`` control
6. Explicit schema for qualified names (both ignore modes)
7. Renaming qualified columns for clean downstream use
8. Write round-trip — qualified names as element names
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

# ── 1. Fully-qualified elements ──────────────────────────────────────────

QUALIFIED_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <doc:document xmlns:doc="http://example.com/document"
                  xmlns:meta="http://example.com/metadata"
                  xmlns:content="http://example.com/content">
      <doc:section>
        <doc:id>1</doc:id>
        <doc:title>Introduction</doc:title>
        <meta:created>2024-01-15</meta:created>
        <meta:author>Alice</meta:author>
        <meta:version>1.0</meta:version>
        <content:body>This is the introduction section.</content:body>
        <content:word_count>42</content:word_count>
      </doc:section>
      <doc:section>
        <doc:id>2</doc:id>
        <doc:title>Methods</doc:title>
        <meta:created>2024-02-20</meta:created>
        <meta:author>Bob</meta:author>
        <meta:version>2.1</meta:version>
        <content:body>Research methodology overview.</content:body>
        <content:word_count>128</content:word_count>
      </doc:section>
      <doc:section>
        <doc:id>3</doc:id>
        <doc:title>Conclusion</doc:title>
        <meta:created>2024-03-10</meta:created>
        <meta:author>Alice</meta:author>
        <meta:version>1.3</meta:version>
        <content:body>Summary of findings and future work.</content:body>
        <content:word_count>95</content:word_count>
      </doc:section>
    </doc:document>
""")

# ── 2. Same local name in different namespaces ───────────────────────────
# Both "hr:name" and "dept:name" have local name "name" but mean different
# things.  This is the primary reason namespaces exist.

COLLISION_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <company xmlns:hr="http://example.com/hr"
             xmlns:dept="http://example.com/department"
             xmlns:proj="http://example.com/project">
      <hr:employee>
        <hr:id>E001</hr:id>
        <hr:name>Alice Johnson</hr:name>
        <hr:title>Senior Engineer</hr:title>
        <dept:department>
          <dept:name>Engineering</dept:name>
          <dept:code>ENG-100</dept:code>
        </dept:department>
        <proj:project>
          <proj:name>Data Platform</proj:name>
          <proj:budget>500000</proj:budget>
        </proj:project>
      </hr:employee>
      <hr:employee>
        <hr:id>E002</hr:id>
        <hr:name>Bob Smith</hr:name>
        <hr:title>Marketing Director</hr:title>
        <dept:department>
          <dept:name>Marketing</dept:name>
          <dept:code>MKT-200</dept:code>
        </dept:department>
        <proj:project>
          <proj:name>Brand Refresh</proj:name>
          <proj:budget>250000</proj:budget>
        </proj:project>
      </hr:employee>
      <hr:employee>
        <hr:id>E003</hr:id>
        <hr:name>Carol Lee</hr:name>
        <hr:title>Finance Analyst</hr:title>
        <dept:department>
          <dept:name>Finance</dept:name>
          <dept:code>FIN-300</dept:code>
        </dept:department>
        <proj:project>
          <proj:name>Budget Optimization</proj:name>
          <proj:budget>150000</proj:budget>
        </proj:project>
      </hr:employee>
    </company>
""")

# ── 3. Qualified attributes ──────────────────────────────────────────────
# Standard qualified attributes: xsi:type, xml:lang, xlink:href

QUALIFIED_ATTRS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <resources xmlns:xlink="http://www.w3.org/1999/xlink"
               xmlns:xml="http://www.w3.org/XML/1998/namespace">
      <resource>
        <id>1</id>
        <title xml:lang="en">PySpark Documentation</title>
        <link xlink:href="https://spark.apache.org/docs/latest/api/python/"
              xlink:type="simple"
              xlink:title="Official Docs"/>
        <category>Documentation</category>
        <rating>4.8</rating>
      </resource>
      <resource>
        <id>2</id>
        <title xml:lang="en">spark-xml GitHub</title>
        <link xlink:href="https://github.com/databricks/spark-xml"
              xlink:type="simple"
              xlink:title="Source Code"/>
        <category>Repository</category>
        <rating>4.5</rating>
      </resource>
      <resource>
        <id>3</id>
        <title xml:lang="de">Spark Handbuch</title>
        <link xlink:href="https://example.com/spark-de"
              xlink:type="simple"
              xlink:title="German Guide"/>
        <category>Documentation</category>
        <rating>4.2</rating>
      </resource>
    </resources>
""")

# ── 4. Industry-standard namespaces (SOAP-like envelope) ─────────────────

SOAP_LIKE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:app="http://example.com/api/v1">
      <env:Body>
        <app:GetOrderResponse>
          <app:order>
            <app:orderId>ORD-1001</app:orderId>
            <app:customer>Alice Johnson</app:customer>
            <app:total>299.99</app:total>
            <app:status>shipped</app:status>
            <app:items>3</app:items>
          </app:order>
          <app:order>
            <app:orderId>ORD-1002</app:orderId>
            <app:customer>Bob Smith</app:customer>
            <app:total>149.50</app:total>
            <app:status>delivered</app:status>
            <app:items>1</app:items>
          </app:order>
          <app:order>
            <app:orderId>ORD-1003</app:orderId>
            <app:customer>Carol Lee</app:customer>
            <app:total>499.00</app:total>
            <app:status>processing</app:status>
            <app:items>5</app:items>
          </app:order>
        </app:GetOrderResponse>
      </env:Body>
    </env:Envelope>
""")


def write_xml(path: Path, content: str) -> None:
    """Write XML string to file, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "namespace_qualified_demo"

    qualified_path = base / "qualified.xml"
    collision_path = base / "collision.xml"
    attrs_path = base / "qualified_attrs.xml"
    soap_path = base / "soap_like.xml"

    write_xml(qualified_path, QUALIFIED_XML)
    write_xml(collision_path, COLLISION_XML)
    write_xml(attrs_path, QUALIFIED_ATTRS_XML)
    write_xml(soap_path, SOAP_LIKE_XML)

    spark = get_spark_session(
        app_name="spark-xml-namespace-qualified",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ==================================================================
    # 1. Fully-Qualified Elements — prefix:localName
    # ==================================================================
    print("=" * 70)
    print("1. Fully-Qualified Elements — prefix:localName")
    print("=" * 70)
    print(
        "Three namespaces qualify different concerns: doc: for structure,\n"
        "meta: for metadata, content: for body text.  Each element has a\n"
        "globally unique identity: {URI}localName.\n"
    )

    # Preserved mode — see the full qualified names
    df_qual_pres = (
        spark.read.format("xml")
        .option("rowTag", "doc:section")
        .option("ignoreNamespace", "false")
        .load(qualified_path.as_posix())
    )
    print("--- ignoreNamespace=false (qualified names visible) ---")
    df_qual_pres.printSchema()
    df_qual_pres.show(truncate=False)

    cols = df_qual_pres.columns
    doc_cols = sorted([c for c in cols if c.startswith("doc:")])
    meta_cols = sorted([c for c in cols if c.startswith("meta:")])
    content_cols = sorted([c for c in cols if c.startswith("content:")])
    print(f"  doc: columns     → {doc_cols}")
    print(f"  meta: columns    → {meta_cols}")
    print(f"  content: columns → {content_cols}")
    print("✓ Each namespace prefix groups related columns")

    # Ignored mode — clean names
    df_qual_clean = (
        spark.read.format("xml")
        .option("rowTag", "doc:section")
        .option("ignoreNamespace", "true")
        .load(qualified_path.as_posix())
    )
    print("\n--- ignoreNamespace=true (prefixes stripped) ---")
    df_qual_clean.printSchema()
    df_qual_clean.show(truncate=False)
    print(f"  Clean columns: {sorted(df_qual_clean.columns)}")

    # ==================================================================
    # 2. Name Collision — Same Local Name, Different Namespaces
    # ==================================================================
    print("\n" + "=" * 70)
    print("2. Name Collision — Why Qualified Names Matter")
    print("=" * 70)
    print(
        "hr:name (person), dept:name (department), proj:name (project)\n"
        "all have local name 'name' but represent different things.\n"
    )

    # Preserved — each "name" is disambiguated by its prefix
    df_coll_pres = (
        spark.read.format("xml")
        .option("rowTag", "hr:employee")
        .option("ignoreNamespace", "false")
        .load(collision_path.as_posix())
    )
    print("--- ignoreNamespace=false (names disambiguated) ---")
    df_coll_pres.printSchema()
    df_coll_pres.show(truncate=False)

    # Access all three "name" fields — they are distinct
    print("--- All three 'name' fields ---")
    df_coll_pres.selectExpr(
        "`hr:id` as emp_id",
        "`hr:name` as employee_name",
        "`dept:department`.`dept:name` as dept_name",
        "`proj:project`.`proj:name` as project_name",
    ).show(truncate=False)

    # Ignored mode — potential collision!
    df_coll_clean = (
        spark.read.format("xml")
        .option("rowTag", "hr:employee")
        .option("ignoreNamespace", "true")
        .load(collision_path.as_posix())
    )
    print("--- ignoreNamespace=true (potential collision) ---")
    df_coll_clean.printSchema()
    df_coll_clean.show(truncate=False)

    cols_clean = df_coll_clean.columns
    print(f"  Clean columns: {sorted(cols_clean)}")

    # With ignoreNamespace=true, "name" at the row level (hr:name) becomes "name",
    # while dept:name and proj:name are inside structs (department.name, project.name).
    # In this case, there's no collision because they are at different nesting levels.
    print(
        "\n  In this case no collision occurs because:\n"
        "    - hr:name → 'name' (top-level column)\n"
        "    - dept:name → 'department.name' (inside struct)\n"
        "    - proj:name → 'project.name' (inside struct)\n"
    )

    # Flatten to show all three "name" values
    print("--- Flattened: all 'name' fields accessible ---")
    df_coll_clean.selectExpr(
        "id as emp_id",
        "name as employee_name",
        "department.name as dept_name",
        "project.name as project_name",
        "title as job_title",
    ).show(truncate=False)
    print("✓ Struct nesting prevents collision even with ignoreNamespace=true")

    # ==================================================================
    # 3. Qualified Attributes — xlink:href, xml:lang
    # ==================================================================
    print("\n" + "=" * 70)
    print("3. Qualified Attributes — xlink:href, xml:lang")
    print("=" * 70)
    print(
        "Standard qualified attributes like xlink:href, xlink:type,\n"
        "and xml:lang are handled like any other namespaced attribute.\n"
    )

    # Preserved mode with attributePrefix
    df_attrs_pres = (
        spark.read.format("xml")
        .option("rowTag", "resource")
        .option("ignoreNamespace", "false")
        .option("attributePrefix", "attr_")
        .load(attrs_path.as_posix())
    )
    print("--- ignoreNamespace=false + attributePrefix='attr_' ---")
    df_attrs_pres.printSchema()
    df_attrs_pres.show(truncate=False)

    # Ignored mode — cleaner attribute names
    df_attrs_clean = (
        spark.read.format("xml")
        .option("rowTag", "resource")
        .option("ignoreNamespace", "true")
        .option("attributePrefix", "attr_")
        .load(attrs_path.as_posix())
    )
    print("--- ignoreNamespace=true + attributePrefix='attr_' ---")
    df_attrs_clean.printSchema()
    df_attrs_clean.show(truncate=False)

    # Flatten to show the qualified attributes
    print("--- Flattened resource links ---")
    df_attrs_clean.selectExpr(
        "id",
        "title",
        "title.attr_lang as language",
        "link.attr_href as url",
        "link.attr_type as link_type",
        "link.attr_title as link_title",
        "category",
        "CAST(rating AS DOUBLE) as rating",
    ).show(truncate=False)

    # ==================================================================
    # 4. Industry-Standard Namespaces (SOAP-like)
    # ==================================================================
    print("\n" + "=" * 70)
    print("4. Industry-Standard Namespaces (SOAP-like Envelope)")
    print("=" * 70)
    print(
        "SOAP, Atom, XHTML, SVG all use fully-qualified namespaces.\n"
        "This example shows a SOAP-like envelope with nested rowTag.\n"
    )

    # Read the orders inside the SOAP body
    df_soap = (
        spark.read.format("xml")
        .option("rowTag", "app:order")
        .option("ignoreNamespace", "true")
        .load(soap_path.as_posix())
    )
    df_soap.printSchema()
    df_soap.show(truncate=False)

    assert df_soap.count() == 3, "Expected 3 orders"
    print("✓ Extracted orders from SOAP-like envelope")
    print("  rowTag='app:order' reached through env:Envelope > env:Body")

    # Queries on extracted data
    print("\n--- Order summary ---")
    df_soap.selectExpr(
        "orderId as order_id",
        "customer",
        "CAST(total AS DOUBLE) as total",
        "status",
        "CAST(items AS INT) as item_count",
    ).show(truncate=False)

    # ==================================================================
    # 5. Qualified Elements with attributePrefix Control
    # ==================================================================
    print("\n" + "=" * 70)
    print("5. attributePrefix with Qualified Names")
    print("=" * 70)
    print(
        "The attributePrefix option controls how XML attributes appear\n"
        "in the schema.  With qualified attributes, the prefix interacts\n"
        "with the ignoreNamespace setting.\n"
    )

    # Default attributePrefix (_) — preserved mode
    df_default_prefix = (
        spark.read.format("xml")
        .option("rowTag", "resource")
        .option("ignoreNamespace", "false")
        .load(attrs_path.as_posix())
    )
    print("--- Default attributePrefix='_' + ignoreNamespace=false ---")
    print(f"  Columns: {sorted(df_default_prefix.columns)}")

    # Custom attributePrefix — ignored mode
    df_custom_prefix = (
        spark.read.format("xml")
        .option("rowTag", "resource")
        .option("ignoreNamespace", "true")
        .option("attributePrefix", "@")
        .load(attrs_path.as_posix())
    )
    print(f"\n--- attributePrefix='@' + ignoreNamespace=true ---")
    df_custom_prefix.printSchema()
    df_custom_prefix.show(truncate=False)
    print("✓ @ prefix clearly distinguishes attributes from elements")

    # ==================================================================
    # 6. Explicit Schema for Qualified Names
    # ==================================================================
    print("\n" + "=" * 70)
    print("6. Explicit Schema — Both Modes")
    print("=" * 70)

    # Schema for ignoreNamespace=true
    section_schema_clean = StructType([
        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("created", StringType(), True),
        StructField("author", StringType(), True),
        StructField("version", StringType(), True),
        StructField("body", StringType(), True),
        StructField("word_count", LongType(), True),
    ])

    df_explicit_clean = (
        spark.read.format("xml")
        .option("rowTag", "doc:section")
        .option("ignoreNamespace", "true")
        .schema(section_schema_clean)
        .load(qualified_path.as_posix())
    )
    print("--- ignoreNamespace=true + explicit schema ---")
    df_explicit_clean.printSchema()
    df_explicit_clean.show(truncate=False)
    assert df_explicit_clean.schema == section_schema_clean
    print("✓ Plain field names match stripped column names")

    # Schema for ignoreNamespace=false
    section_schema_pres = StructType([
        StructField("doc:id", LongType(), True),
        StructField("doc:title", StringType(), True),
        StructField("meta:created", StringType(), True),
        StructField("meta:author", StringType(), True),
        StructField("meta:version", StringType(), True),
        StructField("content:body", StringType(), True),
        StructField("content:word_count", LongType(), True),
    ])

    df_explicit_pres = (
        spark.read.format("xml")
        .option("rowTag", "doc:section")
        .option("ignoreNamespace", "false")
        .schema(section_schema_pres)
        .load(qualified_path.as_posix())
    )
    print("\n--- ignoreNamespace=false + explicit schema ---")
    df_explicit_pres.printSchema()
    df_explicit_pres.show(truncate=False)
    assert df_explicit_pres.schema == section_schema_pres
    print("✓ Prefixed field names match preserved column names")

    # ==================================================================
    # 7. Renaming Qualified Columns for Downstream Use
    # ==================================================================
    print("\n" + "=" * 70)
    print("7. Renaming Qualified Columns")
    print("=" * 70)

    def rename_qualified(df, separator="_"):
        """Replace namespace prefix colon with a separator."""
        for col_name in df.columns:
            if ":" in col_name:
                new_name = col_name.replace(":", separator)
                df = df.withColumnRenamed(col_name, new_name)
        return df

    df_renamed = rename_qualified(df_qual_pres)
    print("Strategy 1: Replace ':' with '_'")
    print(f"  Before: {sorted(df_qual_pres.columns)}")
    print(f"  After:  {sorted(df_renamed.columns)}")

    def strip_qualified(df):
        """Strip namespace prefix, keep local name only."""
        for col_name in df.columns:
            if ":" in col_name:
                local = col_name.split(":")[-1]
                df = df.withColumnRenamed(col_name, local)
        return df

    df_stripped = strip_qualified(df_qual_pres)
    print("\nStrategy 2: Strip prefix entirely")
    print(f"  Before: {sorted(df_qual_pres.columns)}")
    print(f"  After:  {sorted(df_stripped.columns)}")

    def prefix_to_domain(df, mapping):
        """Rename using a domain-specific mapping."""
        for col_name in df.columns:
            if col_name in mapping:
                df = df.withColumnRenamed(col_name, mapping[col_name])
        return df

    domain_mapping = {
        "doc:id": "section_id",
        "doc:title": "section_title",
        "meta:created": "created_date",
        "meta:author": "author_name",
        "meta:version": "doc_version",
        "content:body": "body_text",
        "content:word_count": "word_count",
    }
    df_domain = prefix_to_domain(df_qual_pres, domain_mapping)
    print("\nStrategy 3: Domain-specific mapping")
    print(f"  Before: {sorted(df_qual_pres.columns)}")
    print(f"  After:  {sorted(df_domain.columns)}")
    df_domain.show(truncate=False)
    print("✓ All three rename strategies produce clean column names")

    # ==================================================================
    # 8. Write Round-Trip
    # ==================================================================
    print("\n" + "=" * 70)
    print("8. Write Round-Trip")
    print("=" * 70)
    print(
        "When writing a DataFrame, column names become element names.\n"
        "If columns still have prefixes (ignoreNamespace=false reading),\n"
        "the output elements will contain colons but NO xmlns declarations.\n"
    )

    # Write clean columns
    output_clean = base / "output_clean"
    (
        df_qual_clean.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "document")
        .option("rowTag", "section")
        .save(output_clean.as_posix())
    )

    df_rt_clean = (
        spark.read.format("xml")
        .option("rowTag", "section")
        .load(output_clean.as_posix())
    )
    print("--- Written from ignoreNamespace=true reading ---")
    df_rt_clean.show(truncate=False)
    assert df_rt_clean.count() == 3
    print("✓ Clean element names in output (no prefixes)")

    # Write domain-mapped columns
    output_domain = base / "output_domain"
    (
        df_domain.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "document")
        .option("rowTag", "section")
        .save(output_domain.as_posix())
    )

    df_rt_domain = (
        spark.read.format("xml")
        .option("rowTag", "section")
        .load(output_domain.as_posix())
    )
    print("\n--- Written from domain-mapped columns ---")
    df_rt_domain.show(truncate=False)
    print(f"  Columns: {sorted(df_rt_domain.columns)}")
    print("✓ Domain-mapped names become clean XML element names")

    # ==================================================================
    # Summary
    # ==================================================================
    print("\n" + "=" * 70)
    print("Summary: Fully-Qualified Namespace Handling")
    print("=" * 70)
    print(
        "  Scenario                        Recommendation\n"
        "  ──────────────────────────────   ──────────────────────────────────\n"
        "  No name collisions              ignoreNamespace=true (simplest)\n"
        "  Same local names across NS      ignoreNamespace=false + rename\n"
        "  Qualified attributes            attributePrefix + ignoreNamespace\n"
        "  Industry XML (SOAP, Atom)       ignoreNamespace=true + rowTag prefix\n"
        "  Explicit schema                 Field names must match column mode\n"
        "  Writing output                  Rename to clean names first\n"
        "\n"
        "  spark-xml works with prefix:localName strings.\n"
        "  It does NOT track namespace URIs — two prefixes mapping to the\n"
        "  same URI are treated as different column name prefixes.\n"
    )

    spark.stop()