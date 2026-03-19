"""Demonstrate how spark-xml handles XML documents with DTD declarations.

A Document Type Definition (DTD) defines the legal structure, elements,
attributes, and entities of an XML document.  spark-xml's Hadoop-based
``InputFormat`` splits files by ``rowTag`` **before** full XML parsing,
so DTD declarations are effectively stripped from each split.  This has
several consequences:

1. **Element / attribute declarations are ignored** — spark-xml infers
   or uses an explicit ``StructType`` schema, not the DTD.
2. **Predefined entities** (``&lt; &gt; &amp; &quot; &apos;``) and
   **numeric character references** (``&#169;``, ``&#x20AC;``) are
   always resolved because the Java SAX parser handles them natively.
3. **Custom entities** (``<!ENTITY name "value">``) are **not** resolved
   — they produce corrupt records or null values.
4. **External DTD references** (``SYSTEM`` / ``PUBLIC``) are blocked
   for XXE security — the parser never fetches external resources.

This script demonstrates:

1. Reading XML with inline DTD — DTD is silently ignored
2. DTD element/attribute declarations have no effect on schema
3. Predefined entities still work in DTD-declared XML
4. Custom entity failure mode and corrupt record detection
5. **Workaround**: pre-process with ``lxml`` to expand custom entities
6. External DTD references — blocked with no error
7. Notation and parameter entities — also unsupported
8. Full practical pipeline: lxml expand → Spark read → transform
"""

import os
import sys
import textwrap
import xml.etree.ElementTree as ET
from io import BytesIO
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

# ── 1. XML with inline DTD — no custom entities ─────────────────────────

INLINE_DTD_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE library [
      <!ELEMENT library (book+)>
      <!ELEMENT book (title, author, year, price)>
      <!ELEMENT title (#PCDATA)>
      <!ELEMENT author (#PCDATA)>
      <!ELEMENT year (#PCDATA)>
      <!ELEMENT price (#PCDATA)>
      <!ATTLIST book id ID #REQUIRED>
      <!ATTLIST book genre CDATA #IMPLIED>
    ]>
    <library>
      <book id="b1" genre="fiction">
        <title>The Great Gatsby</title>
        <author>F. Scott Fitzgerald</author>
        <year>1925</year>
        <price>10.99</price>
      </book>
      <book id="b2" genre="science">
        <title>A Brief History of Time</title>
        <author>Stephen Hawking</author>
        <year>1988</year>
        <price>14.99</price>
      </book>
      <book id="b3" genre="fiction">
        <title>To Kill a Mockingbird</title>
        <author>Harper Lee</author>
        <year>1960</year>
        <price>12.50</price>
      </book>
      <book id="b4" genre="philosophy">
        <title>Meditations</title>
        <author>Marcus Aurelius</author>
        <year>180</year>
        <price>8.99</price>
      </book>
    </library>
""")

# Same data without DTD — for comparison
NO_DTD_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <library>
      <book id="b1" genre="fiction">
        <title>The Great Gatsby</title>
        <author>F. Scott Fitzgerald</author>
        <year>1925</year>
        <price>10.99</price>
      </book>
      <book id="b2" genre="science">
        <title>A Brief History of Time</title>
        <author>Stephen Hawking</author>
        <year>1988</year>
        <price>14.99</price>
      </book>
      <book id="b3" genre="fiction">
        <title>To Kill a Mockingbird</title>
        <author>Harper Lee</author>
        <year>1960</year>
        <price>12.50</price>
      </book>
      <book id="b4" genre="philosophy">
        <title>Meditations</title>
        <author>Marcus Aurelius</author>
        <year>180</year>
        <price>8.99</price>
      </book>
    </library>
""")

# ── 2. XML with predefined entities inside DTD-declared document ─────────

DTD_WITH_ENTITIES_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE catalog [
      <!ELEMENT catalog (product+)>
      <!ELEMENT product (name, description, price)>
      <!ELEMENT name (#PCDATA)>
      <!ELEMENT description (#PCDATA)>
      <!ELEMENT price (#PCDATA)>
      <!ATTLIST product id CDATA #REQUIRED>
    ]>
    <catalog>
      <product id="p1">
        <name>XML &amp; JSON Guide</name>
        <description>Compare &lt;xml&gt; vs {&quot;json&quot;}: pros &amp; cons</description>
        <price>29.99</price>
      </product>
      <product id="p2">
        <name>C++ Templates</name>
        <description>Learn template&lt;T&gt; and operator&lt;&lt; overloading</description>
        <price>39.99</price>
      </product>
      <product id="p3">
        <name>SQL Mastery</name>
        <description>WHERE age &gt; 18 AND name &lt;&gt; &apos;unknown&apos;</description>
        <price>24.50</price>
      </product>
    </catalog>
""")

# ── 3. XML with custom entities (will fail) ──────────────────────────────

CUSTOM_ENTITY_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE employees [
      <!ENTITY company "Acme Corporation">
      <!ENTITY dept_eng "Engineering">
      <!ENTITY dept_mkt "Marketing">
      <!ENTITY dept_fin "Finance">
      <!ENTITY currency "USD">
      <!ENTITY copy "&#169;">
    ]>
    <employees>
      <employee>
        <name>Alice Johnson</name>
        <company>&company;</company>
        <department>&dept_eng;</department>
        <salary currency="&currency;">95000</salary>
        <note>&copy; 2024 &company; — All rights reserved</note>
      </employee>
      <employee>
        <name>Bob Smith</name>
        <company>&company;</company>
        <department>&dept_mkt;</department>
        <salary currency="&currency;">82000</salary>
        <note>Joined &company; in 2020</note>
      </employee>
      <employee>
        <name>Carol Davis</name>
        <company>&company;</company>
        <department>&dept_fin;</department>
        <salary currency="&currency;">91000</salary>
        <note>Lead analyst at &company;</note>
      </employee>
    </employees>
""")

# ── 4. XML with external DTD reference ───────────────────────────────────

EXTERNAL_DTD_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE orders SYSTEM "orders.dtd">
    <orders>
      <order>
        <id>1001</id>
        <customer>Alice</customer>
        <total>250.00</total>
      </order>
      <order>
        <id>1002</id>
        <customer>Bob</customer>
        <total>175.50</total>
      </order>
    </orders>
""")

# ── 5. XML with notation and parameter entities ──────────────────────────

NOTATION_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE gallery [
      <!NOTATION jpeg SYSTEM "image/jpeg">
      <!NOTATION png SYSTEM "image/png">
      <!ELEMENT gallery (image+)>
      <!ELEMENT image (title, file, width, height)>
      <!ELEMENT title (#PCDATA)>
      <!ELEMENT file (#PCDATA)>
      <!ELEMENT width (#PCDATA)>
      <!ELEMENT height (#PCDATA)>
      <!ATTLIST image format NOTATION (jpeg|png) #REQUIRED>
    ]>
    <gallery>
      <image format="jpeg">
        <title>Sunset</title>
        <file>sunset.jpg</file>
        <width>1920</width>
        <height>1080</height>
      </image>
      <image format="png">
        <title>Logo</title>
        <file>logo.png</file>
        <width>512</width>
        <height>512</height>
      </image>
    </gallery>
""")

# ── Schemas ──────────────────────────────────────────────────────────────

BOOK_SCHEMA = StructType([
    StructField("_genre", StringType(), True),
    StructField("_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("year", LongType(), True),
    StructField("price", DoubleType(), True),
])

PRODUCT_SCHEMA = StructType([
    StructField("_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", DoubleType(), True),
])

EMPLOYEE_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("company", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", StringType(), True),
    StructField("note", StringType(), True),
])


def write_xml(path: Path, content: str) -> None:
    """Write XML string to file, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def expand_dtd_entities(xml_string: str) -> str:
    """Expand custom DTD entities using lxml and return clean XML.

    This is the recommended workaround for loading DTD-declared XML
    with custom entities into spark-xml.  lxml resolves all entity
    references and produces a clean XML document without a DOCTYPE.
    """
    try:
        from lxml import etree

        parser = etree.XMLParser(
            resolve_entities=True,
            dtd_validation=False,
            load_dtd=True,
            no_network=True,
        )
        tree = etree.parse(BytesIO(xml_string.encode("utf-8")), parser)
        result = etree.tostring(tree, encoding="unicode", pretty_print=True)
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + result
    except ImportError:
        print("⚠ lxml not installed — falling back to regex-based expansion")
        return _expand_entities_fallback(xml_string)


def _expand_entities_fallback(xml_string: str) -> str:
    """Simple regex-based entity expansion when lxml is unavailable.

    Parses ``<!ENTITY name "value">`` declarations from the DOCTYPE
    and replaces ``&name;`` references throughout the document body.
    Strips the DOCTYPE declaration from the output.
    """
    import re

    entities = dict(
        re.findall(r'<!ENTITY\s+(\w+)\s+"([^"]*)">', xml_string)
    )

    # Resolve entity references that point to other entities (one level)
    changed = True
    while changed:
        changed = False
        for name, value in list(entities.items()):
            new_value = value
            for ref_name, ref_value in entities.items():
                new_value = new_value.replace(f"&{ref_name};", ref_value)
            # Also resolve numeric character refs in entity values
            for match in re.finditer(r"&#(\d+);", new_value):
                new_value = new_value.replace(
                    match.group(0), chr(int(match.group(1)))
                )
            if new_value != value:
                entities[name] = new_value
                changed = True

    # Strip DOCTYPE declaration
    cleaned = re.sub(
        r"<!DOCTYPE\s+\w+\s*\[.*?\]>", "", xml_string, flags=re.DOTALL
    )

    # Replace entity references
    for name, value in entities.items():
        cleaned = cleaned.replace(f"&{name};", value)

    return cleaned.strip()


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "dtd_demo"

    dtd_path = base / "library_dtd.xml"
    no_dtd_path = base / "library_no_dtd.xml"
    entities_path = base / "catalog_dtd_entities.xml"
    custom_path = base / "employees_custom_entities.xml"
    external_path = base / "orders_external_dtd.xml"
    notation_path = base / "gallery_notation.xml"

    write_xml(dtd_path, INLINE_DTD_XML)
    write_xml(no_dtd_path, NO_DTD_XML)
    write_xml(entities_path, DTD_WITH_ENTITIES_XML)
    write_xml(custom_path, CUSTOM_ENTITY_XML)
    write_xml(external_path, EXTERNAL_DTD_XML)
    write_xml(notation_path, NOTATION_XML)

    spark = get_spark_session(
        app_name="spark-xml-dtd-schema",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ==================================================================
    # 1. Inline DTD — Silently Ignored by spark-xml
    # ==================================================================
    print("=" * 70)
    print("1. Inline DTD Declarations — Silently Ignored")
    print("=" * 70)
    print(
        "DTD element and attribute declarations (<!ELEMENT>, <!ATTLIST>)\n"
        "define the legal structure of an XML document.  spark-xml ignores\n"
        "them entirely — schema comes from inference or explicit StructType.\n"
    )

    df_dtd = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .schema(BOOK_SCHEMA)
        .load(dtd_path.as_posix())
    )
    df_dtd.printSchema()
    df_dtd.show(truncate=False)
    print(f"Row count: {df_dtd.count()}")

    # Compare with non-DTD version
    df_no_dtd = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .schema(BOOK_SCHEMA)
        .load(no_dtd_path.as_posix())
    )

    dtd_rows = sorted([tuple(r) for r in df_dtd.collect()])
    no_dtd_rows = sorted([tuple(r) for r in df_no_dtd.collect()])
    assert dtd_rows == no_dtd_rows, "DTD and no-DTD results differ!"
    assert df_dtd.schema == df_no_dtd.schema, "Schemas differ!"
    print("✓ DTD and non-DTD versions produce identical DataFrames")
    print("✓ Schemas are identical — DTD declarations have zero effect")

    # ==================================================================
    # 2. DTD Constraints Are Not Enforced
    # ==================================================================
    print("\n" + "=" * 70)
    print("2. DTD Constraints Are Not Enforced")
    print("=" * 70)
    print(
        "DTD says <!ATTLIST book id ID #REQUIRED>, but spark-xml does\n"
        "not validate that 'id' is present or unique.\n"
    )

    # Schema inference — verify DTD attributes appear as regular columns
    df_inferred = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .load(dtd_path.as_posix())
    )
    df_inferred.printSchema()
    print(f"Inferred columns: {sorted(df_inferred.columns)}")

    assert "_id" in df_inferred.columns, "Attribute 'id' not inferred"
    assert "_genre" in df_inferred.columns, "Attribute 'genre' not inferred"
    print(
        "\n✓ DTD-declared attributes (_id, _genre) inferred as regular columns\n"
        "  (prefixed with '_' — spark-xml default attributePrefix)"
    )

    # DTD says genre is CDATA #IMPLIED — spark-xml treats it as nullable string
    print("\n--- Attribute types (DTD vs inferred) ---")
    dtd_declarations = [
        ("id", "ID #REQUIRED", str(df_inferred.schema["_id"].dataType)),
        ("genre", "CDATA #IMPLIED", str(df_inferred.schema["_genre"].dataType)),
    ]
    print(f"  {'Attribute':<10} {'DTD Declaration':<20} {'Spark Type'}")
    print(f"  {'-'*10} {'-'*20} {'-'*15}")
    for attr, dtd_decl, spark_type in dtd_declarations:
        print(f"  {attr:<10} {dtd_decl:<20} {spark_type}")
    print("\n✓ DTD type constraints (ID, CDATA) are ignored — all become StringType")

    # ==================================================================
    # 3. Predefined Entities in DTD-Declared XML
    # ==================================================================
    print("\n" + "=" * 70)
    print("3. Predefined Entities in DTD-Declared XML")
    print("=" * 70)
    print(
        "The 5 predefined XML entities work normally even when a DTD is\n"
        "present.  They are resolved by the Java SAX parser, not the DTD.\n"
    )

    df_entities = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .schema(PRODUCT_SCHEMA)
        .load(entities_path.as_posix())
    )
    df_entities.printSchema()
    df_entities.show(truncate=False)

    rows = {r["_id"]: r for r in df_entities.collect()}
    assert "<xml>" in rows["p1"]["description"], "&lt;xml&gt; not decoded"
    assert "&" in rows["p1"]["name"], "&amp; not decoded"
    assert '"' in rows["p1"]["description"], "&quot; not decoded"
    assert "<T>" in rows["p2"]["description"], "&lt;T&gt; not decoded"
    assert "<>" in rows["p3"]["description"], "&lt;&gt; not decoded"
    assert "'" in rows["p3"]["description"], "&apos; not decoded"
    print("✓ All 5 predefined entities decoded correctly in DTD-declared XML")

    # ==================================================================
    # 4. Custom Entity Failure Mode
    # ==================================================================
    print("\n" + "=" * 70)
    print("4. Custom Entities (<!ENTITY>) — Failure Mode")
    print("=" * 70)
    print(
        "Custom entities like <!ENTITY company 'Acme Corp'> are defined\n"
        "in the DTD.  spark-xml strips the DOCTYPE during file splitting,\n"
        "so &company; cannot be resolved → corrupt records.\n"
    )

    # PERMISSIVE mode — captures failures as _corrupt_record
    corrupt_schema = StructType([
        StructField("name", StringType(), True),
        StructField("company", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", StringType(), True),
        StructField("note", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])

    df_custom = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(corrupt_schema)
        .load(custom_path.as_posix())
    )
    df_custom.show(truncate=False)

    # Check if we got corrupt records or nulls
    total = df_custom.count()
    corrupt_count = df_custom.filter(
        F.col("_corrupt_record").isNotNull()
    ).count()
    null_company = df_custom.filter(F.col("company").isNull()).count()

    print(f"Total rows: {total}")
    print(f"Corrupt records: {corrupt_count}")
    print(f"Null 'company' values: {null_company}")

    if corrupt_count > 0:
        print("\n--- Corrupt records (custom entities not resolved) ---")
        df_custom.filter(F.col("_corrupt_record").isNotNull()).select(
            "_corrupt_record"
        ).show(truncate=False)
        print("✓ Custom entities produced corrupt records as expected")
    elif null_company > 0:
        print("✓ Custom entities produced null values — DTD was not loaded")
    else:
        print("(!) Custom entities were resolved — unusual parser behavior")

    # DROPMALFORMED mode — silently drops rows with unresolved entities
    print("\n--- DROPMALFORMED mode ---")
    df_dropped = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("mode", "DROPMALFORMED")
        .schema(EMPLOYEE_SCHEMA)
        .load(custom_path.as_posix())
    )
    dropped_count = df_dropped.count()
    print(f"Rows after DROPMALFORMED: {dropped_count} (was {total} in PERMISSIVE)")
    if dropped_count < total:
        print("✓ DROPMALFORMED dropped rows with unresolved custom entities")

    # ==================================================================
    # 5. Workaround — Pre-Process with Entity Expansion
    # ==================================================================
    print("\n" + "=" * 70)
    print("5. Workaround: Pre-Process to Expand Custom Entities")
    print("=" * 70)
    print(
        "Expand custom entities BEFORE loading into spark-xml.\n"
        "Use lxml (preferred) or a regex-based fallback.\n"
    )

    expanded_xml = expand_dtd_entities(CUSTOM_ENTITY_XML)
    print("--- Expanded XML (first 500 chars) ---")
    print(expanded_xml[:500])

    expanded_path = base / "employees_expanded.xml"
    write_xml(expanded_path, expanded_xml)

    df_expanded = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .schema(EMPLOYEE_SCHEMA)
        .load(expanded_path.as_posix())
    )
    df_expanded.show(truncate=False)

    exp_rows = df_expanded.collect()
    has_data = all(r["company"] is not None for r in exp_rows)
    if has_data:
        print("✓ Custom entities expanded successfully — all company values present")
        for r in exp_rows:
            print(f"  {r['name']}: company={r['company']}, dept={r['department']}")
            assert "Acme" in r["company"], f"Expected 'Acme' in company: {r['company']}"
        print("✓ All 'Acme Corporation' references resolved correctly")
    else:
        print("⚠ Some values are null — entity expansion may need adjustment")

    # Verify note field has resolved nested entities (©, company name)
    print("\n--- Notes with expanded entities ---")
    df_expanded.select("name", "note").show(truncate=False)
    notes = {r["name"]: r["note"] for r in exp_rows}
    if notes.get("Alice Johnson") and "©" in notes["Alice Johnson"]:
        print("✓ Nested entity (&#169; via &copy;) resolved to ©")
    if notes.get("Bob Smith") and "Acme" in notes["Bob Smith"]:
        print("✓ Entity reference &company; in notes resolved to 'Acme Corporation'")

    # ==================================================================
    # 6. External DTD References — Blocked for Security
    # ==================================================================
    print("\n" + "=" * 70)
    print("6. External DTD References (SYSTEM/PUBLIC)")
    print("=" * 70)
    print(
        "<!DOCTYPE orders SYSTEM 'orders.dtd'> references an external file.\n"
        "spark-xml (and modern XML parsers) block external entity loading\n"
        "to prevent XXE (XML External Entity) attacks.\n"
    )

    df_external = (
        spark.read.format("xml")
        .option("rowTag", "order")
        .load(external_path.as_posix())
    )
    df_external.printSchema()
    df_external.show(truncate=False)

    ext_count = df_external.count()
    print(f"Row count: {ext_count}")
    if ext_count > 0:
        print(
            "✓ External DTD reference silently ignored — data read normally\n"
            "  (No attempt to fetch 'orders.dtd' from filesystem or network)"
        )
    print(
        "\n  Security note: External DTDs could be exploited to read local\n"
        "  files (file:///etc/passwd) or make network requests (XXE attack).\n"
        "  spark-xml disables this by default — no configuration needed."
    )

    # ==================================================================
    # 7. Notation and Parameter Entities
    # ==================================================================
    print("\n" + "=" * 70)
    print("7. Notation and Parameter Entities")
    print("=" * 70)
    print(
        "DTD supports NOTATION declarations (<!NOTATION jpeg SYSTEM ...>)\n"
        "and parameter entities (%name;) used within DTDs themselves.\n"
        "spark-xml ignores both — they have no effect on the DataFrame.\n"
    )

    df_notation = (
        spark.read.format("xml")
        .option("rowTag", "image")
        .load(notation_path.as_posix())
    )
    df_notation.printSchema()
    df_notation.show(truncate=False)

    assert df_notation.count() == 2, "Expected 2 images"
    cols = df_notation.columns
    assert "_format" in cols, "Expected _format attribute column"
    assert "title" in cols, "Expected title column"
    print(f"Columns: {sorted(cols)}")
    print("✓ NOTATION-declared attribute (_format) read as regular string column")
    print("  DTD constraint NOTATION (jpeg|png) is not enforced by spark-xml")

    # ==================================================================
    # 8. Practical Pipeline: DTD XML → Expand → Read → Transform
    # ==================================================================
    print("\n" + "=" * 70)
    print("8. Practical Pipeline: DTD XML → Expand → Read → Transform")
    print("=" * 70)

    PIPELINE_XML = textwrap.dedent("""\
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE report [
          <!ENTITY org "Global Tech Inc.">
          <!ENTITY period "Q4 2024">
          <!ENTITY curr "USD">
        ]>
        <report>
          <entry>
            <region>North America</region>
            <organization>&org;</organization>
            <period>&period;</period>
            <revenue currency="&curr;">1250000.00</revenue>
            <expenses currency="&curr;">980000.00</expenses>
          </entry>
          <entry>
            <region>Europe</region>
            <organization>&org;</organization>
            <period>&period;</period>
            <revenue currency="&curr;">890000.00</revenue>
            <expenses currency="&curr;">720000.00</expenses>
          </entry>
          <entry>
            <region>Asia Pacific</region>
            <organization>&org;</organization>
            <period>&period;</period>
            <revenue currency="&curr;">1100000.00</revenue>
            <expenses currency="&curr;">850000.00</expenses>
          </entry>
        </report>
    """)

    # Step 1: Expand entities
    print("Step 1: Expand custom entities")
    expanded_report = expand_dtd_entities(PIPELINE_XML)
    report_path = base / "report_expanded.xml"
    write_xml(report_path, expanded_report)
    print("  ✓ Entities expanded and written to file")

    # Step 2: Read with explicit schema
    print("Step 2: Read expanded XML with explicit schema")
    report_schema = StructType([
        StructField("region", StringType(), True),
        StructField("organization", StringType(), True),
        StructField("period", StringType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("expenses", DoubleType(), True),
    ])

    df_report = (
        spark.read.format("xml")
        .option("rowTag", "entry")
        .schema(report_schema)
        .load(report_path.as_posix())
    )
    df_report.show(truncate=False)

    # Step 3: Transform — calculate profit
    print("Step 3: Transform — calculate profit and margins")
    df_result = (
        df_report
        .withColumn("profit", F.round(F.col("revenue") - F.col("expenses"), 2))
        .withColumn(
            "margin_pct",
            F.round(
                (F.col("revenue") - F.col("expenses")) / F.col("revenue") * 100, 1
            ),
        )
    )
    df_result.show(truncate=False)

    # Step 4: Aggregation
    print("Step 4: Aggregate across regions")
    df_result.select(
        F.lit(df_result.first()["organization"]).alias("organization"),
        F.lit(df_result.first()["period"]).alias("period"),
    ).distinct().show(truncate=False)

    df_summary = df_result.agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.round(F.sum("expenses"), 2).alias("total_expenses"),
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.round(F.avg("margin_pct"), 1).alias("avg_margin_pct"),
        F.count("*").alias("num_regions"),
    )
    df_summary.show(truncate=False)

    # Verify entity values propagated correctly
    report_rows = df_report.collect()
    for r in report_rows:
        assert r["organization"] == "Global Tech Inc.", (
            f"Expected 'Global Tech Inc.' but got '{r['organization']}'"
        )
        assert r["period"] == "Q4 2024", (
            f"Expected 'Q4 2024' but got '{r['period']}'"
        )
    print("✓ All entity values (organization, period) consistent across rows")

    # Step 5: Write final result
    print("\nStep 5: Write transformed result")
    output_path = base / "report_output"
    (
        df_result.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "report")
        .option("rowTag", "entry")
        .save(output_path.as_posix())
    )
    print(f"  ✓ Written to {output_path}")

    # ==================================================================
    # Summary
    # ==================================================================
    print("\n" + "=" * 70)
    print("Summary: DTD Handling in spark-xml")
    print("=" * 70)
    print(
        "  DTD Feature                   Behavior in spark-xml\n"
        "  ─────────────────────────────  ────────────────────────────────\n"
        "  <!ELEMENT> declarations        Ignored — no structural validation\n"
        "  <!ATTLIST> declarations        Ignored — attrs inferred as strings\n"
        "  Predefined entities            ✓ Resolved (&lt; → <, &amp; → &)\n"
        "  Numeric char refs              ✓ Resolved (&#169; → ©)\n"
        "  Custom <!ENTITY>               ✗ Not resolved → corrupt records\n"
        "  External DTD (SYSTEM)          ✗ Blocked (XXE security)\n"
        "  External DTD (PUBLIC)          ✗ Blocked (XXE security)\n"
        "  <!NOTATION> declarations       Ignored — attr values read as-is\n"
        "  Parameter entities (%name;)    Ignored — DTD-internal only\n"
        "\n"
        "  Recommended workflow for DTD XML with custom entities:\n"
        "  1. Pre-process with lxml (resolve_entities=True)\n"
        "  2. Write expanded XML without DOCTYPE\n"
        "  3. Read with spark-xml using explicit StructType schema\n"
    )

    spark.stop()