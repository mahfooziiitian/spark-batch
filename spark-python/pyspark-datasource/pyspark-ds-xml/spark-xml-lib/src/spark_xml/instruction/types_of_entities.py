"""Demonstrate how spark-xml handles all types of XML entities.

XML defines several entity types that represent special characters,
symbols, or text substitutions.  The underlying Java XML parser resolves
most entities before spark-xml ever sees the data, so DataFrame values
contain the **decoded** characters.  This script covers:

1. **Built-in (predefined) entities** — ``&lt; &gt; &amp; &quot; &apos;``
2. **Numeric character references** — decimal ``&#169;`` and hex ``&#x20AC;``
3. **Unicode symbols** via character references — ©, ™, €, §, etc.
4. **Entities inside XML attributes** — resolved in attribute values too
5. **Write round-trip** — Spark re-escapes ``<``, ``>``, ``&`` in output
6. **Queries on decoded values** — filter / search on the resolved text
7. **Mixed entities in a single element** — multiple entity types together
8. **Caveat: custom / DTD-defined entities** — not supported by spark-xml
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

# ── XML with built-in predefined entities ────────────────────────────────

BUILTIN_ENTITIES_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <messages>
      <message>
        <id>1</id>
        <sender>Alice</sender>
        <subject>Math Formula</subject>
        <body>The condition is: x &lt; 10 &amp;&amp; y &gt; 5</body>
        <note>Use &quot;double quotes&quot; or &apos;single quotes&apos;</note>
        <priority>1</priority>
      </message>
      <message>
        <id>2</id>
        <sender>Bob</sender>
        <subject>HTML Snippet</subject>
        <body>&lt;div class=&quot;container&quot;&gt;Hello &amp; welcome!&lt;/div&gt;</body>
        <note>Tags are escaped with &lt; and &gt;</note>
        <priority>2</priority>
      </message>
      <message>
        <id>3</id>
        <sender>Carol</sender>
        <subject>Legal Notice</subject>
        <body>Terms &amp; Conditions: price &lt; $100 &amp; discount &gt; 10%</body>
        <note>The &amp; entity represents a literal ampersand</note>
        <priority>1</priority>
      </message>
    </messages>
""")

# ── XML with numeric character references (decimal + hex) ────────────────

NUMERIC_ENTITIES_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <products>
      <product>
        <id>101</id>
        <name>Premium Widget</name>
        <description>&#169; 2024 Acme Corp &#8212; All rights reserved</description>
        <price>29.99</price>
        <currency_symbol>&#x24;</currency_symbol>
        <category>Widgets</category>
      </product>
      <product>
        <id>102</id>
        <name>Euro Gadget</name>
        <description>Price in &#x20AC; &#8211; tax included (&#xAE; registered)</description>
        <price>49.50</price>
        <currency_symbol>&#x20AC;</currency_symbol>
        <category>Gadgets</category>
      </product>
      <product>
        <id>103</id>
        <name>Trademark Tool&#8482;</name>
        <description>&#167; Section 42 &#8226; Bullet point &#183; Middle dot</description>
        <price>15.00</price>
        <currency_symbol>&#xA3;</currency_symbol>
        <category>Tools</category>
      </product>
      <product>
        <id>104</id>
        <name>Japanese Toy &#x304A;&#x3082;&#x3061;&#x3083;</name>
        <description>&#x2605; 5-star rated &#x2764; customer favorite</description>
        <price>12.75</price>
        <currency_symbol>&#xA5;</currency_symbol>
        <category>Toys</category>
      </product>
    </products>
""")

# ── XML with entities in attributes ──────────────────────────────────────

ATTRIBUTE_ENTITIES_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <catalog>
      <item id="1" label="Books &amp; Media" status="x &lt; 5">
        <name>XML &amp; JSON Guide</name>
        <price>39.99</price>
      </item>
      <item id="2" label="Electronics &#x2022; Cables" status="stock &gt; 100">
        <name>USB&#8211;C Adapter</name>
        <price>24.99</price>
      </item>
      <item id="3" label="Tools &amp; Hardware" status="rating &#x2265; 4.5">
        <name>Wrench Set (&#xBC;&quot; to 1&quot;)</name>
        <price>59.99</price>
      </item>
    </catalog>
""")

# ── XML with mixed entity types in single elements ───────────────────────

MIXED_ENTITIES_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <records>
      <record>
        <id>1</id>
        <text>&#169; 2024 &amp; &#x2122; brand &lt;em&gt;bold&lt;/em&gt;</text>
        <formula>x&#xB2; + y&#xB2; &lt; r&#xB2;</formula>
        <score>95.5</score>
      </record>
      <record>
        <id>2</id>
        <text>Price: &#x20AC;50 &amp; &#xA3;42 &#8212; 10% off!</text>
        <formula>a &gt; 0 &amp;&amp; b &#x2264; 100</formula>
        <score>87.0</score>
      </record>
      <record>
        <id>3</id>
        <text>&#x2605;&#x2605;&#x2605; &quot;Best in class&quot; &#x2014; Review</text>
        <formula>&#x3C0; &#x2248; 3.14159 &amp; e &#x2248; 2.71828</formula>
        <score>92.3</score>
      </record>
    </records>
""")

# ── Schemas ──────────────────────────────────────────────────────────────

MESSAGE_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("sender", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("body", StringType(), True),
    StructField("note", StringType(), True),
    StructField("priority", LongType(), True),
])

PRODUCT_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("currency_symbol", StringType(), True),
    StructField("category", StringType(), True),
])

RECORD_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("text", StringType(), True),
    StructField("formula", StringType(), True),
    StructField("score", DoubleType(), True),
])


def write_xml(path: Path, content: str) -> None:
    """Write XML string to file, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "entities_demo"

    builtin_path = base / "builtin_entities.xml"
    numeric_path = base / "numeric_entities.xml"
    attr_path = base / "attribute_entities.xml"
    mixed_path = base / "mixed_entities.xml"

    write_xml(builtin_path, BUILTIN_ENTITIES_XML)
    write_xml(numeric_path, NUMERIC_ENTITIES_XML)
    write_xml(attr_path, ATTRIBUTE_ENTITIES_XML)
    write_xml(mixed_path, MIXED_ENTITIES_XML)

    spark = get_spark_session(
        app_name="spark-xml-entity-types",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ==================================================================
    # 1. Built-in (Predefined) XML Entities
    # ==================================================================
    print("=" * 70)
    print("1. Built-in Predefined Entities: &lt; &gt; &amp; &quot; &apos;")
    print("=" * 70)
    print(
        "The 5 predefined XML entities are resolved by the parser into\n"
        "their literal characters BEFORE spark-xml builds the DataFrame.\n"
    )

    df_builtin = (
        spark.read.format("xml")
        .option("rowTag", "message")
        .schema(MESSAGE_SCHEMA)
        .load(builtin_path.as_posix())
    )
    df_builtin.printSchema()
    df_builtin.show(truncate=False)

    print("--- Decoded entity values ---")
    for row in df_builtin.collect():
        print(f"  id={row['id']}  body={row['body']}")
        print(f"           note={row['note']}")

    # Verify decoded characters
    rows = {r["id"]: r for r in df_builtin.collect()}
    assert "<" in rows[1]["body"] and ">" in rows[1]["body"], "< > not decoded"
    assert "&&" in rows[1]["body"], "& not decoded"
    assert '"' in rows[1]["note"], "quote not decoded"
    assert "'" in rows[1]["note"], "apos not decoded"
    assert "<div" in rows[2]["body"], "HTML tags not decoded"
    assert "& " in rows[3]["body"], "ampersand not decoded"
    print("\n✓ All 5 predefined entities correctly decoded")

    # ==================================================================
    # 2. Numeric Character References (Decimal & Hex)
    # ==================================================================
    print("\n" + "=" * 70)
    print("2. Numeric Character References — &#NNN; and &#xHHH;")
    print("=" * 70)
    print(
        "Decimal (&#169; → ©) and hex (&#x20AC; → €) references resolve\n"
        "to their Unicode characters.\n"
    )

    df_numeric = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .schema(PRODUCT_SCHEMA)
        .load(numeric_path.as_posix())
    )
    df_numeric.printSchema()
    df_numeric.show(truncate=False)

    print("--- Currency symbols (resolved from hex entities) ---")
    df_numeric.select("id", "name", "currency_symbol", "price").show(truncate=False)

    rows = {r["id"]: r for r in df_numeric.collect()}
    assert "©" in rows[101]["description"], "&#169; not decoded to ©"
    assert "—" in rows[101]["description"], "&#8212; not decoded to em-dash"
    assert "€" in rows[102]["currency_symbol"], "&#x20AC; not decoded to €"
    assert "®" in rows[102]["description"], "&#xAE; not decoded to ®"
    assert "™" in rows[103]["name"], "&#8482; not decoded to ™"
    assert "§" in rows[103]["description"], "&#167; not decoded to §"
    assert "$" in rows[101]["currency_symbol"], "&#x24; not decoded to $"
    assert "£" in rows[103]["currency_symbol"], "&#xA3; not decoded to £"
    assert "¥" in rows[104]["currency_symbol"], "&#xA5; not decoded to ¥"
    assert "★" in rows[104]["description"], "&#x2605; not decoded to ★"
    assert "❤" in rows[104]["description"], "&#x2764; not decoded to ❤"
    print("\n✓ All numeric character references (decimal + hex) decoded")

    print("\n--- Unicode table ---")
    entity_table = [
        ("&#169;", "©", "Copyright"),
        ("&#8212;", "—", "Em dash"),
        ("&#8211;", "–", "En dash"),
        ("&#x20AC;", "€", "Euro sign"),
        ("&#xA3;", "£", "Pound sign"),
        ("&#xA5;", "¥", "Yen sign"),
        ("&#x24;", "$", "Dollar sign"),
        ("&#xAE;", "®", "Registered"),
        ("&#8482;", "™", "Trademark"),
        ("&#167;", "§", "Section sign"),
        ("&#8226;", "•", "Bullet"),
        ("&#x2605;", "★", "Star"),
        ("&#x2764;", "❤", "Heart"),
    ]
    print(f"  {'Entity':<12} {'Char':<6} {'Name'}")
    print(f"  {'-'*12} {'-'*6} {'-'*20}")
    for ent, char, name in entity_table:
        print(f"  {ent:<12} {char:<6} {name}")

    # ==================================================================
    # 3. Entities Inside XML Attributes
    # ==================================================================
    print("\n" + "=" * 70)
    print("3. Entities Inside Attributes")
    print("=" * 70)
    print(
        "Entity references in attribute values are resolved just like in\n"
        "element text. With attributePrefix, they appear as regular columns.\n"
    )

    df_attrs = (
        spark.read.format("xml")
        .option("rowTag", "item")
        .option("attributePrefix", "attr_")
        .load(attr_path.as_posix())
    )
    df_attrs.printSchema()
    df_attrs.show(truncate=False)

    print("--- Decoded attribute values ---")
    for row in df_attrs.collect():
        print(f"  id={row['attr_id']}  label={row['attr_label']}  status={row['attr_status']}")

    rows = {r["attr_id"]: r for r in df_attrs.collect()}
    assert "&" in rows[1]["attr_label"], "& not decoded in attribute"
    assert "<" in rows[1]["attr_status"], "< not decoded in attribute"
    assert ">" in rows[2]["attr_status"], "> not decoded in attribute"
    assert "•" in rows[2]["attr_label"], "bullet not decoded in attribute"
    assert "–" in rows[2]["name"], "en-dash not decoded in element"
    assert '"' in rows[3]["name"], 'quote not decoded in name'
    assert "≥" in rows[3]["attr_status"], "≥ not decoded in attribute"
    print("\n✓ Entities in attributes decoded correctly")

    # ==================================================================
    # 4. Mixed Entity Types in a Single Element
    # ==================================================================
    print("\n" + "=" * 70)
    print("4. Mixed Entity Types in a Single Element")
    print("=" * 70)
    print(
        "Predefined entities, decimal references, and hex references\n"
        "can all appear together in one element's text.\n"
    )

    df_mixed = (
        spark.read.format("xml")
        .option("rowTag", "record")
        .schema(RECORD_SCHEMA)
        .load(mixed_path.as_posix())
    )
    df_mixed.printSchema()
    df_mixed.show(truncate=False)

    rows = {r["id"]: r for r in df_mixed.collect()}

    # Record 1: © + & + ™ + <em>bold</em>
    r1_text = rows[1]["text"]
    assert "©" in r1_text and "&" in r1_text and "™" in r1_text, "Mixed entities in record 1"
    assert "<em>" in r1_text, "Escaped HTML tags decoded in record 1"
    r1_formula = rows[1]["formula"]
    assert "²" in r1_formula and "<" in r1_formula, "Superscript + lt in formula"

    # Record 2: € + £ + —
    r2_text = rows[2]["text"]
    assert "€" in r2_text and "£" in r2_text and "—" in r2_text, "Currencies in record 2"

    # Record 3: ★ + " + —
    r3_text = rows[3]["text"]
    assert "★★★" in r3_text, "Stars decoded in record 3"
    assert '"' in r3_text, "Quotes decoded in record 3"
    r3_formula = rows[3]["formula"]
    assert "π" in r3_formula and "≈" in r3_formula, "Pi + approx in formula"
    print("✓ All mixed entity combinations decoded correctly")

    # ==================================================================
    # 5. Write Round-Trip — Spark Re-Escapes Special Characters
    # ==================================================================
    print("\n" + "=" * 70)
    print("5. Write Round-Trip — Spark Re-Escapes Characters")
    print("=" * 70)
    print(
        "When writing XML, Spark escapes <, >, & back to &lt; &gt; &amp;\n"
        "but Unicode characters (©, €, ★) are written as literal UTF-8.\n"
    )

    output_path = base / "roundtrip_output"
    (
        df_mixed.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "records")
        .option("rowTag", "record")
        .save(output_path.as_posix())
    )

    df_roundtrip = (
        spark.read.format("xml")
        .option("rowTag", "record")
        .schema(RECORD_SCHEMA)
        .load(output_path.as_posix())
    )
    df_roundtrip.show(truncate=False)

    orig_rows = sorted([tuple(r) for r in df_mixed.collect()])
    rt_rows = sorted([tuple(r) for r in df_roundtrip.collect()])
    assert orig_rows == rt_rows, "Round-trip data mismatch!"
    print("✓ Round-trip data matches original")

    # Inspect written XML content
    import glob as g

    written_files = g.glob(str(output_path / "part-*.xml"))
    if written_files:
        sample = Path(written_files[0]).read_text(encoding="utf-8")
        print("\n--- Sample output XML (first file) ---")
        for line in sample.splitlines()[:15]:
            print(f"  {line}")
        if "&lt;" in sample or "&gt;" in sample or "&amp;" in sample:
            print("\n✓ Spark correctly re-escaped <, >, & as XML entities in output")
        if "©" in sample or "€" in sample or "★" in sample:
            print("✓ Unicode characters written as literal UTF-8 (no entity encoding)")

    # ==================================================================
    # 6. Queries on Entity-Decoded Values
    # ==================================================================
    print("\n" + "=" * 70)
    print("6. Queries on Entity-Decoded Values")
    print("=" * 70)
    print(
        "Since entities are resolved, queries use the decoded characters.\n"
    )

    # Filter on decoded ampersand
    print("--- Messages containing '&' (decoded from &amp;) ---")
    df_builtin.filter(F.col("body").contains("&")).select(
        "id", "subject", "body"
    ).show(truncate=False)

    # Filter on decoded HTML tags
    print("--- Messages containing HTML tags (decoded from &lt;/&gt;) ---")
    df_builtin.filter(F.col("body").contains("<div")).select(
        "id", "subject", "body"
    ).show(truncate=False)

    # Search for Unicode symbols
    print("--- Products with © symbol ---")
    df_numeric.filter(F.col("description").contains("©")).select(
        "id", "name", "description"
    ).show(truncate=False)

    # Aggregate products by currency symbol
    print("--- Products grouped by currency symbol ---")
    df_numeric.groupBy("currency_symbol").agg(
        F.count("*").alias("count"),
        F.round(F.sum("price"), 2).alias("total_price"),
        F.collect_list("name").alias("products"),
    ).show(truncate=False)

    # String operations on decoded text
    print("--- Extract formula operators from mixed entities ---")
    df_mixed.select(
        "id",
        "formula",
        F.length("formula").alias("formula_len"),
    ).show(truncate=False)

    # ==================================================================
    # 7. Inferred Schema Handles Entities Transparently
    # ==================================================================
    print("\n" + "=" * 70)
    print("7. Inferred Schema — Entities Are Transparent")
    print("=" * 70)

    df_inferred = (
        spark.read.format("xml")
        .option("rowTag", "product")
        .load(numeric_path.as_posix())
    )
    df_inferred.printSchema()
    print(
        "Schema inference sees the decoded values — entity references\n"
        "never appear as column names or affect type inference.\n"
    )
    print(f"Columns: {df_inferred.columns}")
    assert "currency_symbol" in df_inferred.columns
    assert "description" in df_inferred.columns
    print("✓ Schema inference works identically with entity-heavy XML")

    # ==================================================================
    # 8. Caveat: Custom / DTD-Defined Entities
    # ==================================================================
    print("\n" + "=" * 70)
    print("8. Caveat: Custom / DTD-Defined Entities")
    print("=" * 70)
    print(
        "XML allows custom entities via DOCTYPE declarations:\n"
        "\n"
        '  <!DOCTYPE root [\n'
        '    <!ENTITY company "Acme Corp">\n'
        '    <!ENTITY year "2024">\n'
        "  ]>\n"
        "  <root><name>&company; &year;</name></root>\n"
        "\n"
        "spark-xml does NOT support custom DTD entity resolution.\n"
        "The Hadoop InputFormat splits files by rowTag before full\n"
        "XML parsing, so DOCTYPE declarations are stripped and custom\n"
        "entities like &company; will cause parse errors or appear\n"
        "as empty/null values.\n"
        "\n"
        "Workaround: pre-process XML to expand custom entities before\n"
        "loading with spark-xml (e.g., using lxml or xmllint).\n"
    )

    # Demonstrate what happens with a DTD entity
    dtd_xml = textwrap.dedent("""\
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE items [
          <!ENTITY brand "Acme Corp">
          <!ENTITY year "2024">
        ]>
        <items>
          <item><name>&brand; Widget</name><year>&year;</year></item>
        </items>
    """)
    dtd_path = base / "dtd_entities.xml"
    write_xml(dtd_path, dtd_xml)

    print("--- Attempting to read XML with custom DTD entities ---")
    try:
        df_dtd = (
            spark.read.format("xml")
            .option("rowTag", "item")
            .option("mode", "PERMISSIVE")
            .load(dtd_path.as_posix())
        )
        df_dtd.show(truncate=False)
        cols = df_dtd.columns
        if "_corrupt_record" in cols:
            print("Custom entities produced corrupt records — DTD not processed")
        elif "name" in cols:
            dtd_rows = df_dtd.collect()
            val = dtd_rows[0]["name"] if dtd_rows else None
            if val and "Acme" in str(val):
                print("(!) Custom entities were resolved — parser DTD support detected")
            else:
                print("Custom entities NOT resolved — values may be empty or partial")
        else:
            print("Unexpected schema — custom entity handling is parser-dependent")
    except Exception as e:
        print(f"Expected error with DTD entities: {type(e).__name__}: {e}")
    print(
        "\n⚠ Custom DTD entities are unreliable in spark-xml.\n"
        "  Stick to predefined entities (&lt; &gt; &amp; &quot; &apos;)\n"
        "  and numeric character references (&#NNN; &#xHHH;)."
    )

    # ==================================================================
    # Summary
    # ==================================================================
    print("\n" + "=" * 70)
    print("Summary: XML Entity Handling in spark-xml")
    print("=" * 70)
    print(
        "  Entity Type                    Supported  Example\n"
        "  ─────────────────────────────  ─────────  ─────────────────────\n"
        "  Predefined (built-in)          ✓ Yes      &lt; → <\n"
        "  Numeric decimal                ✓ Yes      &#169; → ©\n"
        "  Numeric hex                    ✓ Yes      &#x20AC; → €\n"
        "  In attributes                  ✓ Yes      &amp; → & in attrs\n"
        "  Mixed (combined types)         ✓ Yes      &#169; &amp; &#x2122;\n"
        "  Write round-trip               ✓ Yes      Re-escapes < > &\n"
        "  Custom DTD (<!ENTITY>)         ✗ No       Parser-dependent\n"
        "  External DTD (SYSTEM/PUBLIC)   ✗ No       Blocked (XXE safety)\n"
    )

    spark.stop()