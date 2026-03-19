"""Demonstrate XML namespace scoping rules and how spark-xml handles them.

In XML, namespace declarations have **scope** — a namespace defined on a
parent element applies to that element and all its descendants unless
overridden.  Namespaces can be:

- **Root-scoped** — declared on the root element, visible everywhere
- **Element-scoped** — declared on a child, visible only within that subtree
- **Overridden** — a child re-declares a prefix to a different URI
- **Default namespace switching** — ``xmlns="..."`` changes per element

spark-xml's ``ignoreNamespace`` option strips all prefixes regardless
of scope.  When preserving namespaces, column names reflect whichever
prefix is used in the actual element tags — not the declaration scope.

This script demonstrates:

1. Root-scoped namespace — single declaration, used everywhere
2. Element-scoped namespace — prefix declared only within a subtree
3. Namespace override — same prefix bound to different URIs at different levels
4. Default namespace switching — ``xmlns`` changes on nested elements
5. Multiple scopes in one document — combining all patterns
6. Comparison: ignoreNamespace=true vs false for scoped namespaces
7. Explicit schema for documents with scoped namespaces
8. Write round-trip — scope information lost in output
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

# ── 1. Root-scoped namespace ─────────────────────────────────────────────
# All elements share the same namespace declared at the root.

ROOT_SCOPED_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <inv:inventory xmlns:inv="http://example.com/inventory">
      <inv:item>
        <inv:name>Widget A</inv:name>
        <inv:category>Electronics</inv:category>
        <inv:price>29.99</inv:price>
        <inv:stock>150</inv:stock>
      </inv:item>
      <inv:item>
        <inv:name>Widget B</inv:name>
        <inv:category>Mechanical</inv:category>
        <inv:price>14.50</inv:price>
        <inv:stock>300</inv:stock>
      </inv:item>
      <inv:item>
        <inv:name>Widget C</inv:name>
        <inv:category>Electronics</inv:category>
        <inv:price>49.99</inv:price>
        <inv:stock>75</inv:stock>
      </inv:item>
    </inv:inventory>
""")

# ── 2. Element-scoped namespace ──────────────────────────────────────────
# The "loc:" namespace is declared only on the <location> element,
# not at the root level.  It applies to <location> and its children.

ELEMENT_SCOPED_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <company xmlns:emp="http://example.com/employees">
      <emp:employee>
        <emp:name>Alice Johnson</emp:name>
        <emp:role>Engineer</emp:role>
        <emp:salary>95000</emp:salary>
        <location xmlns:loc="http://example.com/location">
          <loc:city>San Francisco</loc:city>
          <loc:state>CA</loc:state>
          <loc:country>USA</loc:country>
        </location>
      </emp:employee>
      <emp:employee>
        <emp:name>Bob Smith</emp:name>
        <emp:role>Designer</emp:role>
        <emp:salary>88000</emp:salary>
        <location xmlns:loc="http://example.com/location">
          <loc:city>Berlin</loc:city>
          <loc:state>Berlin</loc:state>
          <loc:country>Germany</loc:country>
        </location>
      </emp:employee>
      <emp:employee>
        <emp:name>Carol Lee</emp:name>
        <emp:role>Manager</emp:role>
        <emp:salary>110000</emp:salary>
        <location xmlns:loc="http://example.com/location">
          <loc:city>Tokyo</loc:city>
          <loc:state>Tokyo</loc:state>
          <loc:country>Japan</loc:country>
        </location>
      </emp:employee>
    </company>
""")

# ── 3. Namespace override ────────────────────────────────────────────────
# The prefix "ns" is bound to different URIs at different nesting levels.

NS_OVERRIDE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <root xmlns:ns="http://example.com/v1">
      <ns:record>
        <ns:id>1</ns:id>
        <ns:value>Original namespace (v1)</ns:value>
        <ns:score>85.5</ns:score>
        <detail xmlns:ns="http://example.com/v2">
          <ns:note>Inner namespace overrides to v2</ns:note>
          <ns:priority>high</ns:priority>
        </detail>
      </ns:record>
      <ns:record>
        <ns:id>2</ns:id>
        <ns:value>Still v1 at this level</ns:value>
        <ns:score>92.0</ns:score>
        <detail xmlns:ns="http://example.com/v2">
          <ns:note>Again v2 in the detail block</ns:note>
          <ns:priority>medium</ns:priority>
        </detail>
      </ns:record>
    </root>
""")

# ── 4. Default namespace switching ───────────────────────────────────────
# The default xmlns changes at different element levels.

DEFAULT_NS_SWITCH_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <catalog xmlns="http://example.com/catalog">
      <book>
        <title>Spark Guide</title>
        <author>Alice</author>
        <price>45.99</price>
        <metadata xmlns="http://example.com/metadata">
          <isbn>978-0-123456-78-9</isbn>
          <pages>320</pages>
          <language>English</language>
        </metadata>
      </book>
      <book>
        <title>XML Patterns</title>
        <author>Bob</author>
        <price>39.50</price>
        <metadata xmlns="http://example.com/metadata">
          <isbn>978-0-987654-32-1</isbn>
          <pages>256</pages>
          <language>English</language>
        </metadata>
      </book>
    </catalog>
""")

# ── 5. Multiple scope patterns combined ──────────────────────────────────

MULTI_SCOPE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <data xmlns:core="http://example.com/core"
          xmlns="http://example.com/default">
      <core:record>
        <core:id>1</core:id>
        <name>Project Alpha</name>
        <status>active</status>
        <budget xmlns:fin="http://example.com/finance">
          <fin:amount>500000</fin:amount>
          <fin:currency>USD</fin:currency>
        </budget>
        <team xmlns:hr="http://example.com/hr">
          <hr:lead>Alice</hr:lead>
          <hr:size>12</hr:size>
        </team>
      </core:record>
      <core:record>
        <core:id>2</core:id>
        <name>Project Beta</name>
        <status>planning</status>
        <budget xmlns:fin="http://example.com/finance">
          <fin:amount>250000</fin:amount>
          <fin:currency>EUR</fin:currency>
        </budget>
        <team xmlns:hr="http://example.com/hr">
          <hr:lead>Bob</hr:lead>
          <hr:size>6</hr:size>
        </team>
      </core:record>
      <core:record>
        <core:id>3</core:id>
        <name>Project Gamma</name>
        <status>active</status>
        <budget xmlns:fin="http://example.com/finance">
          <fin:amount>750000</fin:amount>
          <fin:currency>USD</fin:currency>
        </budget>
        <team xmlns:hr="http://example.com/hr">
          <hr:lead>Carol</hr:lead>
          <hr:size>20</hr:size>
        </team>
      </core:record>
    </data>
""")


def write_xml(path: Path, content: str) -> None:
    """Write XML string to file, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "namespace_scope_demo"

    root_path = base / "root_scoped.xml"
    elem_path = base / "element_scoped.xml"
    override_path = base / "ns_override.xml"
    default_switch_path = base / "default_ns_switch.xml"
    multi_path = base / "multi_scope.xml"

    write_xml(root_path, ROOT_SCOPED_XML)
    write_xml(elem_path, ELEMENT_SCOPED_XML)
    write_xml(override_path, NS_OVERRIDE_XML)
    write_xml(default_switch_path, DEFAULT_NS_SWITCH_XML)
    write_xml(multi_path, MULTI_SCOPE_XML)

    spark = get_spark_session(
        app_name="spark-xml-namespace-scope",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ==================================================================
    # 1. Root-Scoped Namespace
    # ==================================================================
    print("=" * 70)
    print("1. Root-Scoped Namespace")
    print("=" * 70)
    print(
        "A single namespace declared at the root applies to ALL elements.\n"
        "This is the simplest and most common scoping pattern.\n"
    )

    # ignoreNamespace=true
    df_root_clean = (
        spark.read.format("xml")
        .option("rowTag", "inv:item")
        .option("ignoreNamespace", "true")
        .load(root_path.as_posix())
    )
    print("--- ignoreNamespace=true ---")
    df_root_clean.printSchema()
    df_root_clean.show(truncate=False)

    cols = df_root_clean.columns
    assert not any(":" in c for c in cols), "Prefixes not stripped!"
    assert "name" in cols and "category" in cols
    print(f"✓ Clean columns: {sorted(cols)}")

    # ignoreNamespace=false
    df_root_prefixed = (
        spark.read.format("xml")
        .option("rowTag", "inv:item")
        .option("ignoreNamespace", "false")
        .load(root_path.as_posix())
    )
    print("\n--- ignoreNamespace=false ---")
    df_root_prefixed.printSchema()
    df_root_prefixed.show(truncate=False)

    cols_p = df_root_prefixed.columns
    assert all("inv:" in c for c in cols_p), "Expected all inv: prefixed"
    print(f"✓ Prefixed columns: {sorted(cols_p)}")

    # ==================================================================
    # 2. Element-Scoped Namespace
    # ==================================================================
    print("\n" + "=" * 70)
    print("2. Element-Scoped Namespace")
    print("=" * 70)
    print(
        "The 'loc:' namespace is declared on <location>, NOT at the root.\n"
        "It only applies to <location> and its children (city, state, etc.).\n"
        "spark-xml handles this correctly in both modes.\n"
    )

    df_elem_clean = (
        spark.read.format("xml")
        .option("rowTag", "emp:employee")
        .option("ignoreNamespace", "true")
        .load(elem_path.as_posix())
    )
    print("--- ignoreNamespace=true ---")
    df_elem_clean.printSchema()
    df_elem_clean.show(truncate=False)

    assert "location" in df_elem_clean.columns
    print("✓ Element-scoped 'loc:' namespace stripped → 'location' struct")

    # Flatten to show both root-scoped and element-scoped fields
    print("\n--- Flattened view (root + element scope) ---")
    df_elem_flat = df_elem_clean.selectExpr(
        "name",
        "role",
        "CAST(salary AS INT) as salary",
        "location.city as city",
        "location.state as state",
        "location.country as country",
    )
    df_elem_flat.show(truncate=False)

    # Preserved mode — shows different prefixes at different scopes
    df_elem_pres = (
        spark.read.format("xml")
        .option("rowTag", "emp:employee")
        .option("ignoreNamespace", "false")
        .load(elem_path.as_posix())
    )
    print("--- ignoreNamespace=false ---")
    df_elem_pres.printSchema()

    cols = df_elem_pres.columns
    emp_cols = [c for c in cols if c.startswith("emp:")]
    bare_cols = [c for c in cols if ":" not in c]
    print(f"  Root-scoped (emp:):     {sorted(emp_cols)}")
    print(f"  Element-scoped (bare):  {sorted(bare_cols)}")
    print(
        "\nNote: The <location> element itself has no prefix, so it appears\n"
        "as a bare column. Its children (loc:city etc.) carry the loc: prefix\n"
        "within the struct.\n"
    )

    # ==================================================================
    # 3. Namespace Override — Same Prefix, Different URIs
    # ==================================================================
    print("\n" + "=" * 70)
    print("3. Namespace Override — Same Prefix, Different URIs")
    print("=" * 70)
    print(
        "The prefix 'ns' is bound to http://example.com/v1 at the root,\n"
        "but overridden to http://example.com/v2 inside <detail>.\n"
        "spark-xml sees the local element name regardless of which URI\n"
        "the prefix maps to — it does NOT track namespace URIs.\n"
    )

    df_override_clean = (
        spark.read.format("xml")
        .option("rowTag", "ns:record")
        .option("ignoreNamespace", "true")
        .load(override_path.as_posix())
    )
    print("--- ignoreNamespace=true ---")
    df_override_clean.printSchema()
    df_override_clean.show(truncate=False)

    assert "detail" in df_override_clean.columns
    assert "id" in df_override_clean.columns
    print("✓ Override-scoped namespaces handled — all prefixes stripped")

    # Flatten to show fields from both scope levels
    print("\n--- Flattened: outer (v1) + inner (v2) fields ---")
    df_override_clean.selectExpr(
        "id",
        "value",
        "score",
        "detail.note as detail_note",
        "detail.priority as detail_priority",
    ).show(truncate=False)

    # Preserved mode
    df_override_pres = (
        spark.read.format("xml")
        .option("rowTag", "ns:record")
        .option("ignoreNamespace", "false")
        .load(override_path.as_posix())
    )
    print("--- ignoreNamespace=false ---")
    df_override_pres.printSchema()

    cols = df_override_pres.columns
    print(f"  Columns: {sorted(cols)}")
    print(
        "\n  Key insight: both outer ns:id (v1 URI) and inner ns:note (v2 URI)\n"
        "  share the 'ns:' prefix in column names. spark-xml does NOT\n"
        "  distinguish the same prefix bound to different URIs.\n"
    )

    # ==================================================================
    # 4. Default Namespace Switching
    # ==================================================================
    print("\n" + "=" * 70)
    print("4. Default Namespace Switching")
    print("=" * 70)
    print(
        "The root uses xmlns='http://example.com/catalog', but <metadata>\n"
        "switches to xmlns='http://example.com/metadata'.\n"
        "Both are default (unprefixed) namespaces — spark-xml sees bare names.\n"
    )

    df_switch = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .option("ignoreNamespace", "true")
        .load(default_switch_path.as_posix())
    )
    df_switch.printSchema()
    df_switch.show(truncate=False)

    assert "metadata" in df_switch.columns
    assert "title" in df_switch.columns
    print("✓ Default namespace switch is transparent — all columns bare")

    # Flatten metadata
    print("\n--- Flattened with metadata ---")
    df_switch.selectExpr(
        "title",
        "author",
        "CAST(price AS DOUBLE) as price",
        "metadata.isbn as isbn",
        "CAST(metadata.pages AS INT) as pages",
        "metadata.language as language",
    ).show(truncate=False)

    # ==================================================================
    # 5. Multiple Scope Patterns Combined
    # ==================================================================
    print("\n" + "=" * 70)
    print("5. Multiple Scope Patterns Combined")
    print("=" * 70)
    print(
        "Root-level: core: prefix + default namespace.\n"
        "Element-level: fin: on <budget>, hr: on <team>.\n"
        "All combined in one document.\n"
    )

    df_multi_clean = (
        spark.read.format("xml")
        .option("rowTag", "core:record")
        .option("ignoreNamespace", "true")
        .load(multi_path.as_posix())
    )
    print("--- ignoreNamespace=true ---")
    df_multi_clean.printSchema()
    df_multi_clean.show(truncate=False)

    cols = df_multi_clean.columns
    assert "budget" in cols and "team" in cols
    assert "name" in cols and "status" in cols
    print(f"✓ All scopes unified: {sorted(cols)}")

    # Flatten the multi-scope structure
    print("\n--- Full flattened view ---")
    df_multi_flat = df_multi_clean.selectExpr(
        "id",
        "name as project",
        "status",
        "CAST(budget.amount AS DOUBLE) as budget_amount",
        "budget.currency as budget_currency",
        "team.lead as team_lead",
        "CAST(team.size AS INT) as team_size",
    )
    df_multi_flat.show(truncate=False)

    # Preserved mode — shows the mix of prefixes
    df_multi_pres = (
        spark.read.format("xml")
        .option("rowTag", "core:record")
        .option("ignoreNamespace", "false")
        .load(multi_path.as_posix())
    )
    print("--- ignoreNamespace=false ---")
    df_multi_pres.printSchema()

    cols = df_multi_pres.columns
    core_cols = [c for c in cols if c.startswith("core:")]
    bare_cols = [c for c in cols if ":" not in c]
    print(f"  Root-scoped (core:):    {sorted(core_cols)}")
    print(f"  Default ns (bare):      {sorted(bare_cols)}")

    # ==================================================================
    # 6. Comparison: ignore=true vs false for Scoped Namespaces
    # ==================================================================
    print("\n" + "=" * 70)
    print("6. Scope Comparison: ignoreNamespace=true vs false")
    print("=" * 70)

    print("\n--- Multi-scope document ---")
    print(f"  ignore=true columns:  {sorted(df_multi_clean.columns)}")
    print(f"  ignore=false columns: {sorted(df_multi_pres.columns)}")

    # Verify same data content
    clean_rows = sorted([r["id"] for r in df_multi_clean.collect()])
    pres_rows = sorted(
        [r["`core:id`"] if "`core:id`" in df_multi_pres.columns
         else r["core:id"]
         for r in df_multi_pres.collect()]
    )
    assert clean_rows == pres_rows, "ID data mismatch!"
    print("✓ Same underlying data regardless of ignoreNamespace setting")

    print("\n--- Column mapping across scopes ---")
    scope_mapping = [
        ("core:id", "id", "Root-scoped prefix"),
        ("name", "name", "Default namespace (bare)"),
        ("status", "status", "Default namespace (bare)"),
        ("budget", "budget", "Element-scoped container"),
        ("team", "team", "Element-scoped container"),
    ]
    print(f"  {'Preserved':<20} {'Ignored':<12} {'Scope'}")
    print(f"  {'-'*20} {'-'*12} {'-'*30}")
    for pres, ign, scope in scope_mapping:
        print(f"  {pres:<20} {ign:<12} {scope}")

    # ==================================================================
    # 7. Explicit Schema for Scoped Namespaces
    # ==================================================================
    print("\n" + "=" * 70)
    print("7. Explicit Schema for Scoped Namespaces")
    print("=" * 70)
    print(
        "When using an explicit schema with ignoreNamespace=true, use\n"
        "plain field names — regardless of where namespaces are declared.\n"
    )

    multi_schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("budget", StructType([
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
        ]), True),
        StructField("team", StructType([
            StructField("lead", StringType(), True),
            StructField("size", LongType(), True),
        ]), True),
    ])

    df_explicit = (
        spark.read.format("xml")
        .option("rowTag", "core:record")
        .option("ignoreNamespace", "true")
        .schema(multi_schema)
        .load(multi_path.as_posix())
    )
    df_explicit.printSchema()
    df_explicit.show(truncate=False)

    assert df_explicit.schema == multi_schema, "Schema mismatch!"
    print("✓ Explicit schema applied — element-scoped namespaces transparent")

    # Verify typed values
    rows = df_explicit.collect()
    for r in rows:
        assert r["budget"]["amount"] is not None, "Budget amount is null"
        assert r["team"]["lead"] is not None, "Team lead is null"
    print("✓ All nested values from scoped namespaces correctly typed")

    # ==================================================================
    # 8. Write Round-Trip — Scope Information Lost
    # ==================================================================
    print("\n" + "=" * 70)
    print("8. Write Round-Trip — Scope Information Lost")
    print("=" * 70)
    print(
        "When writing XML, spark-xml uses column names as element names.\n"
        "All namespace scope information (xmlns declarations, prefix\n"
        "bindings, URI associations) is lost — output is plain XML.\n"
    )

    output_path = base / "multi_scope_output"
    (
        df_multi_flat.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "data")
        .option("rowTag", "record")
        .save(output_path.as_posix())
    )

    df_rt = (
        spark.read.format("xml")
        .option("rowTag", "record")
        .load(output_path.as_posix())
    )
    df_rt.show(truncate=False)

    assert df_rt.count() == df_multi_flat.count(), "Row count mismatch!"
    print("✓ Round-trip preserves data")

    # Verify no namespace artifacts in output
    import glob as g

    written_files = g.glob(str(output_path / "part-*.xml"))
    if written_files:
        content = Path(written_files[0]).read_text(encoding="utf-8")
        assert "xmlns" not in content, "xmlns found in output!"
        assert "core:" not in content, "Prefix found in output!"
        assert "fin:" not in content, "Scoped prefix in output!"
        assert "hr:" not in content, "Scoped prefix in output!"
        print("✓ No xmlns declarations or prefixes in written XML")

    # Queries on combined data
    print("\n--- Active projects with budget > 400K ---")
    df_multi_flat.filter(
        (F.col("status") == "active") & (F.col("budget_amount") > 400000)
    ).show(truncate=False)

    print("--- Summary by status ---")
    df_multi_flat.groupBy("status").agg(
        F.count("*").alias("count"),
        F.round(F.sum("budget_amount"), 2).alias("total_budget"),
        F.round(F.avg("team_size"), 1).alias("avg_team_size"),
        F.collect_list("project").alias("projects"),
    ).show(truncate=False)

    # ==================================================================
    # Summary
    # ==================================================================
    print("\n" + "=" * 70)
    print("Summary: Namespace Scoping in spark-xml")
    print("=" * 70)
    print(
        "  Scope Pattern                 spark-xml Behavior\n"
        "  ─────────────────────────────  ──────────────────────────────────\n"
        "  Root-scoped prefix             Handled — prefix on all elements\n"
        "  Element-scoped prefix          Handled — prefix only in subtree\n"
        "  Prefix override (same name)    Columns share prefix (no URI tracking)\n"
        "  Default namespace switch       Transparent — all bare names\n"
        "  Mixed root + element scopes    Handled — each level independent\n"
        "  Explicit schema (ignore=true)  Use plain names for all scopes\n"
        "  Write round-trip               Scope info lost — plain XML output\n"
        "\n"
        "  Key insight: spark-xml tracks PREFIX NAMES, not namespace URIs.\n"
        "  Two elements with the same prefix but different URIs (override)\n"
        "  will appear under the same column name.\n"
    )

    spark.stop()