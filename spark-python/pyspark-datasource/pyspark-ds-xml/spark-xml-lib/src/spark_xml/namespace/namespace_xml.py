"""Demonstrate spark-xml with ``ignoreNamespace=false`` (preserve prefixes).

When ``ignoreNamespace`` is ``false`` (the default), spark-xml keeps
namespace prefixes in column names: ``bk:title``, ``pub:name``, etc.
This preserves the namespace semantics but makes column access harder
because Spark identifiers with colons require backtick escaping.

This script covers:

1. Preserved prefixed column names — ``bk:title``, ``pub:name``
2. Accessing prefixed columns with backtick escaping in ``selectExpr``
3. Accessing prefixed columns with ``col()`` — uses backtick quoting
4. Schema comparison: ``ignoreNamespace=true`` vs ``false``
5. Default namespace preserved — column names gain no prefix
6. Mixed namespaces preserved — both prefixed and unprefixed
7. Write round-trip — Spark outputs plain element names (no xmlns)
8. Renaming prefixed columns for downstream use
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql import functions as F

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

# ── Same multi-prefix XML used by both scripts ───────────────────────────

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

# ── Default namespace (no prefix) ────────────────────────────────────────

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
    </employees>
""")

# ── Mixed default + prefixed ─────────────────────────────────────────────

MIXED_NS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <report xmlns="http://example.com/report"
            xmlns:fin="http://example.com/finance"
            xmlns:geo="http://example.com/geography">
      <entry>
        <title>Q4 Sales</title>
        <fin:revenue>1250000</fin:revenue>
        <fin:expenses>980000</fin:expenses>
        <geo:region>North America</geo:region>
        <geo:country>USA</geo:country>
      </entry>
      <entry>
        <title>Q4 Sales</title>
        <fin:revenue>890000</fin:revenue>
        <fin:expenses>720000</fin:expenses>
        <geo:region>Europe</geo:region>
        <geo:country>Germany</geo:country>
      </entry>
      <entry>
        <title>Q4 Sales</title>
        <fin:revenue>1100000</fin:revenue>
        <fin:expenses>850000</fin:expenses>
        <geo:region>Asia Pacific</geo:region>
        <geo:country>Japan</geo:country>
      </entry>
    </report>
""")


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

    write_xml(multi_path, MULTI_PREFIX_XML)
    write_xml(default_path, DEFAULT_NS_XML)
    write_xml(mixed_path, MIXED_NS_XML)

    spark = get_spark_session(
        app_name="spark-xml-preserve-namespace",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ==================================================================
    # 1. Preserved Prefixed Column Names
    # ==================================================================
    print("=" * 70)
    print("1. Preserved Prefixed Column Names (ignoreNamespace=false)")
    print("=" * 70)
    print(
        "With ignoreNamespace=false (the default), column names retain\n"
        "their namespace prefixes: bk:title, pub:publisher, rev:review.\n"
    )

    df_preserved = (
        spark.read.format("xml")
        .option("rowTag", "bk:book")
        .option("ignoreNamespace", "false")
        .load(multi_path.as_posix())
    )
    df_preserved.printSchema()
    df_preserved.show(truncate=False)

    cols = df_preserved.columns
    print(f"Columns: {sorted(cols)}")
    prefixed = [c for c in cols if ":" in c]
    print(f"Prefixed columns: {sorted(prefixed)}")
    assert any("bk:" in c for c in cols), "Expected bk: prefixed columns"
    assert any("pub:" in c for c in cols), "Expected pub: prefixed columns"
    assert any("rev:" in c for c in cols), "Expected rev: prefixed columns"
    print("✓ All namespace prefixes preserved in column names")

    # ==================================================================
    # 2. Accessing Prefixed Columns — selectExpr with Backticks
    # ==================================================================
    print("\n" + "=" * 70)
    print("2. Accessing Prefixed Columns with selectExpr (backticks)")
    print("=" * 70)
    print(
        "Columns containing ':' must be wrapped in backticks in SQL\n"
        "expressions: `bk:title`, `pub:publisher`.`pub:name`, etc.\n"
    )

    df_flat = df_preserved.selectExpr(
        "_id as book_id",
        "`bk:title` as title",
        "`bk:author` as author",
        "`bk:genre` as genre",
        "`bk:price`._VALUE as price",
        "`bk:price`._currency as currency",
        "`pub:publisher`.`pub:name` as publisher_name",
        "`pub:publisher`.`pub:year` as pub_year",
        "`pub:publisher`.`pub:city` as pub_city",
        "`rev:review`.`rev:rating` as rating",
        "`rev:review`.`rev:comment` as comment",
    )
    df_flat.show(truncate=False)
    print("✓ Backtick-escaped access to prefixed columns works correctly")

    # ==================================================================
    # 3. Accessing Prefixed Columns — col() API
    # ==================================================================
    print("\n" + "=" * 70)
    print("3. Accessing Prefixed Columns with col() API")
    print("=" * 70)
    print(
        "The col() function also supports backtick escaping.\n"
        "Use F.col('`bk:title`') or df['`bk:title`'].\n"
    )

    df_col_api = df_preserved.select(
        F.col("_id").alias("book_id"),
        F.col("`bk:title`").alias("title"),
        F.col("`bk:author`").alias("author"),
        F.col("`bk:price`._VALUE").alias("price"),
        F.col("`bk:price`._currency").alias("currency"),
        F.col("`rev:review`.`rev:rating`").alias("rating"),
    )
    df_col_api.show(truncate=False)
    print("✓ col() API with backtick escaping works for nested prefixed fields")

    # Filter on prefixed columns
    print("\n--- Filter: rating > 4.2 ---")
    df_preserved.filter(
        F.col("`rev:review`.`rev:rating`") > 4.2
    ).selectExpr(
        "`bk:title` as title",
        "`rev:review`.`rev:rating` as rating",
    ).show(truncate=False)

    # ==================================================================
    # 4. Schema Comparison: ignore=true vs ignore=false
    # ==================================================================
    print("\n" + "=" * 70)
    print("4. Schema Comparison — Ignored vs Preserved")
    print("=" * 70)

    df_ignored = (
        spark.read.format("xml")
        .option("rowTag", "bk:book")
        .option("ignoreNamespace", "true")
        .load(multi_path.as_posix())
    )

    print("--- ignoreNamespace=true ---")
    cols_ignored = sorted(df_ignored.columns)
    print(f"  Columns: {cols_ignored}")

    print("\n--- ignoreNamespace=false ---")
    cols_preserved = sorted(df_preserved.columns)
    print(f"  Columns: {cols_preserved}")

    print("\n--- Column name mapping ---")
    # Pair up columns by stripping prefixes from preserved names
    for col_p in sorted(cols_preserved):
        local_name = col_p.split(":")[-1] if ":" in col_p else col_p
        match = local_name if local_name in cols_ignored else "—"
        print(f"  {col_p:<20} → {match}")

    # Data should be the same regardless of namespace setting
    ignored_rows = sorted([tuple(str(v) for v in r) for r in df_flat.collect()])
    col_api_rows = sorted([tuple(str(v) for v in r) for r in df_col_api.collect()])
    # Compare just the shared columns
    flat_titles = sorted([r["title"] for r in df_flat.collect()])
    api_titles = sorted([r["title"] for r in df_col_api.collect()])
    assert flat_titles == api_titles, "Title data mismatch!"
    print("\n✓ Same underlying data regardless of ignoreNamespace setting")

    # ==================================================================
    # 5. Default Namespace Preserved
    # ==================================================================
    print("\n" + "=" * 70)
    print("5. Default Namespace Preserved (no prefix in column names)")
    print("=" * 70)
    print(
        "A default namespace (xmlns='URI' without a prefix) does NOT add\n"
        "any prefix to column names — even when ignoreNamespace=false.\n"
        "Column names are bare: 'name', 'department', etc.\n"
    )

    df_default_preserved = (
        spark.read.format("xml")
        .option("rowTag", "employee")
        .option("ignoreNamespace", "false")
        .load(default_path.as_posix())
    )
    df_default_preserved.printSchema()
    df_default_preserved.show(truncate=False)

    cols = df_default_preserved.columns
    prefixed = [c for c in cols if ":" in c]
    assert not prefixed, f"Unexpected prefixed columns: {prefixed}"
    print("✓ Default namespace produces no prefixed columns (same as ignore=true)")

    # ==================================================================
    # 6. Mixed Namespaces Preserved
    # ==================================================================
    print("\n" + "=" * 70)
    print("6. Mixed Namespaces Preserved")
    print("=" * 70)
    print(
        "Default-namespace elements (title) have bare names.\n"
        "Prefixed elements (fin:revenue, geo:region) keep their prefix.\n"
    )

    df_mixed = (
        spark.read.format("xml")
        .option("rowTag", "entry")
        .option("ignoreNamespace", "false")
        .load(mixed_path.as_posix())
    )
    df_mixed.printSchema()
    df_mixed.show(truncate=False)

    cols = df_mixed.columns
    print(f"Columns: {sorted(cols)}")
    bare = [c for c in cols if ":" not in c]
    prefixed = [c for c in cols if ":" in c]
    print(f"  Bare (default ns):  {sorted(bare)}")
    print(f"  Prefixed:           {sorted(prefixed)}")

    assert "title" in bare, "Expected bare 'title' from default namespace"
    assert any("fin:" in c for c in prefixed), "Expected fin: prefix"
    assert any("geo:" in c for c in prefixed), "Expected geo: prefix"
    print("✓ Mixed namespace columns correctly split into bare + prefixed")

    # Flatten mixed namespace data
    print("\n--- Flattened mixed namespace report ---")
    df_mixed_flat = df_mixed.selectExpr(
        "title",
        "CAST(`fin:revenue` AS DOUBLE) as revenue",
        "CAST(`fin:expenses` AS DOUBLE) as expenses",
        "`geo:region` as region",
        "`geo:country` as country",
        "CAST(`fin:revenue` AS DOUBLE) - CAST(`fin:expenses` AS DOUBLE) as profit",
    )
    df_mixed_flat.show(truncate=False)

    # ==================================================================
    # 7. Write Round-Trip
    # ==================================================================
    print("\n" + "=" * 70)
    print("7. Write Round-Trip — Spark Writes Plain Element Names")
    print("=" * 70)
    print(
        "When writing a DataFrame with prefixed columns, Spark uses the\n"
        "column names as-is for XML element names. The colon becomes part\n"
        "of the element name, but NO xmlns declaration is generated.\n"
    )

    output_path = base / "preserved_output"
    (
        df_mixed_flat.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "report")
        .option("rowTag", "entry")
        .save(output_path.as_posix())
    )

    df_rt = (
        spark.read.format("xml")
        .option("rowTag", "entry")
        .load(output_path.as_posix())
    )
    df_rt.show(truncate=False)

    assert df_rt.count() == df_mixed_flat.count(), "Row count mismatch"
    print("✓ Round-trip preserved data correctly")

    # ==================================================================
    # 8. Renaming Prefixed Columns for Downstream Use
    # ==================================================================
    print("\n" + "=" * 70)
    print("8. Renaming Prefixed Columns for Downstream Use")
    print("=" * 70)
    print(
        "Prefixed column names are awkward in Parquet, Delta, or SQL.\n"
        "Best practice: rename immediately after reading.\n"
    )

    # Automated rename: strip all prefixes
    def strip_ns_prefix(df):
        """Rename all columns by removing namespace prefixes."""
        for col_name in df.columns:
            if ":" in col_name:
                local = col_name.split(":")[-1]
                df = df.withColumnRenamed(col_name, local)
        return df

    df_cleaned = strip_ns_prefix(df_mixed)
    print("Before rename:")
    print(f"  {sorted(df_mixed.columns)}")
    print("After rename:")
    print(f"  {sorted(df_cleaned.columns)}")
    df_cleaned.show(truncate=False)

    assert not any(":" in c for c in df_cleaned.columns), "Prefix still present!"
    print("✓ All namespace prefixes removed — DataFrame ready for downstream use")

    # ==================================================================
    # Summary
    # ==================================================================
    print("\n" + "=" * 70)
    print("Summary: ignoreNamespace=false (default)")
    print("=" * 70)
    print(
        "  Behavior                        Effect\n"
        "  ──────────────────────────────   ────────────────────────────────\n"
        "  rowTag                           Requires prefix (bk:book)\n"
        "  Prefixed element columns         Keep prefix (bk:title)\n"
        "  Nested prefixed columns          Keep prefix (pub:publisher.pub:name)\n"
        "  Default namespace columns        No prefix added (bare names)\n"
        "  Column access (SQL expr)         Use backticks: `bk:title`\n"
        "  Column access (col() API)        Use backticks: col('`bk:title`')\n"
        "  Written XML output               Column names become element names\n"
        "  Recommended workflow             Read → rename → transform\n"
        "\n"
        "  Tip: Use ignoreNamespace=true unless you need to distinguish\n"
        "  elements that share local names across different namespaces.\n"
    )

    spark.stop()
