"""Demonstrate how spark-xml handles XML comments during read and write.

XML comments (``<!-- ... -->``) are generally ignored by the XML parser
and never appear as DataFrame columns. However, spark-xml uses a
text-based row splitter that scans for ``<rowTag>`` before full parsing,
so a ``rowTag`` element inside a multi-line comment **will still be
picked up** as a row. This script shows:

1. Reading XML with comments — intra-element comments are stripped
2. Commented-out child elements are correctly ignored
3. **Caveat**: a full ``<rowTag>`` block inside a comment IS parsed
4. Writing XML — Spark output never contains comments
5. Round-trip verification
6. Queries work the same on commented data
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

# ── XML with comments in various positions ──────────────────────────────

COMMENTED_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <!-- Main configuration file for the bookstore inventory -->
    <!-- Last updated: 2024-11-20 by admin -->
    <bookstore>

      <!-- ===== Fiction Section ===== -->
      <book>
        <title>The Great Gatsby</title>
        <!-- Original publication: 1925, Scribner -->
        <author>F. Scott Fitzgerald</author>
        <genre>Fiction</genre>
        <price>10.99</price>
        <year>1925</year>
        <!-- Pulitzer Prize: No (not awarded that year) -->
      </book>

      <!-- This book is a classic in science communication -->
      <book>
        <title>A Brief History of Time</title>
        <author>Stephen Hawking</author>
        <genre>Science</genre>
        <price>12.50</price>
        <year>1988</year>
      </book>

      <!--
        The following book entry has a commented-out element.
        The <discount> element should NOT appear in the DataFrame.
      -->
      <book>
        <title>Clean Code</title>
        <author>Robert C. Martin</author>
        <genre>Technology</genre>
        <price>37.50</price>
        <year>2008</year>
        <!-- <discount>15</discount> -->
      </book>

      <book>
        <!-- inline comment before the first child element -->
        <title>Design Patterns</title>
        <author>Gang of Four</author>
        <genre>Technology</genre>
        <!-- price is in USD -->
        <price>52.99</price>
        <year>1994</year>
      </book>

    </bookstore>
""")

# Same data, no comments at all (for comparison)
CLEAN_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <bookstore>
      <book>
        <title>The Great Gatsby</title>
        <author>F. Scott Fitzgerald</author>
        <genre>Fiction</genre>
        <price>10.99</price>
        <year>1925</year>
      </book>
      <book>
        <title>A Brief History of Time</title>
        <author>Stephen Hawking</author>
        <genre>Science</genre>
        <price>12.50</price>
        <year>1988</year>
      </book>
      <book>
        <title>Clean Code</title>
        <author>Robert C. Martin</author>
        <genre>Technology</genre>
        <price>37.50</price>
        <year>2008</year>
      </book>
      <book>
        <title>Design Patterns</title>
        <author>Gang of Four</author>
        <genre>Technology</genre>
        <price>52.99</price>
        <year>1994</year>
      </book>
    </bookstore>
""")

# XML where comments sit right next to element text
INLINE_COMMENTS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <records>
      <record>
        <id>1</id>
        <status>active<!-- was: pending --></status>
        <value>100.0</value>
      </record>
      <record>
        <id>2</id>
        <status><!-- temporarily -->inactive</status>
        <value>200.0</value>
      </record>
      <record>
        <id>3</id>
        <status>active</status>
        <value><!-- originally 250 -->300.0</value>
      </record>
    </records>
""")

# Caveat: a commented-out <item> block is STILL parsed by spark-xml's
# text-based row splitter, which finds <item>...</item> before the XML
# parser sees the surrounding <!-- -->.
CAVEAT_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <items>
      <item>
        <name>Real Item</name>
        <qty>10</qty>
      </item>
      <!--
      <item>
        <name>Ghost Item</name>
        <qty>0</qty>
      </item>
      -->
    </items>
""")

BOOK_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("year", LongType(), True),
])


def write_xml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "comment_demo"

    commented_path = base / "books_commented.xml"
    clean_path = base / "books_clean.xml"
    inline_path = base / "inline_comments.xml"
    caveat_path = base / "caveat_commented_row.xml"
    write_xml(commented_path, COMMENTED_XML)
    write_xml(clean_path, CLEAN_XML)
    write_xml(inline_path, INLINE_COMMENTS_XML)
    write_xml(caveat_path, CAVEAT_XML)

    spark = get_spark_session(
        app_name="spark-xml-comments",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ------------------------------------------------------------------
    # 1. Read XML with comments — intra-element comments are stripped
    # ------------------------------------------------------------------
    print("=== 1. Read XML with Comments ===")
    df_commented = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .schema(BOOK_SCHEMA)
        .load(commented_path.as_posix())
    )
    df_commented.printSchema()
    df_commented.show(truncate=False)
    print(f"Row count: {df_commented.count()}")
    print(
        "Note: Comments between elements, section dividers, and inline "
        "annotations (<!-- price is in USD -->) are all stripped.\n"
        "The <!-- <discount>15</discount> --> inside Clean Code is ignored."
    )

    # ------------------------------------------------------------------
    # 2. Verify no 'discount' column from commented-out child element
    # ------------------------------------------------------------------
    print("\n=== 2. No Ghost Columns from Commented-Out Child Elements ===")
    df_inferred = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .load(commented_path.as_posix())
    )
    print(f"Columns: {df_inferred.columns}")
    assert "discount" not in df_inferred.columns, "discount should not appear!"
    print("✓ 'discount' column correctly absent")

    # ------------------------------------------------------------------
    # 3. Compare commented vs clean XML — identical DataFrames
    # ------------------------------------------------------------------
    print("\n=== 3. Commented vs Clean XML — Same Result ===")
    df_clean = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .schema(BOOK_SCHEMA)
        .load(clean_path.as_posix())
    )
    df_clean.show(truncate=False)

    commented_rows = sorted([tuple(r) for r in df_commented.collect()])
    clean_rows = sorted([tuple(r) for r in df_clean.collect()])
    assert commented_rows == clean_rows, "DataFrames should be identical!"
    print("✓ Both files produce identical DataFrames")
    print(f"✓ Schemas match: {df_commented.schema == df_clean.schema}")

    # ------------------------------------------------------------------
    # 4. Inline comments adjacent to element text
    # ------------------------------------------------------------------
    print("\n=== 4. Inline Comments Next to Text ===")
    df_inline = (
        spark.read.format("xml")
        .option("rowTag", "record")
        .load(inline_path.as_posix())
    )
    df_inline.printSchema()
    df_inline.show(truncate=False)
    print(
        "Note: Comments adjacent to text are stripped. The remaining "
        "text content is preserved."
    )

    # ------------------------------------------------------------------
    # 5. CAVEAT: commented-out rowTag block IS still parsed
    # ------------------------------------------------------------------
    print("\n=== 5. ⚠ Caveat: Commented-Out rowTag Block IS Parsed ===")
    df_caveat = (
        spark.read.format("xml")
        .option("rowTag", "item")
        .load(caveat_path.as_posix())
    )
    df_caveat.show(truncate=False)
    print(f"Row count: {df_caveat.count()}  (expected 1, got {df_caveat.count()})")
    print(
        "spark-xml's Hadoop InputFormat splits rows by scanning for\n"
        "<rowTag> text BEFORE XML parsing.  A <rowTag> inside a multi-line\n"
        "comment is still matched by the splitter, so 'Ghost Item' appears.\n"
        "Workaround: never comment out entire rowTag blocks in XML files\n"
        "that will be read by spark-xml — delete them instead."
    )

    # ------------------------------------------------------------------
    # 6. Write round-trip — output never contains comments
    # ------------------------------------------------------------------
    print("\n=== 6. Write Round-Trip (output never has comments) ===")
    output_path = base / "books_output"
    (
        df_commented.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "bookstore")
        .option("rowTag", "book")
        .save(output_path.as_posix())
    )

    df_roundtrip = (
        spark.read.format("xml")
        .option("rowTag", "book")
        .schema(BOOK_SCHEMA)
        .load(output_path.as_posix())
    )
    df_roundtrip.show(truncate=False)

    roundtrip_rows = sorted([tuple(r) for r in df_roundtrip.collect()])
    assert commented_rows == roundtrip_rows, "Round-trip data mismatch!"
    print("✓ Round-trip data matches original")

    import glob

    written_files = glob.glob(str(output_path / "*.xml"))
    for wf in written_files:
        content = Path(wf).read_text(encoding="utf-8")
        assert "<!--" not in content, f"Comment found in {wf}!"
    print("✓ Written XML files contain no comment markers")

    # ------------------------------------------------------------------
    # 7. Queries work the same regardless of comments
    # ------------------------------------------------------------------
    print("\n=== 7. Queries on Commented Data ===")
    print("--- Technology books ---")
    df_commented.filter(F.col("genre") == "Technology").show(truncate=False)

    print("--- Average price by genre ---")
    df_commented.groupBy("genre").agg(
        F.count("*").alias("count"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.min("year").alias("earliest"),
        F.max("year").alias("latest"),
    ).orderBy("genre").show(truncate=False)

    spark.stop()
