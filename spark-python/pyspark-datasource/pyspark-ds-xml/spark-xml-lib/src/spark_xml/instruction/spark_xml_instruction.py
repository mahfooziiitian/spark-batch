"""Demonstrate how spark-xml handles XML processing instructions (PIs).

Processing instructions (``<?target data?>``) are directives embedded
in XML for applications — stylesheets, encoding hints, app-specific
config, etc. spark-xml **silently skips** all PIs and parses only
element data into DataFrame rows. This script shows:

1. Reading XML that contains various processing instructions
2. PIs in the prolog, between rows, and inside row elements
3. Verifying PIs never appear as DataFrame columns or values
4. Comparing PI-laden XML with clean XML — identical results
5. Writing XML — output includes the XML declaration PI only
6. Explicit schema with PI-containing XML
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

# ── XML with processing instructions in various positions ───────────────

PI_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <?xml-stylesheet type="text/xsl" href="notes.xsl"?>
    <?app-config theme="dark" version="2.1"?>
    <notes>
      <?section-start name="personal"?>
      <note>
        <id>1</id>
        <to>Alice</to>
        <from>Bob</from>
        <heading>Reminder</heading>
        <body>Meeting at 10am tomorrow.</body>
        <priority>1</priority>
        <score>9.5</score>
      </note>
      <?page-break?>
      <note>
        <id>2</id>
        <to>Carol</to>
        <from>David</from>
        <heading>Lunch</heading>
        <body>Are you free for lunch?</body>
        <priority>2</priority>
        <score>7.0</score>
      </note>
      <?section-end name="personal"?>
      <?section-start name="work"?>
      <note>
        <?format bold="true"?>
        <id>3</id>
        <to>Eve</to>
        <from>Alice</from>
        <heading>Project Update</heading>
        <body>Sprint review on Friday.</body>
        <priority>1</priority>
        <score>8.8</score>
      </note>
      <note>
        <id>4</id>
        <to>Bob</to>
        <from>Carol</from>
        <heading>Feedback</heading>
        <body>Great presentation!</body>
        <?highlight color="yellow"?>
        <priority>3</priority>
        <score>6.5</score>
      </note>
      <?section-end name="work"?>
    </notes>
""")

# Same data with no PIs at all (for comparison)
CLEAN_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <notes>
      <note>
        <id>1</id>
        <to>Alice</to>
        <from>Bob</from>
        <heading>Reminder</heading>
        <body>Meeting at 10am tomorrow.</body>
        <priority>1</priority>
        <score>9.5</score>
      </note>
      <note>
        <id>2</id>
        <to>Carol</to>
        <from>David</from>
        <heading>Lunch</heading>
        <body>Are you free for lunch?</body>
        <priority>2</priority>
        <score>7.0</score>
      </note>
      <note>
        <id>3</id>
        <to>Eve</to>
        <from>Alice</from>
        <heading>Project Update</heading>
        <body>Sprint review on Friday.</body>
        <priority>1</priority>
        <score>8.8</score>
      </note>
      <note>
        <id>4</id>
        <to>Bob</to>
        <from>Carol</from>
        <heading>Feedback</heading>
        <body>Great presentation!</body>
        <priority>3</priority>
        <score>6.5</score>
      </note>
    </notes>
""")

NOTE_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("to", StringType(), True),
    StructField("from", StringType(), True),
    StructField("heading", StringType(), True),
    StructField("body", StringType(), True),
    StructField("priority", LongType(), True),
    StructField("score", DoubleType(), True),
])


def write_xml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "instruction_demo"

    pi_path = base / "notes_with_pi.xml"
    clean_path = base / "notes_clean.xml"
    write_xml(pi_path, PI_XML)
    write_xml(clean_path, CLEAN_XML)

    spark = get_spark_session(
        app_name="spark-xml-processing-instructions",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # ------------------------------------------------------------------
    # 1. Read XML with processing instructions
    # ------------------------------------------------------------------
    print("=== 1. Read XML Containing Processing Instructions ===")
    df_pi = (
        spark.read.format("xml")
        .option("rowTag", "note")
        .schema(NOTE_SCHEMA)
        .load(pi_path.as_posix())
    )
    df_pi.printSchema()
    df_pi.show(truncate=False)
    print(f"Row count: {df_pi.count()}")
    print(
        "Processing instructions (xml-stylesheet, app-config, section-start,\n"
        "page-break, format, highlight) are all silently skipped."
    )

    # ------------------------------------------------------------------
    # 2. Verify no PI artifacts in columns or data
    # ------------------------------------------------------------------
    print("\n=== 2. No PI Artifacts in Schema ===")
    df_inferred = (
        spark.read.format("xml")
        .option("rowTag", "note")
        .load(pi_path.as_posix())
    )
    print(f"Columns: {df_inferred.columns}")

    pi_keywords = {"stylesheet", "app-config", "section", "page-break",
                   "format", "highlight"}
    leaked = {c for c in df_inferred.columns if any(k in c.lower() for k in pi_keywords)}
    assert not leaked, f"PI artifacts found in columns: {leaked}"
    print("✓ No PI-related columns in schema")

    # Check no PI text leaked into values
    for col_name in df_inferred.columns:
        vals = [
            str(r[col_name]) for r in df_inferred.collect()
            if r[col_name] is not None
        ]
        for v in vals:
            assert "<?" not in v, f"PI text found in {col_name}: {v}"
    print("✓ No PI text in any cell values")

    # ------------------------------------------------------------------
    # 3. Compare with clean XML — identical results
    # ------------------------------------------------------------------
    print("\n=== 3. PI XML vs Clean XML — Same Result ===")
    df_clean = (
        spark.read.format("xml")
        .option("rowTag", "note")
        .schema(NOTE_SCHEMA)
        .load(clean_path.as_posix())
    )

    pi_rows = sorted([tuple(r) for r in df_pi.collect()])
    clean_rows = sorted([tuple(r) for r in df_clean.collect()])
    assert pi_rows == clean_rows, "DataFrames differ!"
    print("✓ Both files produce identical DataFrames")
    print(f"✓ Schemas match: {df_pi.schema == df_clean.schema}")

    # ------------------------------------------------------------------
    # 4. Write round-trip
    # ------------------------------------------------------------------
    print("\n=== 4. Write Round-Trip ===")
    output_path = base / "notes_output"
    (
        df_pi.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "notes")
        .option("rowTag", "note")
        .save(output_path.as_posix())
    )

    df_roundtrip = (
        spark.read.format("xml")
        .option("rowTag", "note")
        .schema(NOTE_SCHEMA)
        .load(output_path.as_posix())
    )
    df_roundtrip.show(truncate=False)

    roundtrip_rows = sorted([tuple(r) for r in df_roundtrip.collect()])
    assert pi_rows == roundtrip_rows, "Round-trip data mismatch!"
    print("✓ Round-trip data matches original")

    # Verify written files have no custom PIs (only the XML declaration)
    import glob

    written_files = glob.glob(str(output_path / "*.xml"))
    for wf in written_files:
        content = Path(wf).read_text(encoding="utf-8")
        for line in content.splitlines():
            stripped = line.strip()
            if stripped.startswith("<?") and not stripped.startswith("<?xml "):
                raise AssertionError(f"Custom PI found in output: {stripped}")
    print("✓ Written XML contains no custom processing instructions")

    # ------------------------------------------------------------------
    # 5. Queries on PI-laden data
    # ------------------------------------------------------------------
    print("\n=== 5. Queries ===")
    print("--- High priority notes (priority = 1) ---")
    df_pi.filter(F.col("priority") == 1).select(
        "id", "heading", "to", "score"
    ).show(truncate=False)

    print("--- Summary by priority ---")
    df_pi.groupBy("priority").agg(
        F.count("*").alias("count"),
        F.round(F.avg("score"), 2).alias("avg_score"),
        F.collect_list("heading").alias("headings"),
    ).orderBy("priority").show(truncate=False)

    spark.stop()
