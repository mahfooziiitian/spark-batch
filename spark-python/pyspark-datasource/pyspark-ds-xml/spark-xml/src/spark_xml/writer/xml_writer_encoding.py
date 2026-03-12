"""XML writer with encoding options.

Demonstrates writing XML with different character encodings
(UTF-8, UTF-16, ISO-8859-1) and handling of special/international characters.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def create_international_df(spark: SparkSession):
    """Create a DataFrame with international characters."""
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("city", StringType()),
        StructField("greeting", StringType()),
        StructField("currency_symbol", StringType()),
    ])
    data = [
        (1, "José García", "Madrid", "¡Hola!", "€"),
        (2, "François Müller", "Zürich", "Grüezi!", "CHF"),
        (3, "田中太郎", "東京", "こんにちは", "¥"),
        (4, "Ólafur Björnsson", "Reykjavík", "Halló!", "kr"),
        (5, "Münevver Çelik", "İstanbul", "Merhaba!", "₺"),
        (6, "Александр Иванов", "Москва", "Привет!", "₽"),
        (7, "李小明", "北京", "你好!", "¥"),
        (8, "Ångström Lindén", "Malmö", "Hej!", "kr"),
    ]
    return spark.createDataFrame(data, schema)


if __name__ == "__main__":
    out_dir = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "writer_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session(
        app_name="xml-writer-encoding",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    df = create_international_df(spark)
    print("=== Source DataFrame (international characters) ===")
    df.show(truncate=False)

    # ── 1. Default UTF-8 encoding ──────────────────────────────────
    print("\n=== 1. UTF-8 (default) ===")
    utf8_path = (out_dir / "encoding_utf8").as_posix()
    (
        df.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "people")
        .option("rowTag", "person")
        .option("declaration", 'xml version="1.0" encoding="UTF-8"')
        .save(utf8_path)
    )
    df_utf8 = spark.read.format("xml").option("rowTag", "person").load(utf8_path)
    print("Round-trip UTF-8:")
    df_utf8.show(truncate=False)

    # ── 2. ISO-8859-1 encoding (Latin-1) ───────────────────────────
    # Note: ISO-8859-1 cannot represent CJK or Cyrillic — use Latin subset
    print("\n=== 2. ISO-8859-1 (Latin characters only) ===")
    latin_schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("city", StringType()),
        StructField("greeting", StringType()),
    ])
    latin_data = [
        (1, "José García", "Madrid", "¡Hola!"),
        (2, "François Müller", "Zürich", "Grüezi!"),
        (4, "Ólafur Björnsson", "Reykjavík", "Halló!"),
        (8, "Ångström Lindén", "Malmö", "Hej!"),
    ]
    df_latin = spark.createDataFrame(latin_data, latin_schema)

    iso_path = (out_dir / "encoding_iso8859").as_posix()
    (
        df_latin.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "people")
        .option("rowTag", "person")
        .option("declaration", 'xml version="1.0" encoding="ISO-8859-1"')
        .save(iso_path)
    )
    df_iso = (
        spark.read.format("xml")
        .option("rowTag", "person")
        .option("charset", "ISO-8859-1")
        .load(iso_path)
    )
    print("Round-trip ISO-8859-1:")
    df_iso.show(truncate=False)

    # ── 3. Special XML characters (auto-escaped) ───────────────────
    print("\n=== 3. Special XML characters (auto-escaped by spark-xml) ===")
    special_schema = StructType([
        StructField("id", IntegerType()),
        StructField("expression", StringType()),
        StructField("description", StringType()),
    ])
    special_data = [
        (1, "x < 10 && y > 5", "Less-than and greater-than"),
        (2, 'He said "hello" & waved', "Ampersand and quotes"),
        (3, "A <tag> inside text", "Angle brackets in value"),
        (4, "Tom & Jerry's \"show\"", "Mixed special chars"),
        (5, "100% → success ← done", "Unicode arrows and percent"),
    ]
    df_special = spark.createDataFrame(special_data, special_schema)
    df_special.show(truncate=False)

    special_path = (out_dir / "encoding_special_chars").as_posix()
    (
        df_special.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "expressions")
        .option("rowTag", "item")
        .save(special_path)
    )
    df_special_back = spark.read.format("xml").option("rowTag", "item").load(special_path)
    print("Round-trip (special chars auto-escaped/unescaped):")
    df_special_back.show(truncate=False)

    spark.stop()
