"""Parse namespace-prefixed XML with ElementTree using namespace maps."""

import os
import xml.etree.ElementTree as ET

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

NS = {
    "bk": "http://example.com/books",
    "rv": "http://example.com/reviews",
}

SAMPLE_XML = """\
<bk:library xmlns:bk="http://example.com/books"
            xmlns:rv="http://example.com/reviews">
  <bk:book isbn="978-0-13-468599-1">
    <bk:title>The Pragmatic Programmer</bk:title>
    <bk:author>David Thomas</bk:author>
    <bk:year>2019</bk:year>
    <rv:reviews>
      <rv:review><rv:rating>5</rv:rating><rv:comment>Classic</rv:comment></rv:review>
      <rv:review><rv:rating>4</rv:rating><rv:comment>Great read</rv:comment></rv:review>
    </rv:reviews>
  </bk:book>
  <bk:book isbn="978-0-596-51774-8">
    <bk:title>JavaScript: The Good Parts</bk:title>
    <bk:author>Douglas Crockford</bk:author>
    <bk:year>2008</bk:year>
    <rv:reviews>
      <rv:review><rv:rating>4</rv:rating><rv:comment>Concise</rv:comment></rv:review>
    </rv:reviews>
  </bk:book>
  <bk:book isbn="978-0-13-235088-4">
    <bk:title>Clean Code</bk:title>
    <bk:author>Robert C. Martin</bk:author>
    <bk:year>2008</bk:year>
    <rv:reviews>
      <rv:review><rv:rating>5</rv:rating><rv:comment>Must read</rv:comment></rv:review>
      <rv:review><rv:rating>3</rv:rating><rv:comment>Somewhat dated</rv:comment></rv:review>
      <rv:review><rv:rating>4</rv:rating><rv:comment>Solid advice</rv:comment></rv:review>
    </rv:reviews>
  </bk:book>
</bk:library>
"""


def _text(el: ET.Element, path: str) -> str | None:
    node = el.find(path, NS)
    return node.text if node is not None else None


def extract_book(payload: str) -> dict[str, str | None]:
    """Extract book metadata from a namespaced XML element."""
    doc = ET.fromstring(payload)
    return {
        "isbn": doc.attrib.get("isbn"),
        "title": _text(doc, "bk:title"),
        "author": _text(doc, "bk:author"),
        "year": _text(doc, "bk:year"),
    }


def extract_review_ratings(payload: str) -> list[int]:
    """Extract all review ratings from a namespaced book element."""
    doc = ET.fromstring(payload)
    return [int(r.text) for r in doc.findall("rv:reviews/rv:review/rv:rating", NS) if r.text is not None]


BOOK_SCHEMA = StructType(
    [
        StructField("isbn", StringType(), True),
        StructField("title", StringType(), True),
        StructField("author", StringType(), True),
        StructField("year", StringType(), True),
    ]
)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("xml-etree-namespaces")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Register default namespace so tostring preserves prefixes
    for prefix, uri in NS.items():
        ET.register_namespace(prefix, uri)

    library = ET.fromstring(SAMPLE_XML)
    book_strings = [ET.tostring(b, encoding="unicode").strip() for b in library.findall("bk:book", NS)]

    rows = [Row(index=i, xml=xml) for i, xml in enumerate(book_strings)]
    book_df = spark.createDataFrame(rows)

    print("=== Raw XML rows ===")
    book_df.show(truncate=80)

    # Extract structured book info
    extract_book_udf = udf(extract_book, BOOK_SCHEMA)
    print("=== Extracted book metadata ===")
    (
        book_df.withColumn("book", extract_book_udf("xml"))
        .select("index", "book.isbn", "book.title", "book.author", "book.year")
        .show(truncate=False)
    )

    # Extract ratings and compute average per book
    extract_ratings_udf = udf(extract_review_ratings, ArrayType(StringType()))
    print("=== Review ratings exploded ===")
    ratings_df = (
        book_df.withColumn("book", extract_book_udf("xml"))
        .withColumn("ratings", extract_ratings_udf("xml"))
        .select("book.title", F.explode("ratings").alias("rating"))
    )
    ratings_df.show(truncate=False)

    print("=== Average rating per book ===")
    (
        ratings_df.groupBy("title")
        .agg(
            F.round(F.avg(F.col("rating").cast("int")), 1).alias("avg_rating"),
            F.count("rating").alias("review_count"),
        )
        .orderBy(F.desc("avg_rating"))
        .show(truncate=False)
    )

    spark.stop()
