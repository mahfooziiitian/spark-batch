"""Demonstrate spark-xml ignoreNamespace option.

Generates a sample XML file with multiple namespace prefixes,
then reads it with ignoreNamespace=true so column names are
clean (no prefixes).
"""

import os
import sys
import textwrap
from pathlib import Path

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

SAMPLE_XML = textwrap.dedent("""\
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


def generate_sample_xml(file_path: Path) -> None:
    """Write the namespaced sample XML to *file_path*."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(SAMPLE_XML, encoding="utf-8")
    print(f"Generated sample XML → {file_path}")


if __name__ == "__main__":
    spark = get_spark_session(
        app_name="spark-xml-ignore-namespace",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    data_path = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "ns_books.xml"
    generate_sample_xml(data_path)

    # --- ignoreNamespace = true  → clean column names (no prefixes) ---
    df = (
        spark.read.format("xml")
        .option("rowTag", "bk:book")
        .option("ignoreNamespace", "true")
        .load(data_path.as_posix())
    )

    print("\n=== Schema (ignoreNamespace=true) ===")
    df.printSchema()

    print("=== Data (ignoreNamespace=true) ===")
    df.show(truncate=False)

    # Access nested namespace-stripped fields
    print("=== Flattened view ===")
    df.selectExpr(
        "_id as book_id",
        "title",
        "author",
        "genre",
        "price._VALUE as price",
        "price._currency as currency",
        "publisher.name as publisher_name",
        "publisher.year as pub_year",
        "review.rating",
        "review.comment",
    ).show(truncate=False)

    spark.stop()
