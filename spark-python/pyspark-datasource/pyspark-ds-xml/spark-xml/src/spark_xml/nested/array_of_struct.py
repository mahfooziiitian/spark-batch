"""Demonstrate reading XML with an array of structs and exploding nested books.

Generates sample XML with <catalog> rows containing a <dt_creation> and
a nested array of <book> elements, then explodes books into flat rows.
"""

import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql.functions import col, explode

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

SAMPLE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <catalogs>
      <catalog>
        <dt_creation>2024-01-15</dt_creation>
        <book id="bk101">
          <author>Alice Johnson</author>
          <title>Learning PySpark</title>
          <genre>Technology</genre>
          <price>45.99</price>
          <publish_date>2024-03-01</publish_date>
        </book>
        <book id="bk102">
          <author>Bob Smith</author>
          <title>XML Processing with Spark</title>
          <genre>Data Engineering</genre>
          <price>39.50</price>
          <publish_date>2023-11-20</publish_date>
        </book>
        <book id="bk103">
          <author>Carol Lee</author>
          <title>Big Data Patterns</title>
          <genre>Computer Science</genre>
          <price>52.00</price>
          <publish_date>2024-06-10</publish_date>
        </book>
      </catalog>
      <catalog>
        <dt_creation>2025-02-20</dt_creation>
        <book id="bk201">
          <author>David Chen</author>
          <title>Streaming Analytics</title>
          <genre>Data Engineering</genre>
          <price>48.75</price>
          <publish_date>2025-01-15</publish_date>
        </book>
        <book id="bk202">
          <author>Eve Martin</author>
          <title>Graph Databases</title>
          <genre>Database</genre>
          <price>41.00</price>
          <publish_date>2025-03-05</publish_date>
        </book>
      </catalog>
    </catalogs>
""")


def generate_sample_xml(file_path: Path) -> None:
    """Write the books catalog sample XML to *file_path*."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(SAMPLE_XML, encoding="utf-8")
    print(f"Generated sample XML → {file_path}")


if __name__ == "__main__":
    data_path = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "books.xml"
    generate_sample_xml(data_path)

    spark = get_spark_session(
        app_name="spark-xml-array-of-struct",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    df = (
        spark.read.format("com.databricks.spark.xml")
        .option("rowTag", "catalog")
        .load(data_path.as_posix())
    )

    print("\n=== Raw Schema ===")
    df.printSchema()

    print("\n=== Exploded Books ===")
    (
        df.withColumn("book", explode(col("book")))
        .select("dt_creation", "book.*")
        .show(truncate=False)
    )

    spark.stop()
