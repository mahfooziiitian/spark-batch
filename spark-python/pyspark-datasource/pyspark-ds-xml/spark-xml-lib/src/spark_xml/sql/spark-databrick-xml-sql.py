"""Demonstrate creating a Spark SQL table backed by XML using CREATE TABLE USING xml.

Generates a sample movies XML file, then registers it as a SQL table
and queries it with plain SQL.
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
    <collection>
      <movie id="1">
        <title>The Spark Chronicles</title>
        <director>Alice Johnson</director>
        <genre>Sci-Fi</genre>
        <year>2023</year>
        <rating>8.5</rating>
        <duration>142</duration>
      </movie>
      <movie id="2">
        <title>Data Lake Monster</title>
        <director>Bob Smith</director>
        <genre>Horror</genre>
        <year>2024</year>
        <rating>7.2</rating>
        <duration>98</duration>
      </movie>
      <movie id="3">
        <title>The Pipeline</title>
        <director>Carol Lee</director>
        <genre>Thriller</genre>
        <year>2022</year>
        <rating>9.0</rating>
        <duration>130</duration>
      </movie>
      <movie id="4">
        <title>Schema Evolution</title>
        <director>David Chen</director>
        <genre>Drama</genre>
        <year>2025</year>
        <rating>8.1</rating>
        <duration>115</duration>
      </movie>
      <movie id="5">
        <title>Batch vs Stream</title>
        <director>Eve Martin</director>
        <genre>Action</genre>
        <year>2024</year>
        <rating>7.8</rating>
        <duration>125</duration>
      </movie>
    </collection>
""")


def generate_sample_xml(file_path: Path) -> None:
    """Write the movies sample XML to *file_path*."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(SAMPLE_XML, encoding="utf-8")
    print(f"Generated sample XML → {file_path}")


if __name__ == "__main__":
    data_path = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "movies.xml"
    generate_sample_xml(data_path)

    spark_warehouse_dir = os.environ["SPARK_WAREHOUSE"]
    spark = get_spark_session(
        app_name="spark-xml-sql",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    xml_file = data_path.as_posix()

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS movies USING xml "
        f"OPTIONS(path 'file:///{xml_file}', rootTag 'collection', rowTag 'movie')"
    )

    print("\n=== SELECT * FROM movies ===")
    spark.sql("SELECT * FROM movies").show(truncate=False)

    print("\n=== Top rated movies (rating > 8.0) ===")
    spark.sql("SELECT title, director, rating FROM movies WHERE rating > 8.0 ORDER BY rating DESC").show(
        truncate=False
    )

    spark.sql("DROP TABLE IF EXISTS movies")
    spark.stop()

