"""Demonstrate spark-xml with namespace prefixes preserved.

Uses the same namespaced sample data as ignore_namespace_xml.py but
reads with ignoreNamespace=false so column names keep their prefixes
(e.g. bk:title, pub:name, rev:rating).
"""

import os
import sys
from pathlib import Path

from spark_xml.namespace.ignore_namespace_xml import generate_sample_xml
from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = get_spark_session(
        app_name="spark-xml-preserve-namespace",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    data_path = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "ns_books.xml"
    generate_sample_xml(data_path)

    # --- ignoreNamespace = false → prefixed column names ---
    df = (
        spark.read.format("xml")
        .option("rowTag", "bk:book")
        .option("ignoreNamespace", "false")
        .load(data_path.as_posix())
    )

    print("\n=== Schema (ignoreNamespace=false) ===")
    df.printSchema()

    print("=== Data (ignoreNamespace=false) ===")
    df.show(truncate=False)

    spark.stop()
