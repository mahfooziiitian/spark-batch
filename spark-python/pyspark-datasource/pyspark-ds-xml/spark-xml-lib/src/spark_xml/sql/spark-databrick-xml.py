"""Demonstrate reading XML with spark-xml DataFrame API.

Generates a sample movies XML file and reads it using
the com.databricks.spark.xml format with rootTag/rowTag options.
"""

import os
import sys
from pathlib import Path

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

# Reuse the same movies XML from the SQL sibling script
from importlib.util import module_from_spec, spec_from_file_location

_spec = spec_from_file_location(
    "sql_mod", Path(__file__).parent / "spark-databrick-xml-sql.py"
)
_mod = module_from_spec(_spec)
_spec.loader.exec_module(_mod)
SAMPLE_XML = _mod.SAMPLE_XML
generate_sample_xml = _mod.generate_sample_xml

if __name__ == "__main__":
    data_path = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "movies.xml"
    generate_sample_xml(data_path)

    spark = get_spark_session(
        app_name="spark-xml-dataframe",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    movies = (
        spark.read.format("com.databricks.spark.xml")
        .option("rootTag", "collection")
        .option("rowTag", "movie")
        .load(data_path.as_posix())
    )

    print("\n=== Schema ===")
    movies.printSchema()

    print("\n=== Data ===")
    movies.show(truncate=False)

    spark.stop()
