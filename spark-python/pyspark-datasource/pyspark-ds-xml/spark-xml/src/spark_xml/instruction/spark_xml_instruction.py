"""XML processing instructions — read XML containing PIs.

Demonstrates that spark-xml skips processing instructions
(e.g. <?xml-stylesheet?>, <?app-config?>) and parses only
the element data into DataFrame rows.
"""

import os
import sys

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    data_home = os.environ["DATA_HOME"]
    xml_file = os.path.join(
        data_home,
        "file_data",
        "xml",
        "notes_with_instructions.xml",
    )

    spark = get_spark_session(
        app_name="spark-xml-instruction",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # spark-xml ignores processing instructions and reads
    # only the <note> elements as rows
    df = (
        spark.read.format("xml")
        .option("rowTag", "note")
        .load(xml_file)
    )
    df.printSchema()
    df.show(truncate=False)

    spark.stop()
