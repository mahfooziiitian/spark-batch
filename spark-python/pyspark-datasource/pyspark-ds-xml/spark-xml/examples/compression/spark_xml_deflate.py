"""Write and read back an XML file using the built-in deflate compression codec.

Demonstrates round-tripping a DataFrame through the native Spark 4 ``xml`` data
source with ``compression=deflate``. Self-contained and runnable:

    uv run python examples/compression/spark_xml_deflate.py

Override the output location with ``XML_OUTPUT_DIR`` (defaults to ``/tmp/spark-xml``).
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("spark-xml-deflate")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    out_dir = Path(os.environ.get("XML_OUTPUT_DIR", "/tmp/spark-xml"))
    out_dir.mkdir(parents=True, exist_ok=True)
    xml_file = str(out_dir / "people.xml.deflate")

    data = [("John", 28), ("Anna", 23), ("Peter", 34)]
    df1 = spark.createDataFrame(data, ["Name", "Age"])

    # Write compressed XML with the built-in "xml" source.
    (
        df1.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "people")
        .option("rowTag", "person")
        .option("compression", "deflate")
        .save(xml_file)
    )

    # Read it back — Spark auto-detects the deflate codec.
    df2 = spark.read.format("xml").option("rowTag", "person").load(xml_file)
    df2.show()

    spark.stop()
