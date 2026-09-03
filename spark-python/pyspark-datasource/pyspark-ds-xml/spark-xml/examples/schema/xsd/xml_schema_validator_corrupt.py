import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from spark_xml.util.sample_data import ensure_orders_corrupt_xml, ensure_orders_xsd

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-xml-validator").getOrCreate()

    # data path
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlFile = str(ensure_orders_corrupt_xml(data_home / "file_data" / "xml" / "orders_corrupt.xml"))
    xmlXsd = str(ensure_orders_xsd(data_home / "file_data" / "xml" / "orders.xsd"))

    # adding spark context
    spark.sparkContext.addFile(xmlXsd)

    #
    try:
        orders = (
            spark.read.format("xml")
            .option("rowTag", "Root")
            .option("mode", "FAILFAST")
            .option("rowValidationXSDPath", "orders.xsd")
            .load(xmlFile)
        )
        orders.show(truncate=False)
        orders.printSchema()
    except Exception as exc:  # noqa: BLE001 - demonstrating XSD validation failure handling
        print(f"XSD validation failed as expected (FAILFAST): {exc}")
    finally:
        spark.stop()
