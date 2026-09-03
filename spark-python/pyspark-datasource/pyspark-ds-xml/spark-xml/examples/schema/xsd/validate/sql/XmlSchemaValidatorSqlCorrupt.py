import os
from pathlib import Path

from pyspark.sql import SparkSession

from spark_xml.util.sample_data import ensure_orders_invalid_format_xml, ensure_orders_xsd

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-xml-validator").getOrCreate()

    # data path
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlFile = ensure_orders_invalid_format_xml(data_home / "file_data" / "xml" / "orders_invalid_format.xml").as_uri()
    xmlXsd = ensure_orders_xsd(data_home / "file_data" / "xml" / "orders.xsd").as_uri()

    print(xmlFile)

    # adding spark context
    spark.sparkContext.addFile(xmlXsd)

    #
    rootTag = "Root"
    rowTag = "Root"

    try:
        spark.sql(f"""
            create table orders
            USING xml
                OPTIONS (path "{xmlFile}", rowTag "{rowTag}",
                rowValidationXSDPath "orders.xsd",
                rootTag "{rootTag}",
                mode "FAILFAST")
        """)
        spark.sql("select * from orders").printSchema()
    except Exception as exc:  # noqa: BLE001 - demonstrating XSD validation failure handling
        print(f"XSD validation failed as expected (FAILFAST): {exc}")
    finally:
        spark.stop()
