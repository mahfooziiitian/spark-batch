import os
from pathlib import Path

from pyspark.sql import SparkSession

from spark_xml.util.sample_data import ensure_orders_xml, ensure_orders_xsd

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-xml-validator").getOrCreate()

    # data path
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xml_file = str(ensure_orders_xml(data_home / "file_data" / "xml" / "orders.xml"))
    xml_xsd = str(ensure_orders_xsd(data_home / "file_data" / "xml" / "orders.xsd"))

    print(xml_file)

    # adding spark context
    spark.sparkContext.addFile(xml_xsd)

    #
    rootTag = "Root"
    rowTag = "Root"

    spark.sql(f"""
        create table orders
        USING xml
            OPTIONS (path "{xml_file}",
            rowTag "{rowTag}",
            inferSchema "false",
            rowValidationXSDPath "orders.xsd",
            rootTag "{rootTag}")
    """)
    spark.sql("select * from orders").printSchema()
