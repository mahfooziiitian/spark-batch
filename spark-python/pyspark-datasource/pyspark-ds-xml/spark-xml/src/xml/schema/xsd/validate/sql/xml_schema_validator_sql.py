import os

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
        .appName("spark-xml-validator")
        .getOrCreate()
    )

    # data path
    data_home = os.environ["DATA_HOME"]
    xml_file = os.path.join(data_home, "file_data", "xml", "orders.xml")
    xml_xsd = os.path.join(data_home, "file_data", "xml", "orders.xsd")

    print(xml_file)

    # adding spark context
    spark.sparkContext.addFile(xml_xsd)

    #
    rootTag = "Root"
    rowTag = "Root"

    spark.sql(
        f"""
        create table orders
        USING xml
            OPTIONS (path "{xml_file}",
            rowTag "{rowTag}",
            inferSchema "false",
            rowValidationXSDPath "orders.xsd",
            rootTag "{rootTag}")
    """
    )
    spark.sql("select * from orders").printSchema()
