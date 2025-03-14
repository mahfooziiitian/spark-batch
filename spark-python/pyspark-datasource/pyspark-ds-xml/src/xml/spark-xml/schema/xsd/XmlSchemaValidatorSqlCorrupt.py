import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0') \
        .appName("spark-xml-validator").getOrCreate()

    # data path
    dataHome = os.environ["DATA_HOME"]
    xmlFile = "file:///"+(dataHome + "\\FileData\\Xml\\orders_invalid_format.xml").replace("\\", "/")
    xmlXsd = "file:///"+(dataHome + "\\FileData\\Xml\\orders.xsd").replace("\\", "/")

    print(xmlFile)

    # adding spark context
    spark.sparkContext.addFile(xmlXsd)

    #
    rootTag = "Root"
    rowTag = "Root"

    spark.sql(f"""
        create table orders
        USING xml
            OPTIONS (path "{xmlFile}", rowTag "{rowTag}", 
            rowValidationXSDPath "orders.xsd",
            rootTag "{rootTag}",
            mode "FAILFAST")
    """)
    spark.sql("select * from orders").printSchema()


