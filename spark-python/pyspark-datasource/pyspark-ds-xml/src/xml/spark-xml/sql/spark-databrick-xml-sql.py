import os

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark_warehouse_dir = os.environ["SPARK_WAREHOUSE"]
    spark = (SparkSession.builder.master("local[*]")
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .config('spark.sql.warehouse.dir', spark_warehouse_dir)
             .appName("spark-db-xml").getOrCreate())
    dataHome = os.environ["DATA_HOME"]
    xmlFile = (dataHome + "\\FileData\\Xml\\movies.xml").replace("\\", "/")
    spark.sql(
        f"""CREATE TABLE movies USING xml OPTIONS(path 'file:///{xmlFile}', rootTag 'collection', rowTag 'movie')""")

    spark.sql("select * from movies").show(truncate=False)
