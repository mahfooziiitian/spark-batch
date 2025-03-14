import os
import sys
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    spark = (SparkSession.builder.master("local[*]")
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .appName("spark-db-xml").getOrCreate())
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "\\FileData\\Xml\\people.xml.gz"

    # Example DataFrame
    data = [("John", 28), ("Anna", 23), ("Peter", 34)]
    columns = ["Name", "Age"]
    df1 = spark.createDataFrame(data, columns)

    # Write the DataFrame to a compressed XML file
    (df1.write.format("xml")
     .mode("overwrite")
     .option("rootTag", "people")
     .option("rowTag", "person")
     .option("compression", "gzip")
     .save(xmlFile))

    df2 = (spark.read.format("xml")
           .option("rowTag", "person")
           .option("compression", "gzip")
           .load(xmlFile))

    # Show the data
    df2.show()
