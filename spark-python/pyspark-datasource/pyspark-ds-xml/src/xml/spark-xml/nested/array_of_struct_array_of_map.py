import os

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

if __name__ == '__main__':
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "\\FileData\\Xml\\data.xml"
    print(os.environ["JAVA_HOME"])

    spark = (SparkSession.builder
             .appName("array_of_struct_array_of_string")
             .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0')
             .master("local[*]")
             .getOrCreate())
    df = (spark.read.format("com.databricks.spark.xml")
          .option("rootTag", "root")
          .option("rowTag", "arrayOfStructs")
          .load(xmlFile)
          )

    df.printSchema()

    print(df.schema.json())

    arrayOfStructsDF = df.select(explode(col("struct")).alias("struct"))

    arrayOfStructsDF.printSchema()

    arrayOfStringsDF = arrayOfStructsDF.select(
        concat_ws(",",
                  col("struct.field1"),
                  col("struct.field2")).alias("arrayOfStrings")
    )
    arrayOfStrings = arrayOfStringsDF.collect()
