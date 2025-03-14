

if __name__ == '__main__':
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("XML Encoding utf16") \
        .getOrCreate()

    df = spark.read \
        .format("xml") \
        .option("rowTag", "rowTagName") \
        .option("encoding", "UTF-16") \
        .load("path/to/your/xmlfile.xml")

    df.show()

    df.write \
        .format("com.databricks.spark.xml") \
        .option("rootTag", "rootTagName") \
        .option("rowTag", "rowTagName") \
        .option("encoding", "UTF-8") \
        .save("path/to/output/xmlfile.xml")
