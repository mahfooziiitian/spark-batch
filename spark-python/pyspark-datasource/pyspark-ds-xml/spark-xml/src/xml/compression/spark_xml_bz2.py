import os
import sys

from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]").appName("spark-db-xml").getOrCreate()
    )
    dataHome = os.environ["DATA_HOME"]
    xmlFile = dataHome + "/file_data/xml/people.xml.bz2"

    # Example DataFrame
    data = [("John", 28), ("Anna", 23), ("Peter", 34)]
    columns = ["Name", "Age"]
    df1 = spark.createDataFrame(data, columns)

    # Write the DataFrame to a compressed XML file
    (
        df1.write.format("xml")
        .mode("overwrite")
        .option("rootTag", "people")
        .option("rowTag", "person")
        .option("compression", "bzip2")
        .save(xmlFile)
    )

    df2 = (
        spark.read.format("xml")
        .option("rowTag", "person")
        .option("compression", "bzip2")
        .load(xmlFile)
    )

    # Show the data
    df2.show()
