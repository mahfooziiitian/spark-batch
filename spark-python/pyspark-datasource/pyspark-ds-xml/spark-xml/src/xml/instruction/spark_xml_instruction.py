import os
import sys

from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    data_home = os.environ["DATA_HOME"]
    xmlFile = os.path.join(data_home, "file_data", "xml", "notes.xml")
    print(os.environ["JAVA_HOME"])

    spark = (
        SparkSession.builder.appName("instruction")
        .master("local[*]")
        .getOrCreate()
    )

    df = spark.read.format("xml").option("rowTag", "row").load(xmlFile)

    df.printSchema()

    (df.show(truncate=False))
