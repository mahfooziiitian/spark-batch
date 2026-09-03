import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("spark-db-xml").getOrCreate()
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlFile = str(data_home / "file_data" / "xml" / "people.xml.snappy")
    Path(xmlFile).parent.mkdir(parents=True, exist_ok=True)

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
        .option("compression", "snappy")
        .save(xmlFile)
    )

    df2 = spark.read.format("xml").option("rowTag", "person").option("compression", "snappy").load(xmlFile)

    # Show the data
    df2.show()
