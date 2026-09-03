import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_xml.util.sample_data import ensure_data_array_of_structs_xml

if __name__ == "__main__":
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xmlFile = str(ensure_data_array_of_structs_xml(data_home / "file_data" / "xml" / "data.xml"))

    spark = SparkSession.builder.appName("array_of_struct_array_of_string").master("local[*]").getOrCreate()
    df = spark.read.format("xml").option("rootTag", "root").option("rowTag", "arrayOfStructs").load(xmlFile)

    df.printSchema()

    print(df.schema.json())

    arrayOfStructsDF = df.select(F.explode(F.col("struct")).alias("struct"))

    arrayOfStructsDF.printSchema()

    arrayOfStringsDF = arrayOfStructsDF.select(
        F.concat_ws(",", F.col("struct.field1"), F.col("struct.field2")).alias("arrayOfStrings")
    )
    arrayOfStrings = arrayOfStringsDF.collect()
