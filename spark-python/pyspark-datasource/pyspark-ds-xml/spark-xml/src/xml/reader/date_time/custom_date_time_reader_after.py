from pyspark.sql.functions import to_date, to_timestamp
import os
import sys

from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_8"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    data_home = os.environ["DATA_HOME"]
    xml_file = os.path.join(
        data_home, "file_data", "xml", "date_time", "sample_data.xml"
    )
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
        .appName("spark-db-xml-date-time")
        .getOrCreate()
    )
    df_raw = spark.read.format("xml").option("rowTag", "record").load(xml_file)

    # Convert string fields into proper Date and Timestamp
    df = df_raw.withColumn(
        "birth_date", to_date("birth_date", "dd-MM-yyyy")
    ).withColumn("created_at", to_timestamp("created_at", "MM/dd/yyyy HH:mm"))

    df.show()
    df.printSchema()
