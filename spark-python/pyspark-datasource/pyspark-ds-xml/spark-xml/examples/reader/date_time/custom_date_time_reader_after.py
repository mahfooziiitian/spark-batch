import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp

from spark_xml.util.sample_data import ensure_date_time_sample_xml

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xml_file = str(ensure_date_time_sample_xml(data_home / "file_data" / "xml" / "date_time" / "sample_data.xml"))
    spark = SparkSession.builder.master("local[*]").appName("spark-db-xml-date-time").getOrCreate()
    df_raw = spark.read.format("xml").option("rowTag", "record").load(xml_file)

    # Convert string fields into proper Date and Timestamp
    df = df_raw.withColumn("birth_date", to_date("birth_date", "dd-MM-yyyy")).withColumn(
        "created_at", to_timestamp("created_at", "MM/dd/yyyy HH:mm")
    )

    df.show()
    df.printSchema()
