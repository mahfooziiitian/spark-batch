import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    data_home = os.environ["DATA_HOME"]
    xml_file = os.path.join(
        data_home, "file_data", "xml", "date_time", "non_iso_timestamp.xml"
    )
    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("_corrupt_record", StringType()),
        ]
    )

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("spark-db-xml-date-time")
        .getOrCreate()
    )
    df = (
        spark.read.format("xml")
        .option("rowTag", "record")
        .option("dateFormat", "dd-MM-yyyy")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(xml_file)
    )

    df.show(truncate=False)
    df.printSchema()
