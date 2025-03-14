import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Sample list of dictionaries
    mylist = [
        {"type_activity_id": 1, "type_activity_name": "xxx"},
        {"type_activity_id": 2, "type_activity_name": "yyy"},
        {"type_activity_id": 3, "type_activity_name": "zzz"}
    ]
    my_json = spark.sparkContext.parallelize(mylist)
    df = spark.read.json(my_json)

    df.printSchema()

    # Show DataFrame
    df.show()

    spark.stop()