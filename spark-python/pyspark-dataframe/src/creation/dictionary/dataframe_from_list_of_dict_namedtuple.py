import os
import sys

from pyspark.sql import SparkSession
from collections import namedtuple

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    # Create a Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()

    final_list = []
    mylist = [{"type_activity_id": 1, "type_activity_name": "xxx"},
              {"type_activity_id": 2, "type_activity_name": "yyy"},
              {"type_activity_id": 3, "type_activity_name": "zzz"}
              ]
    ActivityTuple = namedtuple('activity', ['type_activity_id', 'type_activity_name'])

    for my_dict in mylist:
        namedtuple_obj = ActivityTuple(**my_dict)
        final_list.append(namedtuple_obj)

    spark.createDataFrame(final_list).show(truncate=False)
