import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
# Initialize Spark Session
spark = SparkSession.builder.appName("FilterDF").getOrCreate()

# Sample DataFrame
data = [
    (1, "USA", "California"),
    (2, "USA", "Texas"),
    (3, "India", "Maharashtra"),
    (4, "India", "Karnataka"),
    (5, "Canada", "Ontario"),
    (6, "Canada", "Quebec"),
]

df = spark.createDataFrame(data, ["id", "country", "state"])
df.show()

df.createOrReplaceTempView("locations")

spark.sql(
    """SELECT array(array('a','b')) FROM locations
"""
).show()
