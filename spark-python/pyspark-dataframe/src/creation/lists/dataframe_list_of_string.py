import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

if __name__ == '__main__':
    spark = SparkSession.builder.appName("example").getOrCreate()

    # notice the variable name (more below)
    mylist = [1, 2, 3, 4]

    # notice the parens after the type name
    spark.createDataFrame(mylist, IntegerType()).show()
