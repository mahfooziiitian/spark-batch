import os
import sys

from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    # Create SparkSession
    spark = (SparkSession.builder.master("local[*]")
             .appName('variable_keys')
             .getOrCreate())
