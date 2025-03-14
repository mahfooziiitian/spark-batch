import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName('json_array')
             .getOrCreate()
             )

    schema = StructType([
        StructField("book_name", StringType(), True),
        StructField("author", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    data = [("book", "John", 59),
            ("book", "Björn", 61),
            ("tv", "Roger", 36)]

    df = spark.createDataFrame(data=data, schema=schema)

    df.show(truncate=False)
