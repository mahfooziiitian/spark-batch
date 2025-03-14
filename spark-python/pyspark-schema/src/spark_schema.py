import os

from pyspark.sql import *
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

if __name__ == '__main__':
    data_schema = [StructField('name', StringType(), True),
                   StructField('email', StringType(), True),
                   StructField('city', StringType(), True),
                   StructField('mac', StringType(), True),
                   StructField('timestamp', TimestampType(), True),
                   StructField('creditcard', StringType(), True)
                   ]
    final_schema = StructType(fields=data_schema)
    spark = SparkSession.builder.appName("spark-schema").getOrCreate()
    data_path = os.environ["DATA_HOME"]+"\\FileData\\Json\\people.json"
    df = spark.read.json(data_path, schema=final_schema)
    df.show(truncate=False)

