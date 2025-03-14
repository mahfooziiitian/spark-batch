import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, inline

os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName('json_array')
             .getOrCreate()
             )

    json_string = json.dumps({
        "key1": 0.75,
        "values": [
            {
                "id": 2313,
                "val1": 350,
                "val2": 6000
            },
            {
                "id": 2477,
                "val1": 340,
                "val2": 6500
            }
        ]
    })
    df = spark.read.json(spark.sparkContext.parallelize([json_string]))

    df.printSchema()

    df.select("key1", "values.id", "values.val1", "values.val2").show()

    # using explode
    df.select("key1", explode(col("values")).alias("values")).select("key1", "values.*").show()

    # using inline function
    df.select("key1", inline(col("values"))).show()
