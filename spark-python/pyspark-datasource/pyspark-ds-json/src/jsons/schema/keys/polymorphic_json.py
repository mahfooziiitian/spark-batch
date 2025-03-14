import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, ArrayType, StructField, IntegerType, TimestampType

os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    # Create SparkSession
    spark = (SparkSession.builder.master("local[*]")
             .appName('variable_keys')
             .getOrCreate())

    data_schema = StructType([
        # Type A related attributes
        # True implies nullable
        StructField("name", StringType(), True),
        StructField("pets", ArrayType(StringType()), True),

        # Type B related attributes
        StructField("whatever", StructType([
            StructField("X", StructType([
                StructField("foo", IntegerType(), True)
            ]), True),
            StructField("Y", StringType(), True)
        ]), True),  # True implies nullable
        StructField("favoriteInts", ArrayType(IntegerType()), True),
    ])

    common_schema = [
        StructField("type", StringType(), False),
        StructField("date", TimestampType(), False),
    ]

    schema = StructType([
        StructField("common", StructType(common_schema), False),
        StructField("data", data_schema, False)
    ])

    print(schema.json())

    data_file = os.environ.get("DATA_HOME", "").replace("\\", "/") + "/FileData/Json/dynamic_keys/polymorphic.json"

    df = (spark.read.format("json")
          .option("multiLine", True)
          .schema(schema)
          .load(data_file)
          )

    df.show()
    df.printSchema()
