import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, ArrayType, MapType

os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk1.8.0_251"
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == '__main__':
    # Create SparkSession
    spark = (SparkSession.builder.master("local[*]")
             .appName('variable_keys')
             .getOrCreate())

    # Create DataFrame
    valSchema = (StructType()
                 .add("id", StringType())
                 .add("name", StringType())
                 .add("val", StringType()))

    valArrSchema = ArrayType(valSchema, True)
    mapSchema = MapType(StringType(), valArrSchema, True)
    jsonSchema = StructType().add("Items", mapSchema)
    schema = jsonSchema.json()
    print(schema)

    data_file = os.environ.get("DATA_HOME", "").replace("\\", "/") + "/FileData/Json/dynamic_keys/dynamic_keys.json"

    df = (spark.read.format("json").option("multiLine", True)
          .option("inferSchema", True)
          .schema(schema)
          .load(data_file)
          )

    df.show(truncate=False)
    print(df.schema.json())
