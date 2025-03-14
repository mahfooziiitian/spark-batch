import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .appName('parse-schema-json') \
        .getOrCreate()

    structureData = [
        (("James", "", "Smith"), "36636", "M", 3100),
        (("Michael", "Rose", ""), "40288", "M", 4300),
        (("Robert", "", "Williams"), "42114", "M", 1400),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500),
        (("Jen", "Mary", "Brown"), "", "F", -1)
    ]

    with open('schema.json', 'r') as jf:
        jsonSchema = json.load(jf)

    # df = pd.read_json("schema.json", lines=True)
    schemaFromJson = StructType.fromJson(jsonSchema)
    df = spark.createDataFrame(
        spark.sparkContext.parallelize(structureData), schemaFromJson)
    df.printSchema()
