import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, Row

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0') \
        .appName("spark-xml-validator").getOrCreate()

    data_home = os.environ["DATA_HOME"]
    schema_path = f"{data_home}/FileData/Xml/schema/notes_schema.json"
    xml_file = f"{data_home}/FileData/Xml/notes.xml"

    with open(schema_path) as f:
        d = json.load(f)
        schema = StructType.fromJson(d)

    json_df = (spark.read.format("xml")
               .option("rowTag", "note")
               .option("excludeAttribute", "true")
               .option("ignoreNamespace", "true")
               .schema(schema)
               .load(xml_file))

    json_df.show(truncate=False)
